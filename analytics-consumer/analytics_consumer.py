"""
Ruuhkavahti — analytics-consumer.

Core-platform-laajennus (ks. DEEP_DIVE.md "Core platform -laajennus"):
tähän asti vain guardrail-consumer ja dashboard-backend ovat lukeneet
Kafka-topiceja. Tämä palvelu on kolmas, täysin itsenäinen kuluttaja
approved/escalated/blocked-topiceille — eri consumer group ("analytics-group"),
eri tarkoitus, ei riipu dashboard-backendistä eikä päinvastoin.

Ero dashboard-backendin decision_consumer_loop:iin (ks. kafka_metrics.py):
dashboard pitää lyhyen liukuvan ikkunan (2000 viimeisintä) live-näyttöä
varten ja työntää sen WebSocketilla selaimeen 0.3s välein. Tämä palvelu
sen sijaan kasaa 10 sekunnin ämpäreitä viimeisen tunnin ajalta muistiin ja
tarjoaa ne REST-rajapinnasta — eri retentio, eri kuluttajamalli,
osoittaakseen että samaa tapahtumavirtaa voi lukea useampi riippumaton
palvelu ilman että ne tietävät toisistaan.

GET /health   - avoin, ei vaadi autentikointia (kontin healthcheck)
GET /metrics  - vaatii sisäisen palvelu-palvelu-allekirjoituksen
                (ks. internal_auth.py) — dashboard-backend on ainoa kutsuja.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from threading import Event, Lock

import uvicorn
from confluent_kafka import Consumer
from fastapi import FastAPI, Header, HTTPException
from opentelemetry import trace

from vendor import internal_auth, tracing

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = "analytics-group"
TOPICS = ["approved-messages", "escalated-messages", "blocked-messages"]
BUCKET_S = 10
RETENTION_S = 3600  # 1h liukuva ikkunö bucket-tasolla

tracer = tracing.init_tracing("analytics-consumer")

app = FastAPI()
stop_event = Event()


class RollingAggregate:
    """10s-ämpärit viimeiseltä tunnilta, decision -> lkm per ämpäri."""

    def __init__(self) -> None:
        self._lock = Lock()
        # bucket_start_epoch -> {"PASS": n, "ESCALATE": n, "BLOCK": n}
        self._buckets: dict[int, dict[str, int]] = defaultdict(lambda: {"PASS": 0, "ESCALATE": 0, "BLOCK": 0})
        self.total_consumed = 0

    def record(self, decision: str) -> None:
        bucket = int(time.time() // BUCKET_S) * BUCKET_S
        with self._lock:
            self._buckets[bucket][decision] = self._buckets[bucket].get(decision, 0) + 1
            self.total_consumed += 1
            self._evict(bucket)

    def _evict(self, now_bucket: int) -> None:
        cutoff = now_bucket - RETENTION_S
        for key in [k for k in self._buckets if k < cutoff]:
            del self._buckets[key]

    def snapshot(self) -> dict:
        with self._lock:
            series = [
                {
                    "bucket_start": datetime.fromtimestamp(b, tz=timezone.utc).isoformat(),
                    **counts,
                }
                for b, counts in sorted(self._buckets.items())
            ]
            totals = {"PASS": 0, "ESCALATE": 0, "BLOCK": 0}
            for counts in self._buckets.values():
                for k, v in counts.items():
                    totals[k] += v
            block_rate = totals["BLOCK"] / sum(totals.values()) if sum(totals.values()) else 0.0
            return {
                "window_s": RETENTION_S,
                "bucket_s": BUCKET_S,
                "total_consumed_lifetime": self.total_consumed,
                "totals_in_window": totals,
                "block_rate_in_window": round(block_rate, 4),
                "series": series,
            }


aggregate = RollingAggregate()


def consume_loop() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(TOPICS)
    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, AttributeError):
                continue

            ctx = tracing.extract_kafka_headers(msg.headers())
            with tracer.start_as_current_span("analytics.aggregate", context=ctx) as span:
                decision = payload.get("decision", "UNKNOWN")
                span.set_attribute("ruuhkavahti.decision", decision)
                span.set_attribute("ruuhkavahti.topic", msg.topic())
                aggregate.record(decision)
    finally:
        consumer.close()


@app.on_event("startup")
def start_background_thread() -> None:
    threading.Thread(target=consume_loop, daemon=True).start()


@app.on_event("shutdown")
def stop_background_thread() -> None:
    stop_event.set()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "total_consumed_lifetime": aggregate.total_consumed}


@app.get("/metrics")
def metrics(
    x_ruuhkavahti_timestamp: str | None = Header(default=None),
    x_ruuhkavahti_signature: str | None = Header(default=None),
) -> dict:
    with tracer.start_as_current_span("analytics.metrics_request") as span:
        try:
            internal_auth.verify("GET", "/metrics", x_ruuhkavahti_timestamp, x_ruuhkavahti_signature)
        except internal_auth.AuthError as exc:
            span.set_attribute("ruuhkavahti.auth_denied", True)
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        return aggregate.snapshot()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)
