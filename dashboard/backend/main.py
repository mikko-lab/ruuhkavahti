"""
Ruuhkavahti — dashboard-backend: WebSocket-silta Kafka-mittareista selaimeen.

/ws                    - lähettää mittarisnapshotin ~3x/s (lag, päätösjakauma, p50/p95)
POST /api/trigger-spike - välittää piikin laukaisun producerille (aito live-kontrolli)
GET  /api/scale-command  - palauttaa kopioitavan `docker compose --scale`-komennon
                            valitulle kuluttajamäärälle (ks. README: ei docker.sock-
                            mounttia backendiin, tietoinen turvallisuusvalinta demolle)
POST /api/export-video          - välittää videonauhoituksen käynnistyksen
GET  /api/export-video/status   - välittää nauhoituksen tilan (idle/running/done/error)
GET  /api/export-video/download - striimaa valmiin MP4:n selaimelle
                            (ks. video-exporter/ — erillinen Playwright+ffmpeg-palvelu)
"""

from __future__ import annotations

import asyncio
import os
import threading

import httpx
from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from kafka_metrics import (
    MetricsState,
    decision_consumer_loop,
    duplicate_events_consumer_loop,
    lag_poll_loop,
    producer_status_poll_loop,
    rebalance_events_consumer_loop,
)
from vendor import internal_auth, tracing

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
PRODUCER_URL = os.environ.get("PRODUCER_URL", "http://producer:8001")
VIDEO_EXPORTER_URL = os.environ.get("VIDEO_EXPORTER_URL", "http://video-exporter:8002")
ANALYTICS_URL = os.environ.get("ANALYTICS_URL", "http://analytics-consumer:8003")

tracer = tracing.init_tracing("dashboard-backend")

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

state = MetricsState()
stop_event = threading.Event()


@app.on_event("startup")
def start_background_threads() -> None:
    threading.Thread(
        target=lag_poll_loop, args=(state, KAFKA_BOOTSTRAP, stop_event), daemon=True
    ).start()
    threading.Thread(
        target=decision_consumer_loop, args=(state, KAFKA_BOOTSTRAP, stop_event), daemon=True
    ).start()
    threading.Thread(
        target=producer_status_poll_loop, args=(state, PRODUCER_URL, stop_event), daemon=True
    ).start()
    threading.Thread(
        target=rebalance_events_consumer_loop, args=(state, KAFKA_BOOTSTRAP, stop_event), daemon=True
    ).start()
    threading.Thread(
        target=duplicate_events_consumer_loop, args=(state, KAFKA_BOOTSTRAP, stop_event), daemon=True
    ).start()


@app.on_event("shutdown")
def stop_background_threads() -> None:
    stop_event.set()


@app.post("/api/trigger-spike")
async def trigger_spike() -> dict:
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{PRODUCER_URL}/trigger-spike", timeout=5.0)
        return r.json()


@app.get("/api/producer-status")
async def producer_status() -> dict:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{PRODUCER_URL}/status", timeout=5.0)
        return r.json()


@app.get("/api/scale-command")
def scale_command(count: int = 1) -> dict:
    count = max(1, min(4, count))
    return {"command": f"docker compose up -d --scale guardrail-consumer={count}"}


@app.get("/api/assignor-command")
def assignor_command(strategy: str = "cooperative-sticky") -> dict:
    # Sama periaate kuin scale-command: strategian vaihto vaatii kontin
    # uudelleenkäynnistyksen, joten dashboard näyttää komennon kopioitavaksi
    # sen sijaan että ohjaisi Dockeria suoraan (ks. README).
    if strategy not in ("cooperative-sticky", "range", "roundrobin"):
        strategy = "cooperative-sticky"
    return {
        "command": (
            f"ASSIGNMENT_STRATEGY={strategy} docker compose up -d --build guardrail-consumer"
        )
    }


@app.post("/api/export-video")
async def export_video() -> dict:
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{VIDEO_EXPORTER_URL}/export", timeout=5.0)
        return r.json()


@app.get("/api/export-video/status")
async def export_video_status() -> dict:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{VIDEO_EXPORTER_URL}/status", timeout=5.0)
        return r.json()


@app.get("/api/export-video/download")
async def export_video_download() -> Response:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{VIDEO_EXPORTER_URL}/download", timeout=30.0)
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text)
        return Response(
            content=r.content,
            media_type="video/mp4",
            headers={"Content-Disposition": "attachment; filename=ruuhkavahti-demo.mp4"},
        )


@app.get("/api/platform-metrics")
async def platform_metrics() -> dict:
    """Hakee analytics-consumerin liukuvan tunnin ikkunan (ks. sen docstring).

    Tämä on ensimmäinen suora palvelu-palvelu HTTP-kutsu ruuhkavahdissa
    (aiemmin palvelut ovat kommunikoineet vain Kafkan kautta) — siksi
    allekirjoitetaan sisäisellä jaetulla salaisuudella (ks. vendor/internal_auth.py)
    eikä vain luoteta Docker-verkon sisäisyyteen."""
    with tracer.start_as_current_span("dashboard.fetch_platform_metrics") as span:
        try:
            headers = internal_auth.sign("GET", "/metrics")
        except internal_auth.AuthError as exc:
            span.set_attribute("ruuhkavahti.auth_error", True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(f"{ANALYTICS_URL}/metrics", headers=headers, timeout=5.0)
            except httpx.HTTPError as exc:
                span.set_attribute("ruuhkavahti.upstream_error", True)
                raise HTTPException(status_code=502, detail=f"analytics-consumer ei vastaa: {exc}") from exc
            if r.status_code != 200:
                raise HTTPException(status_code=r.status_code, detail=r.text)
            return r.json()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(state.snapshot())
            await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        pass
