"""
Ruuhkavahti — dashboard-mittarit: consumer lag, aktiivisten kuluttajien määrä,
päätösjakauma, läpimenoajan p50/p95, rebalance-tila ja duplikaattilaskuri.

Taustasäikeet:
  - lag_poll_loop: kysyy Kafkalta (AdminClient) guardrail-groupin committed
    offsetit + partitioiden korkeimmat offsetit kerran sekunnissa, laskee
    lag = high_watermark - committed per partitio (ks. README "Consumer lag
    mittarina"). Kysyy myös ryhmän jäsenmäärän ja ryhmän tilan (STABLE vs.
    PREPARING/COMPLETING_REBALANCING) — jälkimmäinen on autoritatiivinen
    signaali siitä, onko ryhmä juuri nyt rebalancoimassa.
  - decision_consumer_loop: kuluttaa approved/escalated/blocked-topicit omalla,
    guardrail-groupista erillisellä group.id:llä, laskee jakauman ja
    latency_ms-jakauman p50/p95:tä varten.
  - rebalance_events_consumer_loop: kuluttaa guardrail_consumer.py:n
    julkaisemat on_assign/on_revoke-tapahtumat, päättelee mitkä partitiot
    ovat juuri nyt siirtymässä (revoke:ssa lisätty, assign:ssa poistettu) —
    tätä käytetään cooperative-sticky-tilan osittaiseen pysähdysvisualisointiin.
  - duplicate_events_consumer_loop: laskee guardrail_consumer.py:n
    suodattamat duplikaatit.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from threading import Event, Lock

import httpx
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

# confluent-kafka nimeää tämän luokan alaviivalla varustettuna sisäisenä
# symbolina (_ConsumerGroupTopicPartitions) vaikka dokumentaatio ja
# list_consumer_group_offsets()-metodin oma docstring puhuvat siitä
# julkisena "ConsumerGroupTopicPartitions"-nimenä — todennettu versioissa
# 2.4.0 ja 2.15.0, ei siis versiokohtainen kirjoitusvirhe.
from confluent_kafka.admin import _ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions

TOPIC = "viewer-messages"
NUM_PARTITIONS = 4
GUARDRAIL_GROUP_ID = "guardrail-group"
REBALANCING_STATES = {"PREPARING_REBALANCING", "COMPLETING_REBALANCING"}


class MetricsState:
    def __init__(self) -> None:
        self._lock = Lock()
        self.lag: dict[int, int] = {p: 0 for p in range(NUM_PARTITIONS)}
        self.active_consumers = 0
        self.decisions = {"PASS": 0, "ESCALATE": 0, "BLOCK": 0}
        self._latencies: deque[float] = deque(maxlen=2000)
        self.producer_mode = "baseline"
        self.producer_rate = 0
        self.rebalancing = False
        self.transitioning_partitions: set[int] = set()
        self.assignment_strategy = "cooperative-sticky"
        self.duplicates_filtered = 0

    def snapshot(self) -> dict:
        with self._lock:
            lat = sorted(self._latencies)

            def pct(p: float) -> float:
                if not lat:
                    return 0.0
                idx = min(len(lat) - 1, int(len(lat) * p))
                return round(lat[idx], 1)

            return {
                "lag": dict(self.lag),
                "total_lag": sum(self.lag.values()),
                "active_consumers": self.active_consumers,
                "decisions": dict(self.decisions),
                "latency_p50_ms": pct(0.50),
                "latency_p95_ms": pct(0.95),
                "producer_mode": self.producer_mode,
                "producer_rate": self.producer_rate,
                "rebalancing": self.rebalancing,
                "transitioning_partitions": sorted(self.transitioning_partitions),
                "assignment_strategy": self.assignment_strategy,
                "duplicates_filtered": self.duplicates_filtered,
            }

    def update_producer_status(self, mode: str, rate: int) -> None:
        with self._lock:
            self.producer_mode = mode
            self.producer_rate = rate

    def record_decision(self, decision: str, latency_ms: float) -> None:
        with self._lock:
            self.decisions[decision] = self.decisions.get(decision, 0) + 1
            self._latencies.append(latency_ms)

    def update_lag(self, lag_by_partition: dict[int, int], active_consumers: int) -> None:
        with self._lock:
            self.lag = lag_by_partition
            self.active_consumers = active_consumers

    def update_group_state(self, state_name: str) -> None:
        with self._lock:
            self.rebalancing = state_name in REBALANCING_STATES
            if not self.rebalancing:
                self.transitioning_partitions.clear()

    def apply_rebalance_event(self, event: str, partitions: list[int], strategy: str) -> None:
        with self._lock:
            self.assignment_strategy = strategy
            if event == "revoke":
                self.transitioning_partitions.update(partitions)
            elif event == "assign":
                self.transitioning_partitions.difference_update(partitions)

    def record_duplicate(self) -> None:
        with self._lock:
            self.duplicates_filtered += 1


def lag_poll_loop(state: MetricsState, bootstrap: str, stop_event: Event) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    watermark_consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "ruuhkavahti-dashboard-watermark",
    })
    tps = [TopicPartition(TOPIC, p) for p in range(NUM_PARTITIONS)]

    while not stop_event.is_set():
        try:
            request = ConsumerGroupTopicPartitions(GUARDRAIL_GROUP_ID, tps)
            futures = admin.list_consumer_group_offsets([request])
            result = futures[GUARDRAIL_GROUP_ID].result(timeout=5)
            committed = {
                tp.partition: (tp.offset if tp.offset is not None and tp.offset >= 0 else 0)
                for tp in result.topic_partitions
            }

            lag: dict[int, int] = {}
            for p in range(NUM_PARTITIONS):
                _low, high = watermark_consumer.get_watermark_offsets(
                    TopicPartition(TOPIC, p), timeout=5, cached=False
                )
                lag[p] = max(0, high - committed.get(p, 0))

            group_futures = admin.describe_consumer_groups([GUARDRAIL_GROUP_ID])
            desc = group_futures[GUARDRAIL_GROUP_ID].result(timeout=5)
            active = len(desc.members)

            state.update_lag(lag, active)
            state.update_group_state(desc.state.name)
        except Exception as exc:  # demo-taustasäie: virhe ei saa kaataa dashboardia
            print(f"lag-poll-virhe: {exc}")
        time.sleep(1.0)


def decision_consumer_loop(state: MetricsState, bootstrap: str, stop_event: Event) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "ruuhkavahti-dashboard-metrics",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["approved-messages", "escalated-messages", "blocked-messages"])
    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode("utf-8"))
        state.record_decision(data["decision"], data.get("latency_ms", 0.0))
    consumer.close()


def rebalance_events_consumer_loop(state: MetricsState, bootstrap: str, stop_event: Event) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "ruuhkavahti-dashboard-rebalance-events",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["rebalance-events"])
    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode("utf-8"))
        state.apply_rebalance_event(data["event"], data["partitions"], data["strategy"])
    consumer.close()


def duplicate_events_consumer_loop(state: MetricsState, bootstrap: str, stop_event: Event) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "ruuhkavahti-dashboard-duplicate-events",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["duplicate-events"])
    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        state.record_duplicate()
    consumer.close()


def producer_status_poll_loop(state: MetricsState, producer_url: str, stop_event: Event) -> None:
    with httpx.Client(timeout=3.0) as client:
        while not stop_event.is_set():
            try:
                r = client.get(f"{producer_url}/status")
                data = r.json()
                state.update_producer_status(data["mode"], data["rate"])
            except Exception as exc:
                print(f"producer-status-poll-virhe: {exc}")
            time.sleep(1.0)
