#!/usr/bin/env python3
"""
Ruuhkavahti — mittausskripti, joka ajaa oikeita kokeita pyörivää
docker-compose-stackia vasten (localhost:9092 / localhost:8000) ja kirjoittaa
tulokset results.json:iin. Ei simuloi mitään — jokainen luku tulee joko
Kafka AdminClientin/Consumerin suorasta havainnosta tai producer/backend-APIsta.

Käyttö (aja host-koneella, stack pystyssä; vaatii .venv:n jossa
confluent-kafka + httpx: python3 -m venv .venv && .venv/bin/pip install confluent-kafka httpx):
    .venv/bin/python3 scripts/measure.py rebalance --strategy cooperative-sticky
    .venv/bin/python3 scripts/measure.py rebalance --strategy range
    .venv/bin/python3 scripts/measure.py spike --n 1
    .venv/bin/python3 scripts/measure.py spike --n 2
    .venv/bin/python3 scripts/measure.py spike --n 4
    .venv/bin/python3 scripts/measure.py duplicates

Jokainen komento päivittää results.json:n omaa avaintaan, ei kirjoita
muiden komentojen tuloksia yli.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import threading
import time
from pathlib import Path

import httpx
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import _ConsumerGroupTopicPartitions as CGTP

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "results.json"
BOOTSTRAP = "localhost:9092"
BACKEND = "http://localhost:8000"
GROUP = "guardrail-group"
SRC_TOPIC = "viewer-messages"
NUM_PARTITIONS = 4
OUTPUT_TOPICS = ["approved-messages", "escalated-messages", "blocked-messages"]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_results() -> dict:
    if RESULTS.exists():
        return json.loads(RESULTS.read_text())
    return {}


def save_results(d: dict) -> None:
    RESULTS.write_text(json.dumps(d, indent=2, ensure_ascii=False) + "\n")


def compose(*args: str, env_extra: dict | None = None) -> None:
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    log("docker " + " ".join(args))
    subprocess.run(["docker", *args], cwd=REPO, check=True, env=env)


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def group_state(a: AdminClient) -> tuple[str, int]:
    desc = a.describe_consumer_groups([GROUP])[GROUP].result(timeout=5)
    return desc.state.name, len(desc.members)


def wait_stable(a: AdminClient, expected_n: int, timeout: float = 60.0) -> bool:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            state, n = group_state(a)
            if state == "STABLE" and n == expected_n:
                return True
        except Exception as exc:
            log(f"wait_stable poll-virhe: {exc}")
        time.sleep(0.2)
    return False


def total_lag(a: AdminClient) -> int:
    tps = [TopicPartition(SRC_TOPIC, p) for p in range(NUM_PARTITIONS)]
    req = CGTP(GROUP, tps)
    res = a.list_consumer_group_offsets([req])[GROUP].result(timeout=5)
    committed = {
        tp.partition: (tp.offset if tp.offset is not None and tp.offset >= 0 else 0)
        for tp in res.topic_partitions
    }
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"measure-lag-{time.time()}"})
    lag = 0
    for p in range(NUM_PARTITIONS):
        _low, high = c.get_watermark_offsets(TopicPartition(SRC_TOPIC, p), timeout=5, cached=False)
        lag += max(0, high - committed.get(p, 0))
    c.close()
    return lag


def output_watermark_sum() -> int:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"measure-wm-{time.time()}"})
    total = 0
    for t in OUTPUT_TOPICS:
        _low, high = c.get_watermark_offsets(TopicPartition(t, 0), timeout=5, cached=False)
        total += high
    c.close()
    return total


def duplicate_events_watermark() -> int:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"measure-dupwm-{time.time()}"})
    _low, high = c.get_watermark_offsets(TopicPartition("duplicate-events", 0), timeout=5, cached=False)
    c.close()
    return high


def producer_status() -> dict:
    r = httpx.get(f"{BACKEND}/api/producer-status", timeout=5)
    r.raise_for_status()
    return r.json()


def trigger_spike() -> dict:
    r = httpx.post(f"{BACKEND}/api/trigger-spike", timeout=5)
    r.raise_for_status()
    return r.json()


def scale_to(n: int, strategy: str | None = None, build: bool = False) -> None:
    env_extra = {"ASSIGNMENT_STRATEGY": strategy} if strategy else None
    args = ["compose", "up", "-d"]
    if build:
        args.append("--build")
    args += ["--scale", f"guardrail-consumer={n}", "guardrail-consumer"]
    compose(*args, env_extra=env_extra)


# ---------------------------------------------------------------------------
# rebalance: mittaa STABLE -> ei-STABLE -> STABLE -kestoa skaalattaessa 1->4
# ---------------------------------------------------------------------------

def cmd_rebalance(strategy: str) -> None:
    a = admin()
    log(f"=== rebalance-pausi, strategia={strategy} ===")

    build_needed = strategy != "cooperative-sticky"
    scale_to(1, strategy=strategy, build=build_needed)
    if not wait_stable(a, 1, timeout=90):
        raise SystemExit("Ei saatu ryhmää stabiiliksi n=1:llä")
    time.sleep(3.0)  # anna ryhmän asettua ennen mittausta

    transitions: list[tuple[float, str, int]] = []
    stop = threading.Event()

    def poll() -> None:
        while not stop.is_set():
            try:
                state, n = group_state(a)
                transitions.append((time.monotonic(), state, n))
            except Exception as exc:
                log(f"poll-virhe: {exc}")
            time.sleep(0.05)

    th = threading.Thread(target=poll, daemon=True)
    th.start()

    # rebalance-events-topicin kuuntelu: antaa oikean PER-PARTITIO
    # revoke->assign-keston, koska pelkkä ryhmän STABLE-tila ei kerro montako
    # partitiota oikeasti pysähtyi (ks. README: cooperative-sticky pysäyttää
    # vain siirtyvät partitiot, eager kaikki neljä).
    events: list[dict] = []
    stop_events = threading.Event()

    def rebalance_events_loop() -> None:
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"measure-rebalance-events-{time.time()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        c.subscribe(["rebalance-events"])
        while not stop_events.is_set():
            msg = c.poll(timeout=0.3)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value().decode("utf-8"))
            data["recv_monotonic"] = time.monotonic()
            events.append(data)
        c.close()

    ev_th = threading.Thread(target=rebalance_events_loop, daemon=True)
    ev_th.start()
    time.sleep(1.0)  # anna event-consumerin liittyä ennen skaalausta

    t_issue = time.monotonic()
    scale_to(4, strategy=strategy, build=False)
    ok = wait_stable(a, 4, timeout=90)
    t_confirmed_stable = time.monotonic()
    time.sleep(1.5)
    stop.set()
    stop_events.set()
    th.join()
    ev_th.join()

    if not ok:
        raise SystemExit("Ei saatu ryhmää stabiiliksi n=4:llä")

    # per-partitio revoke->assign-kesto rebalance-events-topicista
    revoke_at: dict[int, float] = {}
    assign_at: dict[int, float] = {}
    for e in sorted(events, key=lambda x: x["recv_monotonic"]):
        if e["recv_monotonic"] < t_issue - 0.5:
            continue  # tämän skaalauksen ulkopuolinen tapahtuma (esim. edellisen testin jäänne)
        for p in e["partitions"]:
            if e["event"] == "revoke" and p not in revoke_at:
                revoke_at[p] = e["recv_monotonic"]
            elif e["event"] == "assign" and p in revoke_at and p not in assign_at:
                assign_at[p] = e["recv_monotonic"]

    per_partition_pause = {
        p: round(assign_at[p] - revoke_at[p], 3)
        for p in revoke_at
        if p in assign_at
    }
    partitions_revoked = sorted(revoke_at.keys())
    log(f"partitiot jotka pysähtyivät: {partitions_revoked} (kaikkiaan {NUM_PARTITIONS})")
    log(f"per-partitio pausi (s): {per_partition_pause}")

    # ensimmäinen ei-STABLE havainto t_issue:n jälkeen == pausin alku
    pause_start = None
    for t, state, _n in transitions:
        if t >= t_issue and state != "STABLE":
            pause_start = t
            break
    # viimeinen STABLE+n==4 havainto == pausin loppu (konservatiivinen yläraja)
    pause_end = None
    for t, state, n in transitions:
        if state == "STABLE" and n == 4:
            pause_end = t
            break

    if pause_start is None or pause_end is None:
        log("VAROITUS: rebalance-tapahtumaa ei havaittu pollausvälissä (50ms) — "
            "pausi oli joko lyhyempi kuin pollausväli tai tapahtui pollien välissä.")
        pause_s = None
    else:
        pause_s = round(pause_end - pause_start, 3)

    log(f"pause_seconds={pause_s} (issue->stable_confirmed kokonaiskesto={round(t_confirmed_stable - t_issue, 3)}s)")

    results = load_results()
    results.setdefault("rebalance_pause_seconds", {})
    results["rebalance_pause_seconds"][strategy] = {
        "group_coordinator_pause_seconds": pause_s,
        "issue_to_stable_confirmed_seconds": round(t_confirmed_stable - t_issue, 3),
        "partitions_stopped": partitions_revoked,
        "partitions_stopped_count": len(partitions_revoked),
        "num_partitions": NUM_PARTITIONS,
        "per_partition_pause_seconds": per_partition_pause,
        "poll_interval_seconds": 0.05,
        "scale": "1->4",
        "note": (
            "group_coordinator_pause_seconds = ensimmäisen ei-STABLE-havainnon ja "
            "ensimmäisen STABLE+4-jäsentä-havainnon välinen aika (50ms pollaus, "
            "AdminClient.describe_consumer_groups), sisältää broker-oletuksen "
            "group.initial.rebalance.delay.ms. Tämä EI ole sama asia kuin per-partitio "
            "kulutuskatko: partitions_stopped/per_partition_pause_seconds tulevat "
            "guardrail_consumer.py:n on_assign/on_revoke-callbackien julkaisemasta "
            "rebalance-events-topicista ja kertovat todellisen, partitiokohtaisen "
            "revoke->assign-ajan."
        ),
    }
    save_results(results)
    log("tallennettu results.json:iin")


# ---------------------------------------------------------------------------
# spike: yhdellä piikillä samaan aikaan throughput + latenssi (+lag-recovery)
# ---------------------------------------------------------------------------

def cmd_spike(n: int) -> None:
    a = admin()
    log(f"=== piikki-koe, kuluttajia={n} ===")

    scale_to(n, strategy="cooperative-sticky", build=False)
    if not wait_stable(a, n, timeout=90):
        raise SystemExit(f"Ei saatu ryhmää stabiiliksi n={n}:llä")

    # varmista ettei jäljellä vanhaa lagia ennen mittausta
    log("odotetaan lagin tyhjenemistä ennen koetta...")
    t0 = time.monotonic()
    while total_lag(a) > 5:
        if time.monotonic() - t0 > 60:
            log("VAROITUS: lag ei tyhjentynyt 60s:ssa, jatketaan silti")
            break
        time.sleep(1.0)

    # temp-consumer keräämään latenssit koko piikki-ikkunalta
    latencies: list[float] = []
    stop_latency = threading.Event()

    def latency_loop() -> None:
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"measure-latency-{time.time()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        c.subscribe(OUTPUT_TOPICS)
        while not stop_latency.is_set():
            msg = c.poll(timeout=0.5)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value().decode("utf-8"))
            latencies.append(data.get("latency_ms", 0.0))
        c.close()

    lat_th = threading.Thread(target=latency_loop, daemon=True)
    lat_th.start()
    time.sleep(1.0)  # anna consumerin liittyä groupiin ennen piikkiä

    wm_start = output_watermark_sum()
    t_spike_trigger = time.monotonic()
    trigger_spike()
    log("piikki laukaistu")

    # seuraa producerin tilaa + lagia kunnes takaisin baseline + lag nolla
    lag_series: list[tuple[float, int, str]] = []
    spike_active_start = None
    spike_active_end = None
    wm_at_spike_start = None
    wm_at_spike_end = None
    settle_count = 0
    while True:
        try:
            st = producer_status()
        except Exception as exc:
            log(f"producer-status-virhe: {exc}")
            time.sleep(0.5)
            continue
        lag = total_lag(a)
        t = time.monotonic() - t_spike_trigger
        lag_series.append((round(t, 2), lag, st["mode"]))

        if st["mode"] == "spike" and spike_active_start is None:
            spike_active_start = time.monotonic()
            wm_at_spike_start = output_watermark_sum()
        if st["mode"] == "baseline" and spike_active_start is not None and spike_active_end is None:
            spike_active_end = time.monotonic()
            wm_at_spike_end = output_watermark_sum()

        if st["mode"] == "baseline" and spike_active_end is not None and lag <= 5:
            settle_count += 1
        else:
            settle_count = 0
        if settle_count >= 3:
            break
        if t > 180:
            log("VAROITUS: 180s aikakatkaisu, lopetetaan seuranta")
            break
        time.sleep(0.5)

    stop_latency.set()
    lat_th.join()

    t_lag_zero = time.monotonic()
    recovery_s = round(t_lag_zero - spike_active_end, 2) if spike_active_end else None

    throughput_msg_s = None
    if wm_at_spike_start is not None and wm_at_spike_end is not None and spike_active_end:
        elapsed = spike_active_end - spike_active_start
        if elapsed > 0:
            throughput_msg_s = round((wm_at_spike_end - wm_at_spike_start) / elapsed, 1)

    lat_sorted = sorted(latencies)
    def pct(p: float) -> float | None:
        if not lat_sorted:
            return None
        idx = min(len(lat_sorted) - 1, int(len(lat_sorted) * p))
        return round(lat_sorted[idx], 1)

    log(f"throughput={throughput_msg_s} msg/s  p50={pct(0.5)}ms  p95={pct(0.95)}ms  "
        f"recovery={recovery_s}s  n_latency_samples={len(lat_sorted)}")

    results = load_results()
    results.setdefault("spike_runs", {})
    results["spike_runs"][str(n)] = {
        "consumers": n,
        "sustained_throughput_msg_s": throughput_msg_s,
        "latency_p50_ms": pct(0.5),
        "latency_p95_ms": pct(0.95),
        "latency_sample_count": len(lat_sorted),
        "lag_recovery_seconds_after_spike_end": recovery_s,
        "spike_duration_measured_seconds": round(spike_active_end - spike_active_start, 2) if spike_active_end else None,
        "lag_series_sample": lag_series[::4],  # harvennettu, koko sarja liian pitkä
    }
    save_results(results)
    log("tallennettu results.json:iin")


# ---------------------------------------------------------------------------
# duplicates: pakotettu session-timeout-rebalance (docker pause, EI restart)
# yhdellä kuluttajalla, mitataan duplicate-events-topicin kasvu
# ---------------------------------------------------------------------------

def cmd_duplicates(pause_seconds: float, target_messages: int) -> None:
    a = admin()
    log("=== duplikaattikoe: docker pause ilman restarttia (DedupCache säilyy) ===")

    scale_to(1, strategy="cooperative-sticky", build=False)
    if not wait_stable(a, 1, timeout=90):
        raise SystemExit("Ei saatu ryhmää stabiiliksi n=1:llä")

    t0 = time.monotonic()
    while total_lag(a) > 5:
        if time.monotonic() - t0 > 60:
            break
        time.sleep(1.0)

    dup_before = duplicate_events_watermark()
    wm_before = output_watermark_sum()

    container = subprocess.run(
        ["docker", "compose", "ps", "-q", "guardrail-consumer"],
        cwd=REPO, check=True, capture_output=True, text=True,
    ).stdout.strip().splitlines()
    if len(container) != 1:
        raise SystemExit(f"Odotettiin tasan 1 guardrail-consumer-konttia, löytyi {len(container)}")
    cid = container[0]

    log(f"pausetaan kontti {cid[:12]} {pause_seconds}s (session.timeout.ms ylitetään ilman restarttia)")
    subprocess.run(["docker", "pause", cid], check=True)
    time.sleep(pause_seconds)
    subprocess.run(["docker", "unpause", cid], check=True)
    log("unpause tehty, odotetaan groupin palautumista ja lagin tyhjenemistä")

    if not wait_stable(a, 1, timeout=90):
        log("VAROITUS: ryhmä ei palautunut STABLE+1:ksi 90s:ssa")

    t1 = time.monotonic()
    while total_lag(a) > 5:
        if time.monotonic() - t1 > 90:
            log("VAROITUS: lag ei tyhjentynyt 90s:ssa unpausen jälkeen")
            break
        time.sleep(1.0)

    time.sleep(2.0)  # varmista että duplicate-events-tuotot ehtivät perille
    dup_after = duplicate_events_watermark()
    wm_after = output_watermark_sum()

    results = load_results()
    results["duplicate_filter_test"] = {
        "method": "docker pause (SIGSTOP-tyyppinen jäädytys, EI kontin restart) "
                  f"{pause_seconds}s yksittäiselle guardrail-consumer-instanssille "
                  "ylittäen session.timeout.ms:n ilman prosessin uudelleenkäynnistystä, "
                  "jolloin DedupCache säilyy muistissa (ks. README/DEEP_DIVE rajaus: "
                  "restart tyhjentäisi cachen eikä tätä testiä silloin voisi ajaa näin).",
        "messages_produced_during_test": wm_after - wm_before,
        "duplicates_filtered": dup_after - dup_before,
        "note": (
            "guardrail_consumer.py commitoi jokaisen viestin heti tuoton jälkeen "
            "(ei batch-commit), joten mahdollisten kesken jääneiden, ei-committoitujen "
            "viestien ikkuna on tyypillisesti korkeintaan yksi viesti per pause-sykli — "
            "tämä on odotettu tulos, ei mittausvirhe."
        ),
    }
    save_results(results)
    log(f"tallennettu: duplicates_filtered={dup_after - dup_before} "
        f"messages_produced={wm_after - wm_before}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_reb = sub.add_parser("rebalance")
    p_reb.add_argument("--strategy", required=True, choices=["cooperative-sticky", "range"])

    p_spike = sub.add_parser("spike")
    p_spike.add_argument("--n", type=int, required=True, choices=[1, 2, 4])

    p_dup = sub.add_parser("duplicates")
    p_dup.add_argument("--pause-seconds", type=float, default=50.0)
    p_dup.add_argument("--target-messages", type=int, default=10000)

    args = p.parse_args()
    if args.cmd == "rebalance":
        cmd_rebalance(args.strategy)
    elif args.cmd == "spike":
        cmd_spike(args.n)
    elif args.cmd == "duplicates":
        cmd_duplicates(args.pause_seconds, args.target_messages)


if __name__ == "__main__":
    main()
