"""
Ruuhkavahti — guardrail-kuluttaja.

Kuluttaa `viewer-messages`-topicia osana consumer groupia `guardrail-group`
(ks. repon README "Consumer group ja rebalance"). Jokainen instanssi saa
Kafkalta oman siivun partitioista; useampi instanssi rinnakkain nostaa
läpimenoa ilman koodimuutoksia (`docker compose up --scale guardrail-consumer=N`).

Jokainen viesti ajetaan pipeline.process_message() läpi ja reititetään
päätöksen mukaan approved/escalated/blocked-topiciin, mukana viewer_id,
alkuperäinen timestamp, decision ja latency_ms — näistä dashboard laskee
päätösjakauman ja p50/p95-läpimenoajan.

Kaksi lisäkerrosta, molemmat liittyvät suoraan at-least-once-semantiikkaan
(ks. README):
  - on_assign/on_revoke-callbackit julkaisevat rebalance-tapahtumat
    `rebalance-events`-topiciin, jotta dashboard voi näyttää milloin
    partitioita siirtyy kuluttajien välillä (ks. README "Rebalance-strategiat").
  - (partition, offset)-pohjainen duplikaattisuodatus: jos committia ei
    ehditä tehdä ennen kuin sama offset tarjotaan uudelleen samalle
    prosessille (esim. session timeout -pohjainen rebalance ilman
    varsinaista käynnistystä uudelleen), sama viesti käsiteltäisiin kahdesti.
    (partition, offset) on Kafkan oma, aidosti uniikki koordinaatti yhdelle
    fyysiselle viestille — ei tarvitse keksiä erillistä viesti-ID:tä.
    Duplikaatti ESCALATE/BLOCK olisi oikea ongelma (sama viesti
    ihmismoderaattorille kahdesti), joten tunnistetut duplikaatit
    suodatetaan pois ennen output-julkaisua. HUOM: cache on prosessin
    muistissa — se EI selviä guardrail-consumerin omasta uudelleen-
    käynnistyksestä (ks. README "Idempotenssin rajaukset").
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition

from dedup import DedupCache
from vendor.refuse_dont_guess import Decision
from pipeline import process_message

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = "guardrail-group"
SOURCE_TOPIC = "viewer-messages"
REBALANCE_EVENTS_TOPIC = "rebalance-events"
DUPLICATE_EVENTS_TOPIC = "duplicate-events"

# "cooperative-sticky" (KIP-429) tai "range"/"roundrobin" (klassinen eager).
# Vaihto vaatii kontin uudelleenkäynnistyksen — ei voi vaihtaa ajon aikana.
ASSIGNMENT_STRATEGY = os.environ.get("ASSIGNMENT_STRATEGY", "cooperative-sticky")

# Demo-tason ikkuna — ks. README "Idempotenssin rajaukset": ei selviä
# consumerin uudelleenkäynnistyksestä, ei ole pysyvä tallennus.
DEDUP_WINDOW_SIZE = 500

OUTPUT_TOPIC = {
    Decision.PASS: "approved-messages",
    Decision.ESCALATE: "escalated-messages",
    Decision.BLOCK: "blocked-messages",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "partition.assignment.strategy": ASSIGNMENT_STRATEGY,
        # Konsepti 3 (offset/at-least-once): commitoidaan käsin vasta output-tuotoksen
        # jälkeen, joten viesti ei koskaan katoa jäljettömiin kesken käsittelyn.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    dedup = DedupCache(DEDUP_WINDOW_SIZE)

    def publish_rebalance_event(event: str, partitions: list[TopicPartition]) -> None:
        payload = {
            "event": event,
            "partitions": [p.partition for p in partitions],
            "strategy": ASSIGNMENT_STRATEGY,
            "timestamp": _now_iso(),
        }
        producer.produce(REBALANCE_EVENTS_TOPIC, value=json.dumps(payload).encode("utf-8"))
        producer.poll(0)

    def on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        publish_rebalance_event("assign", partitions)

    def on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        publish_rebalance_event("revoke", partitions)

    consumer.subscribe([SOURCE_TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer-virhe: {msg.error()}")
                continue

            partition = msg.partition()
            offset = msg.offset()

            if dedup.is_duplicate(partition, offset):
                source = json.loads(msg.value().decode("utf-8"))
                producer.produce(
                    DUPLICATE_EVENTS_TOPIC,
                    value=json.dumps({
                        "viewer_id": source["viewer_id"],
                        "partition": partition,
                        "offset": offset,
                        "timestamp": _now_iso(),
                    }).encode("utf-8"),
                )
                producer.poll(0)
                # Ei julkaista uudelleen approved/escalated/blocked-topiciin —
                # duplikaatti ESCALATE/BLOCK menisi ihmismoderaattorille kahdesti.
                # Commit silti, ettei samaa offsettia lueta loputtomiin.
                consumer.commit(msg)
                continue

            received_at = time.time()
            source = json.loads(msg.value().decode("utf-8"))
            result = process_message(source["content"])
            sent_at = source["timestamp"]

            out = {
                "viewer_id": source["viewer_id"],
                "timestamp": sent_at,
                "decision": result.decision.value,
                "reason": result.reason,
                "latency_ms": round((received_at - _parse_ts(sent_at)) * 1000, 2),
                "partition": partition,
            }
            producer.produce(
                OUTPUT_TOPIC[result.decision],
                key=source["viewer_id"].encode("utf-8"),
                value=json.dumps(out).encode("utf-8"),
            )
            producer.poll(0)

            # Vasta nyt commit: jos worker kaatuisi ennen tätä riviä, viesti
            # luetaan uudelleen seuraavalla käynnistyksellä (at-least-once) —
            # ja dedup-cache tunnistaa sen duplikaatiksi yllä.
            consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5)
        consumer.close()


def _parse_ts(iso_ts: str) -> float:
    from datetime import datetime as _dt
    return _dt.fromisoformat(iso_ts).timestamp()


if __name__ == "__main__":
    main()
