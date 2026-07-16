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
"""

from __future__ import annotations

import json
import os
import time

from confluent_kafka import Consumer, Producer

from vendor.refuse_dont_guess import Decision
from pipeline import process_message

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = "guardrail-group"
SOURCE_TOPIC = "viewer-messages"

OUTPUT_TOPIC = {
    Decision.PASS: "approved-messages",
    Decision.ESCALATE: "escalated-messages",
    Decision.BLOCK: "blocked-messages",
}


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        # Konsepti 3 (offset/at-least-once): commitoidaan käsin vasta output-tuotoksen
        # jälkeen, joten viesti ei koskaan katoa jäljettömiin kesken käsittelyn.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([SOURCE_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer-virhe: {msg.error()}")
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
                "partition": msg.partition(),
            }
            producer.produce(
                OUTPUT_TOPIC[result.decision],
                key=source["viewer_id"].encode("utf-8"),
                value=json.dumps(out).encode("utf-8"),
            )
            producer.poll(0)

            # Vasta nyt commit: jos worker kaatuisi ennen tätä riviä, viesti
            # luetaan uudelleen seuraavalla käynnistyksellä (at-least-once).
            consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5)
        consumer.close()


def _parse_ts(iso_ts: str) -> float:
    from datetime import datetime
    return datetime.fromisoformat(iso_ts).timestamp()


if __name__ == "__main__":
    main()
