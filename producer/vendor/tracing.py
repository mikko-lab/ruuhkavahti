"""
Ruuhkavahti — jaettu jäljitys (distributed tracing) OpenTelemetrillä.

Core-platform-laajennus: putki on jo Kafka-topicien kautta kytketty
(producer -> guardrail-consumer -> [approved|escalated|blocked]-messages),
mutta ennen tätä ei ollut mitään tapaa nähdä yhden yksittäisen viestin
koko matkaa palvelusta toiseen. Tämä moduuli antaa jokaiselle palvelulle
saman span-luonnin ja Kafka-header-pohjaisen kontekstin siirron.

Kafka ei tue W3C trace-contextia natiivisti, joten trace-parent kuljetetaan
viestin Kafka-headereissä (avain "traceparent") — sama periaate kuin HTTP:ssä,
vain eri kuljetuskerros. inject_kafka_headers/extract_kafka_headers hoitavat
tämän serialisoinnin.

LIPUTA ÄLÄ PIILOTA — tunnetut rajoitukset:
  - Sampling on head-based ja karkea: producer heittää kolikkoa per viesti
    (TRACE_SAMPLE_RATE) ja injektoi traceparentin vain "voittaneisiin";
    guardrail- ja analytics-consumer eivät koskaan *aloita* uutta tracea
    itse, ne vain jatkavat jos header on läsnä. Riittävä demolle, mutta
    naiivi: ei perustu virhetilanteisiin tai latenssiin (esim. "jäljitä
    aina jos ESCALATE/BLOCK" puuttuu), joten kiinnostavimmat yksittäis-
    tapaukset saattavat jäädä otannan ulkopuolelle.
  - Yksi Jaeger-instanssi ilman pysyvyyttä (in-memory storage) — span-data
    katoaa kun jaeger-kontti sammuu, ei kelpaa auditointiin.
  - dashboard-backend -> analytics-consumer -kutsu käynnistää oman spaninsa
    child-spanina, muttei linkity takaisin alkuperäisen viewer-viestin
    traceen — Kafka-osuus ja HTTP-osuus näkyvät siis kahtena erillisenä
    traceena Jaegerissa, ei yhtenä päästä-päähän-puuna.
"""

from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_initialized_for: str | None = None


def init_tracing(service_name: str) -> trace.Tracer:
    """Alustaa TracerProviderin kerran per prosessi ja palauttaa tracerin."""
    global _initialized_for
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")

    if _initialized_for != service_name:
        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
        )
        trace.set_tracer_provider(provider)
        _initialized_for = service_name

    return trace.get_tracer(service_name)


def inject_kafka_headers() -> list[tuple[str, bytes]]:
    """Ota nykyinen (aktiivinen) span-konteksti ja pakkaa se Kafka-headereiksi."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return [(k, v.encode("utf-8")) for k, v in carrier.items()]


def extract_kafka_headers(headers: list[tuple[str, bytes]] | None):
    """Lue traceparent Kafka-viestin headereista ja palauta konteksti
    jota vasten uusi span voidaan luoda lapsena (kafka Consumer -> msg.headers())."""
    carrier = {k: v.decode("utf-8") for k, v in (headers or [])}
    return extract(carrier)


def has_trace_context(headers: list[tuple[str, bytes]] | None) -> bool:
    """True jos viesti kantaa traceparent-headeria.

    Head-based sampling: producer päättää mitkä viestit jäljitetään
    (ks. TRACE_SAMPLE_RATE producer.py:ssä) eikä injektoi headeria muihin.
    Downstream-kuluttajat kunnioittavat tätä päätöstä eivätkä avaa uutta
    spania viesteille joissa headeria ei ole — muuten Jaeger tukehtuisi
    piikin ~8000 msg/s -kuormaan (ks. rajoitukset yllä)."""
    return any(k == "traceparent" for k, _ in (headers or []))
