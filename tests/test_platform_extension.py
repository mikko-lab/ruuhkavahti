"""
Testit sisäiselle allekirjoitusautentikoinnille (shared/internal_auth.py) ja
analytics-consumerin liukuvalle aggregaatille + HTTP-kerrokselle.

Ei Kafka-riippuvuutta: RollingAggregate on puhdas tietorakenne, ja FastAPI-
testit käyttävät TestClientia KAFKA_BOOTSTRAP-arvolla joka ei resolvoidu —
consume_loop-säie yrittää yhdistää taustalla ja epäonnistuu toistuvasti
(näkyy stderr:ssä), mutta se ei kaada HTTP-kerrosta koska poll() vain
aikakatkaisee eikä nosta poikkeusta.

Ajo: python3 -m unittest tests/test_platform_extension.py -v
"""

import importlib.util
import os
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHARED_DIR = os.path.join(REPO_ROOT, "shared")
ANALYTICS_DIR = os.path.join(REPO_ROOT, "analytics-consumer")
ANALYTICS_VENDOR_DIR = os.path.join(ANALYTICS_DIR, "vendor")

os.environ.setdefault("INTERNAL_SHARED_SECRET", "test-secret")
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:1")  # tarkoituksella ei resolvoidu

sys.path.insert(0, SHARED_DIR)
sys.path.insert(0, ANALYTICS_DIR)
sys.path.insert(0, ANALYTICS_VENDOR_DIR)

import internal_auth  # noqa: E402

# Namespace-kollisio: guardrail/vendor/ ja analytics-consumer/vendor/ ovat
# molemmat paketteja nimeltä "vendor" (sama vendorointi-kuvio molemmissa
# palveluissa, ks. DEEP_DIVE.md "Repo-rakenne"). `python -m unittest discover`
# ajaa kaikki testitiedostot samassa prosessissa, joten jos
# test_guardrail_logic.py on jo tuonut guardrailin vendor-paketin, se jää
# sys.modules-välimuistiin nimellä "vendor" eikä analytics_consumer.py:n
# `from vendor import internal_auth, tracing` löydä oikeaa pakettia enää.
# Tuotannossa (Docker) tätä ongelmaa ei ole — kussakin kontissa on vain
# yksi "vendor"-hakemisto. Poistetaan välimuistiin jäänyt versio ennen
# tämän moduulin lataamista, jotta testit ovat riippumattomia ajojärjestyksestä.
for _mod_name in list(sys.modules):
    if _mod_name == "vendor" or _mod_name.startswith("vendor."):
        del sys.modules[_mod_name]

_spec = importlib.util.spec_from_file_location(
    "analytics_consumer", os.path.join(ANALYTICS_DIR, "analytics_consumer.py")
)
analytics_consumer = importlib.util.module_from_spec(_spec)
sys.modules["analytics_consumer"] = analytics_consumer
_spec.loader.exec_module(analytics_consumer)


class TestInternalAuth(unittest.TestCase):
    def test_valid_signature_is_accepted(self):
        headers = internal_auth.sign("GET", "/metrics")
        internal_auth.verify(
            "GET", "/metrics",
            headers["X-Ruuhkavahti-Timestamp"], headers["X-Ruuhkavahti-Signature"],
        )  # ei poikkeusta = ok

    def test_tampered_signature_is_rejected(self):
        headers = internal_auth.sign("GET", "/metrics")
        with self.assertRaises(internal_auth.AuthError):
            internal_auth.verify(
                "GET", "/metrics", headers["X-Ruuhkavahti-Timestamp"], "deadbeef" * 8,
            )

    def test_signature_is_bound_to_path(self):
        headers = internal_auth.sign("GET", "/metrics")
        with self.assertRaises(internal_auth.AuthError):
            internal_auth.verify(
                "GET", "/some-other-path",
                headers["X-Ruuhkavahti-Timestamp"], headers["X-Ruuhkavahti-Signature"],
            )

    def test_expired_timestamp_is_rejected(self):
        import hashlib
        import hmac
        import time
        old_ts = str(int(time.time()) - 3600)
        sig = hmac.new(
            b"test-secret", f"GET\n/metrics\n{old_ts}".encode(), hashlib.sha256
        ).hexdigest()
        with self.assertRaises(internal_auth.AuthError):
            internal_auth.verify("GET", "/metrics", old_ts, sig)

    def test_missing_headers_are_rejected(self):
        with self.assertRaises(internal_auth.AuthError):
            internal_auth.verify("GET", "/metrics", None, None)


class TestRollingAggregate(unittest.TestCase):
    def test_counts_and_block_rate(self):
        agg = analytics_consumer.RollingAggregate()
        for _ in range(97):
            agg.record("PASS")
        for _ in range(2):
            agg.record("ESCALATE")
        agg.record("BLOCK")

        snap = agg.snapshot()
        self.assertEqual(snap["total_consumed_lifetime"], 100)
        self.assertEqual(snap["totals_in_window"], {"PASS": 97, "ESCALATE": 2, "BLOCK": 1})
        self.assertAlmostEqual(snap["block_rate_in_window"], 0.01)

    def test_empty_aggregate_has_zero_block_rate(self):
        agg = analytics_consumer.RollingAggregate()
        snap = agg.snapshot()
        self.assertEqual(snap["block_rate_in_window"], 0.0)
        self.assertEqual(snap["series"], [])


class TestAnalyticsConsumerHTTP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient
        cls.client = TestClient(analytics_consumer.app)
        cls.client.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)

    def test_health_is_open(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "ok")

    def test_metrics_without_auth_is_rejected(self):
        r = self.client.get("/metrics")
        self.assertEqual(r.status_code, 401)

    def test_metrics_with_valid_signature_is_accepted(self):
        headers = internal_auth.sign("GET", "/metrics")
        r = self.client.get("/metrics", headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertIn("totals_in_window", r.json())

    def test_metrics_with_stale_signature_is_rejected(self):
        headers = internal_auth.sign("GET", "/metrics")
        headers["X-Ruuhkavahti-Timestamp"] = str(int(headers["X-Ruuhkavahti-Timestamp"]) - 3600)
        r = self.client.get("/metrics", headers=headers)
        self.assertEqual(r.status_code, 401)


if __name__ == "__main__":
    unittest.main()
