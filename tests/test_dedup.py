"""
Testit (partition, offset)-pohjaiselle duplikaattisuodatukselle. Ei Kafka-
riippuvuutta — guardrail/dedup.py on puhdas tietorakenne.

Ajo: python3 -m unittest tests/test_dedup.py -v
"""

import os
import sys
import unittest

GUARDRAIL_DIR = os.path.join(os.path.dirname(__file__), "..", "guardrail")
sys.path.insert(0, os.path.abspath(GUARDRAIL_DIR))

from dedup import DedupCache  # noqa: E402


class TestDedupCache(unittest.TestCase):
    def test_first_sighting_is_not_duplicate(self):
        cache = DedupCache(500)
        self.assertFalse(cache.is_duplicate(0, 42))

    def test_second_sighting_of_same_key_is_duplicate(self):
        cache = DedupCache(500)
        cache.is_duplicate(0, 42)
        self.assertTrue(cache.is_duplicate(0, 42))

    def test_same_offset_different_partition_is_not_duplicate(self):
        cache = DedupCache(500)
        cache.is_duplicate(0, 42)
        self.assertFalse(cache.is_duplicate(1, 42))

    def test_bounded_window_evicts_oldest(self):
        cache = DedupCache(3)
        cache.is_duplicate(0, 1)
        cache.is_duplicate(0, 2)
        cache.is_duplicate(0, 3)
        cache.is_duplicate(0, 4)  # täyttää ikkunan, häätää (0,1)
        self.assertFalse(cache.is_duplicate(0, 1), "häädetty avain ei saa enää olla duplikaatti")
        self.assertEqual(len(cache), 3)

    def test_recently_seen_key_is_not_evicted_early(self):
        cache = DedupCache(2)
        cache.is_duplicate(0, 1)
        cache.is_duplicate(0, 2)
        cache.is_duplicate(0, 1)  # kosketus siirtää (0,1):n tuoreimmaksi
        cache.is_duplicate(0, 3)  # häätää nyt (0,2), ei (0,1)
        self.assertTrue(cache.is_duplicate(0, 1))
        self.assertFalse(cache.is_duplicate(0, 2))

    def test_rejects_non_positive_max_size(self):
        with self.assertRaises(ValueError):
            DedupCache(0)


if __name__ == "__main__":
    unittest.main()
