"""
Ruuhkavahti — (partition, offset)-pohjainen duplikaattisuodatus.

Ei Kafka-riippuvuutta (ks. README "Idempotenssi"): tämä on puhdas
tietorakenne, testattavissa ilman brokeria, samalla periaatteella kuin
guardrail/pipeline.py.
"""

from __future__ import annotations

from collections import OrderedDict


class DedupCache:
    """Rajattu LRU-joukko käsitellyistä (partition, offset)-pareista.

    Prosessin muistissa — ei selviä guardrail-consumerin omasta
    uudelleenkäynnistyksestä. Tietoinen demo-tason rajaus, ks. README.
    """

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError(f"max_size oltava vähintään 1, saatiin {max_size!r}")
        self._max_size = max_size
        self._seen: "OrderedDict[tuple[int, int], None]" = OrderedDict()

    def is_duplicate(self, partition: int, offset: int) -> bool:
        key = (partition, offset)
        if key in self._seen:
            self._seen.move_to_end(key)
            return True
        self._seen[key] = None
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)
