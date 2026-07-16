"""
Ruuhkavahti — katsojaviestien deterministinen moderointisääntö.

Tämä tiedosto on TARKOITUKSELLA saman muotoinen kuin
vendor/refuse_dont_guess.py:n apply_rule(): luottamuskynnys + deterministinen
sääntö, joka EI KOSKAAN palauta BLOCK (BLOCK on varattu yksinomaan
injektiontunnistukselle, ks. pipeline.py). Epävarma tapaus eskaloi ihmiselle,
ei arvata — sama periaate, uusi toimiala.

Avainsanalistat ovat HAVAINNOLLISTAVA placeholder, ei tuotantotason
sisällönmoderointimalli (vrt. alkuperäisen repon ALV-sääntö: sama rajaus,
sama rehellisyys siitä).
"""

from __future__ import annotations
from dataclasses import dataclass

from vendor.refuse_dont_guess import Decision, Result

CONFIDENCE_THRESHOLD = 0.80

# Havainnollistava placeholder — ei tuotantotason kieltolista.
BLOCKLIST_WORDS = ("saatana", "vittu", "perkele")
# Rajatapaus: lievä loukkaus/sarkasmi, ei automaattisesti selvä — ihminen päättää.
WATCHLIST_WORDS = ("idiootti", "tyhmä", "surkea")


@dataclass(frozen=True)
class ChatExtraction:
    """Deterministisen avainsanaskannauksen tulos yhdestä viestistä."""
    flagged: bool
    confidence: float

    def __post_init__(self):
        if not isinstance(self.confidence, (int, float)) or not (0.0 <= self.confidence <= 1.0):
            raise ValueError(f"confidence oltava luku [0,1], saatiin {self.confidence!r}")


def extract_chat_signal(content: str) -> ChatExtraction:
    """Deterministinen avainsanaskannaus. Ei mallikutsua — tämä EI ole toksisuusluokitin,
    vaan yksinkertainen, testattava placeholder samassa hengessä kuin alkuperäisen
    repon ALV-sääntö."""
    lowered = content.lower()
    if any(w in lowered for w in BLOCKLIST_WORDS):
        return ChatExtraction(flagged=True, confidence=0.95)
    if any(w in lowered for w in WATCHLIST_WORDS):
        return ChatExtraction(flagged=True, confidence=0.55)
    return ChatExtraction(flagged=False, confidence=0.97)


def apply_chat_rule(e: ChatExtraction) -> Result:
    """Palauttaa vain PASS tai ESCALATE — ei koskaan BLOCK (ks. moduulin docstring)."""
    if e.confidence < CONFIDENCE_THRESHOLD:
        return Result(
            Decision.ESCALATE, None,
            f"Luottamus {e.confidence:.2f} < kynnys {CONFIDENCE_THRESHOLD:.2f} — ei arvata.",
        )
    if e.flagged:
        return Result(
            Decision.ESCALATE, None,
            "Kielletty ilmaus tunnistettu korkealla luottamuksella — ihminen vahvistaa.",
            evidence=("avainsanaosuma",),
        )
    return Result(Decision.PASS, None, "Ei osumaa kielto- tai tarkkailulistaan.")
