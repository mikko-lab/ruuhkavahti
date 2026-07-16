"""
Ruuhkavahti — koko päätösputki yhdelle katsojaviestille.

raaka viesti
  → input-guardrail   (scan_for_injection, vendoroitu muuttumattomana)
  → chat-poiminta      (extract_chat_signal, uusi, sama muoto kuin alkuperäisessä)
  → sääntö + kynnys     (apply_chat_rule)
  → PÄÄTÖS ∈ { PASS, ESCALATE, BLOCK }

Sama rakenne kuin vendor/refuse_dont_guess.py:n process() — ks. sen docstring.
Ei Kafka-riippuvuutta, jotta pipeline on testattavissa ilman brokeria
(tests/test_guardrail_logic.py).
"""

from __future__ import annotations

from vendor.refuse_dont_guess import Decision, Result, scan_for_injection
from chat_rule import extract_chat_signal, apply_chat_rule

MAX_CHAT_CHARS = 2_000


def process_message(content: str) -> Result:
    if not isinstance(content, str):
        raise TypeError(f"content oltava str, saatiin {type(content).__name__}")
    if len(content) > MAX_CHAT_CHARS:
        return Result(
            Decision.ESCALATE, None,
            f"Viesti ylittää {MAX_CHAT_CHARS} merkkiä — ei käsitellä automaattisesti, eskaloidaan.",
        )
    inj = scan_for_injection(content)
    if inj is not None:
        return Result(
            Decision.BLOCK, None,
            "Mahdollinen prompt injection torjuttu. Viestiä ei näytetä eikä eskaloida sellaisenaan.",
        )
    return apply_chat_rule(extract_chat_signal(content))
