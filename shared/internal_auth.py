"""
Ruuhkavahti — sisäinen palveluiden-välinen autentikointi (HMAC-allekirjoitus).

Core-platform-laajennus (ks. DEEP_DIVE.md "Core platform -laajennus"):
kun useampi palvelu alkaa kutsua toisiaan suoraan (dashboard-backend ->
analytics-consumer) eikä vain Kafkan kautta, tarvitaan jokin tapa varmistaa
että kutsuja on joku muu ruuhkavahti-palvelu eikä kuka tahansa Docker-verkossa.

Ei OAuth2/mTLS (ks. rajoitukset alla) vaan yksinkertaisin toimiva malli:
jaettu salaisuus + HMAC-SHA256 allekirjoitus menetelmästä, polusta ja
aikaleimasta. Aikaleima estää allekirjoitetun pyynnön uudelleenkäytön
ikkunan (DEFAULT_WINDOW_S) jälkeen.

Allekirjoitettava merkkijono: "{method}\\n{path}\\n{timestamp}"
Header: X-Ruuhkavahti-Timestamp, X-Ruuhkavahti-Signature

LIPUTA ÄLÄ PIILOTA — tunnetut rajoitukset:
  - Yksi jaettu salaisuus kaikille palveluille, ei per-palvelu-avaimia eikä
    rotaatiota. Yhden palvelun kompromissi kompromisoi koko sisäisen verkon.
  - Ei nonce-tallennusta: sama allekirjoitettu pyyntö kelpaa uudelleen koko
    aikaikkunan ajan (replay mahdollinen ikkunan sisällä), ei vain kerran.
  - Ei kuljetustason salausta (ei TLS palveluiden välillä) — tämä suojaa
    vain *kuka* kutsuu, ei *mitä* siirretään. Tuotannossa tarvittaisiin
    lisäksi mTLS tai verkkotason eristys.
  - Kellojen pitää pysyä suunnilleen synkassa (NTP) — tässä demossa kaikki
    kontit jakavat hostin kellon, joten ei ole testattu kellovinouman kanssa.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time

DEFAULT_WINDOW_S = 30
SHARED_SECRET_ENV = "INTERNAL_SHARED_SECRET"


class AuthError(Exception):
    pass


def _secret() -> bytes:
    secret = os.environ.get(SHARED_SECRET_ENV)
    if not secret:
        raise AuthError(
            f"{SHARED_SECRET_ENV} puuttuu ympäristömuuttujista — sisäistä "
            "kutsua ei voi allekirjoittaa/varmistaa ilman jaettua salaisuutta."
        )
    return secret.encode("utf-8")


def _canonical(method: str, path: str, timestamp: str) -> bytes:
    return f"{method.upper()}\n{path}\n{timestamp}".encode("utf-8")


def sign(method: str, path: str) -> dict[str, str]:
    """Palauttaa headerit joilla lähtevä sisäinen pyyntö allekirjoitetaan."""
    timestamp = str(int(time.time()))
    signature = hmac.new(_secret(), _canonical(method, path, timestamp), hashlib.sha256).hexdigest()
    return {
        "X-Ruuhkavahti-Timestamp": timestamp,
        "X-Ruuhkavahti-Signature": signature,
    }


def verify(method: str, path: str, timestamp: str | None, signature: str | None,
           window_s: int = DEFAULT_WINDOW_S) -> None:
    """Nostaa AuthError:n jos pyyntöä ei voida hyväksyä. Ei paluuarvoa onnistuessa."""
    if not timestamp or not signature:
        raise AuthError("Puuttuva X-Ruuhkavahti-Timestamp tai X-Ruuhkavahti-Signature.")

    try:
        ts = int(timestamp)
    except ValueError as exc:
        raise AuthError("Aikaleima ei ole kokonaisluku.") from exc

    if abs(time.time() - ts) > window_s:
        raise AuthError(f"Aikaleima ikkunan ({window_s}s) ulkopuolella — mahdollinen replay.")

    expected = hmac.new(_secret(), _canonical(method, path, timestamp), hashlib.sha256).hexdigest()
    # compare_digest: vakioaikainen vertailu timing-hyökkäyksiä vastaan.
    if not hmac.compare_digest(expected, signature):
        raise AuthError("Allekirjoitus ei täsmää.")
