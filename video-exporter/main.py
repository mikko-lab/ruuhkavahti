"""
Ruuhkavahti — video-exporter.

Erillinen, kevyt mikropalvelu (sama periaate kuin producer/dashboard-backend:
yksi vastuu per kontti) joka ajaa dashboardin ?demo=true-käsikirjoituksen
oikealla Chromiumilla Playwrightin kautta, nauhoittaa sen 1920x1080-videona
ja muuntaa lopputuloksen MP4:ksi ffmpegillä. Näin "Export Video" -nappi
dashboardissa tuottaa joka kerta identtisen videon yhdellä API-kutsulla,
ilman OBS:ää tai manuaalista nauhoitusta.

POST /export           - käynnistää nauhoituksen taustalla (idempotentti:
                          jos ajo on jo käynnissä, palauttaa sen tilan)
GET  /status            - nauhoituksen tila: idle / running / done / error
GET  /download          - valmis MP4, kun status == done
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from playwright.sync_api import sync_playwright

FRONTEND_URL = os.environ.get("FRONTEND_URL", "http://dashboard-frontend:5173")
DEMO_DURATION_S = float(os.environ.get("DEMO_DURATION_S", "38"))
# Verkon asettuminen ennen kelloa + viimeisen tekstityksen ("System stable")
# näkymisaika ennen kontekstin sulkemista.
PRE_ROLL_S = 2.0
POST_ROLL_S = 3.0

EXPORT_DIR = Path("/app/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILENAME = "ruuhkavahti-demo.mp4"

app = FastAPI()

_lock = threading.Lock()
_state: dict = {
    "status": "idle",  # idle | running | done | error
    "started_at": None,
    "finished_at": None,
    "error": None,
    "download_url": None,
}


def _record_and_convert() -> None:
    started = time.monotonic()
    tmp_dir = Path(f"/tmp/ruuhkavahti-video-{uuid.uuid4().hex}")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                record_video_dir=str(tmp_dir),
                record_video_size={"width": 1920, "height": 1080},
            )
            page = context.new_page()
            page.goto(f"{FRONTEND_URL}/?demo=true", wait_until="networkidle")
            time.sleep(PRE_ROLL_S)
            time.sleep(DEMO_DURATION_S)
            time.sleep(POST_ROLL_S)
            video = page.video
            context.close()
            browser.close()
            webm_path = Path(video.path()) if video else None

        if webm_path is None or not webm_path.exists():
            raise RuntimeError("Playwright ei tuottanut video-tiedostoa (page.video puuttui)")

        mp4_path = EXPORT_DIR / OUTPUT_FILENAME
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(webm_path),
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-r", "30",
                str(mp4_path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg epäonnistui: {result.stderr[-2000:]}")

        with _lock:
            _state["status"] = "done"
            _state["finished_at"] = time.time()
            _state["download_url"] = "/download"
            _state["error"] = None
    except Exception as exc:  # taustasäie: virhe ei saa jäädä hiljaiseksi
        with _lock:
            _state["status"] = "error"
            _state["finished_at"] = time.time()
            _state["error"] = str(exc)
    finally:
        for f in tmp_dir.glob("*"):
            f.unlink(missing_ok=True)
        tmp_dir.rmdir()
        with _lock:
            _state["elapsed_s"] = round(time.monotonic() - started, 1)


@app.post("/export")
def export_video() -> dict:
    with _lock:
        if _state["status"] == "running":
            return dict(_state)
        _state.update(status="running", started_at=time.time(), finished_at=None, error=None, download_url=None)
    threading.Thread(target=_record_and_convert, daemon=True).start()
    with _lock:
        return dict(_state)


@app.get("/status")
def status() -> dict:
    with _lock:
        return dict(_state)


@app.get("/download")
def download():
    mp4_path = EXPORT_DIR / OUTPUT_FILENAME
    with _lock:
        ready = _state["status"] == "done"
    if not ready or not mp4_path.exists():
        raise HTTPException(status_code=404, detail="Videota ei ole vielä valmiina — kutsu ensin POST /export")
    return FileResponse(mp4_path, media_type="video/mp4", filename=OUTPUT_FILENAME)


@app.get("/health")
def health() -> dict:
    return {"ok": True}
