"""
Saavutettavuustesti dashboardille axe-core:lla (samaa työkalua kuin muualla
a11y-työssä). Ei Kafka-riippuvuutta: frontend renderöityy täysin alkutilassa
(EMPTY_SNAPSHOT) vaikka WebSocket-yhteys backendiin epäonnistuisi, joten
brokeria tai backendia ei tarvitse käynnistää tätä testiä varten.

Ajo-ohjeet:
    cd dashboard/frontend && npm install   # axe-core tulee mukana devDependencynä
    pip install -r tests/requirements.txt
    playwright install chromium
    python3 -m pytest tests/test_a11y.py -v

(Tässä hiekkalaatikossa käytetty valmiiksi asennettua Chromiumia
PLAYWRIGHT_CHROMIUM_EXECUTABLE-ympäristömuuttujan kautta, ks. alla.)
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from contextlib import closing

import pytest
from playwright.sync_api import Browser, sync_playwright

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard", "frontend")
AXE_CORE_PATH = os.path.join(FRONTEND_DIR, "node_modules", "axe-core", "axe.min.js")


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def dev_server():
    port = _free_port()
    proc = subprocess.Popen(
        ["npx", "vite", "--port", str(port), "--strictPort"],
        cwd=FRONTEND_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://localhost:{port}/"
    deadline = time.time() + 60
    ready = False
    while time.time() < deadline:
        try:
            with closing(socket.create_connection(("127.0.0.1", port), timeout=1)):
                ready = True
                break
        except OSError:
            time.sleep(0.5)
    if not ready:
        proc.terminate()
        raise RuntimeError("vite-kehityspalvelin ei käynnistynyt 60 sekunnissa")

    yield url

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="module")
def browser():
    executable = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
    launch_kwargs = {"executable_path": executable} if executable else {}
    with sync_playwright() as p:
        b = p.chromium.launch(**launch_kwargs)
        yield b
        b.close()


def _run_axe(page) -> dict:
    assert os.path.exists(AXE_CORE_PATH), (
        f"axe-core puuttuu: {AXE_CORE_PATH} — aja `npm install` dashboard/frontend-kansiossa"
    )
    with open(AXE_CORE_PATH, encoding="utf-8") as f:
        page.add_script_tag(content=f.read())
    return page.evaluate("async () => await axe.run()")


def _serious_or_worse(results: dict) -> list[dict]:
    return [v for v in results["violations"] if v["impact"] in ("serious", "critical")]


def test_default_view_has_no_serious_a11y_violations(dev_server, browser: Browser):
    page = browser.new_page()
    page.goto(dev_server)
    page.wait_for_selector(".app-header h1")
    page.wait_for_timeout(300)  # anna Three.js-kohtaus alustua

    results = _run_axe(page)
    serious = _serious_or_worse(results)
    assert not serious, json.dumps(serious, indent=2, ensure_ascii=False)
    page.close()


def test_particle_canvas_is_hidden_from_assistive_tech(dev_server, browser: Browser):
    page = browser.new_page()
    page.goto(dev_server)
    page.wait_for_selector(".particle-stream")
    aria_hidden = page.get_attribute(".particle-stream", "aria-hidden")
    assert aria_hidden == "true"
    page.close()


def test_reduced_motion_replaces_canvas_with_same_data(dev_server, browser: Browser):
    context = browser.new_context(reduced_motion="reduce")
    page = context.new_page()
    page.goto(dev_server)
    page.wait_for_selector(".app-header h1")

    assert page.query_selector(".particle-stream") is None
    assert page.query_selector(".total-lag") is not None
    assert page.query_selector(".gauge-grid") is not None
    assert page.query_selector(".reduced-motion-note") is not None

    results = _run_axe(page)
    serious = _serious_or_worse(results)
    assert not serious, json.dumps(serious, indent=2, ensure_ascii=False)

    context.close()


def test_data_table_toggle_is_keyboard_operable(dev_server, browser: Browser):
    page = browser.new_page()
    page.goto(dev_server)
    toggle = page.wait_for_selector(".data-table-toggle")
    assert toggle.get_attribute("aria-expanded") == "false"

    toggle.focus()
    page.keyboard.press("Enter")
    page.wait_for_timeout(100)
    assert toggle.get_attribute("aria-expanded") == "true"

    table = page.query_selector(".data-table-wrapper table")
    assert table is not None
    page.close()


def test_live_region_present_for_announcements(dev_server, browser: Browser):
    page = browser.new_page()
    page.goto(dev_server)
    page.wait_for_selector(".app-header h1")
    live_region = page.query_selector('[aria-live="polite"]')
    assert live_region is not None
    page.close()
