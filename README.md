**English** | [Suomi](README.fi.md)

# Ruuhkavahti

*A Kafka-based real-time guardrail demo*

> **TL;DR** — A simulated live-TV viewer chat, moderated in real time by a deterministic PASS/ESCALATE/BLOCK safety layer running inside a scalable Kafka consumer group. The dashboard shows **live consumer lag per partition** — the one number that actually proves whether the system keeps pace with a spike and whether adding consumers helps — while the whole thing is WCAG 2.1/2.2 AA accessible, not bolted on afterward but proven as rigorously as the lag metric itself.

**Scenario:** A live TV broadcast where viewer-message volume spikes momentarily (e.g. at a goal). Every message is moderated in real time without the surge taking the service down or dropping messages.

**Principle: "Flag it, don't hide it."** No decision disappears untraceably — every message ends up on one of three traceable paths: `approved`, `escalated`, or `blocked`. The same principle applies to the metrics: the numbers below are measured from the actual running stack, not estimated (see "Results").

---

![Demo: spike + lag recovery](docs/demo.gif)

*Watch the full 50-second recording with captions: [docs/demo.mp4](docs/demo.mp4). Both were generated automatically with the "Export Video" button (see "Demo Mode" below) — the same, repeatable script every time, not manually recorded.*

## What this demonstrates

- **Consumer lag is the one metric that doesn't lie.** It rises during the spike, and adding consumers shows up directly in it in real time — other metrics (e.g. total throughput) can stay nearly flat even when lag doesn't (see "Results").
- **Accessibility is a second, equally load-bearing claim.** `prefers-reduced-motion` swaps the 3D particle stream for the same data without continuous motion, every metric exists as real semantic HTML in addition to pixels, and `tests/test_a11y.py` proves this automatically (axe-core, both modes).
- **Eager vs. cooperative-sticky (KIP-429) is made visible live**, including an honest account of when the difference actually shows up (see "Results" and DEEP_DIVE.md, Finnish).
- **At-least-once + idempotency is solved at the design level**, not skipped: (partition, offset)-based duplicate filtering, measured with a real fault-injection experiment, not asserted.
- **Core-platform primitives are included, not just the guardrail demo.** An independent consumer connected to the same event stream, signed service-to-service auth, and shared tracing through Kafka headers into Jaeger — see "Core platform extension" in DEEP_DIVE.md (Finnish).

## Architecture

```
[Viewer Simulator]  →  Kafka topic: viewer-messages  →  [Guardrail Consumer Group]
   (producer,             (4 partitions,                  (1-4 parallel workers,
    adjustable              key = viewer_id                 deterministic
    send rate,               to preserve                     PASS/ESCALATE/BLOCK)
    "spike" mode)            ordering)
                                                                      │
                              ┌───────────────────────────────────────┼───────────────────────┐
                              ▼                                       ▼                       ▼
                    Kafka topic:                          Kafka topic:              Kafka topic:
                    approved-messages                      escalated-messages         blocked-messages
                    (to screen)                             (to human moderator)       (audit log)
                              │
                              ▼
                    [Dashboard / Visualization]
                    - live consumer lag per partition
                    - decision distribution (pass/escalate/block) in real time
                    - throughput latency (p50/p95)
```

The guardrail logic builds on (partly vendored, partly new) [`mikko-lab/refuse-dont-guess`](https://github.com/mikko-lab/refuse-dont-guess) — the exact boundary is documented in DEEP_DIVE.md (Finnish).

The same `approved/escalated/blocked-messages` topics are also read by an independent `analytics-consumer` (its own consumer group, its own retention, a signed HTTP interface) — pipeline tracing runs through Kafka headers into Jaeger (`localhost:16686`). See "Core platform extension" in DEEP_DIVE.md (Finnish).

## Running

### Without your own machine (e.g. iPad / Chromebook) — GitHub Codespaces

1. Open the repo on GitHub: `github.com/mikko-lab/ruuhkavahti`
2. **Code** → **Codespaces** tab → **Create codespace on main**
3. Run the Docker commands below in the terminal as normal.
4. The **Ports** tab exposes `5173` (dashboard) and `8000` (backend) as public preview links.

### Locally

```bash
docker compose up -d --build
# wait for kafka-init to have created the topics (docker compose logs kafka-init)
```

Once the stack is running, open the live dashboard: **[http://localhost:5173](http://localhost:5173)** (the full UI with controls — captioned Demo Mode version: [http://localhost:5173/?demo=true](http://localhost:5173/?demo=true)). Jaeger UI for tracing: **[http://localhost:16686](http://localhost:16686)** (select service `producer`, `guardrail-consumer`, or `analytics-consumer`).

```bash
# unit and accessibility tests without Kafka
python3 -m unittest tests/test_guardrail_logic.py tests/test_dedup.py tests/test_platform_extension.py -v
cd dashboard/frontend && npm install && cd ../..
pip install -r tests/requirements.txt && playwright install chromium
python3 -m pytest tests/test_a11y.py -v

# scale consumers in the live demo
docker compose up -d --scale guardrail-consumer=4
```

The dashboard's "Trigger spike" button calls the producer's `/trigger-spike` endpoint directly (a genuine live control). The consumer-count and strategy selectors show a copyable command instead of driving Docker from inside the container — a deliberate security choice, not a `docker.sock` mount into a backend service.

### Demo Mode (for single-take recording)

`http://localhost:5173/?demo=true` starts a fixed ~46-second script (`dashboard/frontend/src/demoScript.ts`) so an OBS recording plays back identically every time: an opening caption ("Simulating a live TV broadcast traffic spike") gives the viewer context immediately, the spike triggers automatically at t=9s, fade captions follow the script (`aria-hidden`, not announced to screen readers), manual controls hide, the 3D camera's orbit motion is frozen — motion comes only from the data — and a closing caption ("Deterministic guardrails stayed online during the spike") drives the point home. It respects `prefers-reduced-motion` normally. Consumer scaling (`docker compose up -d --scale guardrail-consumer=4`) is still the presenter's own manual step in a second terminal — the caption "Scaling consumer group…" is a timing cue, not automation (same `docker.sock` restriction as above). Rehearse the timing once before the actual take.

**The Export Video button** (at the bottom of the sidebar, not shown in demo mode itself) makes OBS unnecessary: it calls a separate `video-exporter` service (its own container, Playwright + Chromium + ffmpeg) that runs the `?demo=true` script in a real browser at 1920×1080, records it, and converts it to H.264 MP4 (~50 s total, the finished file auto-downloads from a link that appears on the button). The same command produces an identical video every time — no manual recording, no camera/mic setup. `dashboard-backend` acts as a thin proxy (`/api/export-video`), the same principle as the other controls.

## Results

Measured against the real running stack with `scripts/measure.py` (not a simulation) on a local machine (Docker 29.6.1, KRaft Kafka, 4 partitions), an 8,000 msg/s spike lasting ~18 s, 200 msg/s baseline. **Single-run results** (n=1 per scenario), not repeated measurements with standard deviations — raw data and method: `results.json`.

**Throughput and latency during the spike, as a function of consumer count:**

| Consumers | Throughput (msg/s) | p50 (ms) | p95 (ms) | Peak spike lag | Recovery after spike |
|---|---|---|---|---|---|
| 1 | 7719 | 8.9 | 13.3 | 1489 | 3.35 s |
| 2 | 7830 | 7.8 | 12.2 | 659 | 3.88 s |
| 4 | 7704 | 7.0 | 9.2 | 400 | 4.43 s |

**Note — flagged, not hidden:** total throughput and recovery time stay nearly constant regardless of consumer count: an 8,000 msg/s spike and lightweight keyword scanning aren't enough to make a single consumer a bottleneck in this environment. The real, measurable benefit shows up in **peak lag during the spike**, which falls almost linearly as consumer count increases (1489 → 659 → 400) — more consumers keep the queue shorter throughout the spike, even though the end state after the spike is the same.

**Rebalance pause when scaling 1 → 4 consumers:**

| Strategy | Group coordinator pause | Partitions stopped |
|---|---|---|
| cooperative-sticky | 2.79 s | 4 / 4 |
| eager (range) | 2.65 s | 4 / 4 |

**Note:** when scaling 1→4, all 4 partitions inevitably change ownership regardless of strategy (one original consumer held all four) — so cooperative-sticky's "only the moving partitions stop" advantage doesn't concretely show up here. In preliminary, uninstrumented runs, coordinator-pause variance was large (0.76 s – 6.57 s); n=1 per strategy, not a precise benchmark. Per-partition data and explanation: DEEP_DIVE.md (Finnish).

**Duplicate filtering** (`docker pause` for 50 s on a single consumer, not a restart — the DedupCache stays in memory):

| Messages processed during the test | Duplicates filtered |
|---|---|
| 12,251 | 0 |

**Note:** zero is not a measurement error — per-message synchronous commit (see DEEP_DIVE.md, Finnish) makes the window of uncommitted messages so narrow that this experiment produced no duplicates at all. The mechanism is proven at the unit level (`tests/test_dedup.py`); this experiment instead demonstrates how rarely at-least-once redelivery actually triggers.

## Accessibility

The same data in three parallel presentations, not a "main version" plus a stripped-down fallback:

| Presentation | When shown | Component |
|---|---|---|
| 3D particle stream | default, `prefers-reduced-motion: no-preference` | `ParticleFlow3D.tsx` (`aria-hidden="true"`) |
| 2D gauge view | `prefers-reduced-motion: reduce` | `LagGauge.tsx` (same component in both modes) |
| Semantic `<table>` | always available, behind a button | `AccessibleDataTable.tsx` |

In addition: `LiveAnnouncer.tsx` announces lag-level changes and the start/end of a spike as text (`aria-live="polite"`, not every update); color is never the only signal (always a number + `aria-label`); every control is keyboard-operable, with visible `:focus-visible`. **Proof, not a claim:** `tests/test_a11y.py` runs axe-core in both default and `reduced-motion` mode — **0 findings, 36 passes**, in both modes.

## Limitations (flag it, don't hide it)

- **A real toxicity classifier** → replaced with keyword scanning (`chat_rule.py`); the point is the safety layer's structure, not content-classification accuracy.
- **Kafka transactions / exactly-once** → a deliberate choice in favor of at-least-once semantics. Duplicate processing is an accepted risk.
- **Duplicate filtering doesn't survive a consumer restart** → `DedupCache` lives in the process's memory, capped at 500 messages. The measured duplicate rate (0/12,251) is partly a result of the per-message commit model itself — it doesn't mean the mechanism is unnecessary, only that its natural trigger rate is low in this architecture. A production-grade alternative: persistent dedup storage (Redis/database) or a Kafka transactional producer. See DEEP_DIVE.md (Finnish).
- **Throughput/recovery time don't differentiate consumer count in this environment** → see "Results": an 8,000 msg/s spike plus lightweight moderation logic isn't enough to bottleneck a single consumer. Peak spike lag, by contrast, differentiates clearly.
- **The rebalance-pause measurement is n=1 per strategy, not repeated** → observed run-to-run variance was significant in preliminary runs. Run `scripts/measure.py rebalance` multiple more times before treating the numbers as a precise benchmark.
- **Internal auth is a shared static secret, not mTLS or rotated** → `INTERNAL_SHARED_SECRET` protects only *who* can call `analytics-consumer`, not the transport (no TLS between services). A single service compromise compromises the whole internal network. See `shared/internal_auth.py`.
- **Tracing is coarsely sampled and not persistent** → 2% head-based sampling because of the spike, not based on error state (ESCALATE/BLOCK is not traced any more reliably than PASS). Jaeger is a single in-memory instance — span data disappears when the container stops; not fit for audit use. See `shared/tracing.py`.
- **axe-core covers what's automatically detectable** → typically around 30-50% of WCAG issues; manual screen-reader testing (VoiceOver/NVDA) is missing, flagged here.

---

*See also: [mikko-lab/refuse-dont-guess](https://github.com/mikko-lab/refuse-dont-guess) — the deterministic safety layer this demo's guardrail logic is drawn from.*

**Full technical deep dive:** [DEEP_DIVE.md](DEEP_DIVE.md) (Finnish)

*Part of [mikko-lab](https://github.com/mikko-lab/mikko-lab)'s deterministic-systems portfolio.*
