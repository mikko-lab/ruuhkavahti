import { useEffect, useRef, useState } from "react";
import { AssignmentStrategy, MetricsSnapshot } from "../types";

type ExportStatus = "idle" | "running" | "done" | "error";

// Tyhjä = sama origin kuin sivu itse; Vite proxyaa /api:n backendille
// (ks. vite.config.ts), joten selain ei tarvitse koskaan backendin
// todellista osoitetta — toimii sellaisenaan myös Codespacesin kaltaisen
// forwarded-URL:n takana.
const API_BASE = (import.meta as any).env?.VITE_BACKEND_HTTP ?? "";

interface ControlsProps {
  snapshot: MetricsSnapshot;
  demoMode?: boolean;
}

export function Controls({ snapshot, demoMode = false }: ControlsProps) {
  const [desiredCount, setDesiredCount] = useState(1);
  const [scaleCommand, setScaleCommand] = useState("");
  const [scaleCopied, setScaleCopied] = useState(false);
  const [assignorCommand, setAssignorCommand] = useState("");
  const [assignorCopied, setAssignorCopied] = useState(false);
  const [triggering, setTriggering] = useState(false);
  const [exportStatus, setExportStatus] = useState<ExportStatus>("idle");
  const [exportError, setExportError] = useState<string | null>(null);
  const pollRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
    };
  }, []);

  async function handleTriggerSpike() {
    setTriggering(true);
    try {
      await fetch(`${API_BASE}/api/trigger-spike`, { method: "POST" });
    } finally {
      setTimeout(() => setTriggering(false), 1000);
    }
  }

  async function handleSliderChange(count: number) {
    setDesiredCount(count);
    const r = await fetch(`${API_BASE}/api/scale-command?count=${count}`);
    const data = await r.json();
    setScaleCommand(data.command);
    setScaleCopied(false);
  }

  async function handleStrategyChange(strategy: AssignmentStrategy) {
    const r = await fetch(`${API_BASE}/api/assignor-command?strategy=${strategy}`);
    const data = await r.json();
    setAssignorCommand(data.command);
    setAssignorCopied(false);
  }

  async function copyTo(text: string, setCopied: (v: boolean) => void) {
    if (!text) return;
    await navigator.clipboard.writeText(text);
    setCopied(true);
  }

  // Export Video: ajaa ?demo=true-käsikirjoituksen erillisessä
  // video-exporter-palvelussa (oikea Chromium + Playwright, ei tämä sivu
  // itse), joten sama ~46s nauhoitus (ks. demoScript.ts) tuottaa joka
  // kerta identtisen MP4:n ilman OBS:ää. Pollaa tilaa kunnes valmis.
  async function handleExportVideo() {
    setExportError(null);
    setExportStatus("running");
    try {
      await fetch(`${API_BASE}/api/export-video`, { method: "POST" });
    } catch {
      setExportStatus("error");
      setExportError("Nauhoituksen käynnistys epäonnistui — onko video-exporter käynnissä?");
      return;
    }
    pollRef.current = window.setInterval(async () => {
      try {
        const r = await fetch(`${API_BASE}/api/export-video/status`);
        const data = await r.json();
        setExportStatus(data.status);
        if (data.status === "error") setExportError(data.error ?? "Tuntematon virhe");
        if (data.status === "done" || data.status === "error") {
          if (pollRef.current) window.clearInterval(pollRef.current);
          pollRef.current = null;
        }
      } catch {
        // väliaikainen verkkovirhe pollauksessa — yritetään uudelleen seuraavalla kierroksella
      }
    }, 2000);
  }

  // Demo Mode: piikki laukeaa automaattisesti (ks. useDemoMode.ts) ja
  // kuluttajien skaalaus/strategian vaihto ovat presenterin omia,
  // etukäteen valmisteltuja terminaalikomentoja (ks. README "Demo Mode") —
  // manuaaliset napit/liukusäätimet/command-boxit vain veisivät tilaa
  // siististä nauhoituksesta. Live-data (kuluttajamäärä, tila) pysyy näkyvissä.
  if (demoMode) {
    return (
      <div className="controls controls--demo">
        <div className="active-consumers active-consumers--demo">
          Aktiiviset kuluttajat: <strong>{snapshot.active_consumers}</strong>
        </div>
        <div className="producer-status">
          Tila: <strong>{snapshot.producer_mode === "spike" ? "PIIKKI" : "normaali"}</strong>
          {" · "}~{snapshot.producer_rate.toLocaleString("fi-FI")} viestiä/s
        </div>
      </div>
    );
  }

  return (
    <div className="controls">
      <button className="spike-button" onClick={handleTriggerSpike} disabled={triggering}>
        {triggering ? "Piikki käynnissä…" : "Laukaise piikki"}
      </button>

      <div className="scale-control">
        <label>
          Tavoiteltu kuluttajamäärä: {desiredCount}
          <input
            type="range"
            min={1}
            max={4}
            value={desiredCount}
            onChange={(e) => handleSliderChange(Number(e.target.value))}
          />
        </label>
        <div className="active-consumers">
          Aktiiviset kuluttajat (live, Kafkan ryhmämetadata): <strong>{snapshot.active_consumers}</strong>
        </div>
        {scaleCommand && (
          <div className="command-box">
            <code>{scaleCommand}</code>
            <button onClick={() => copyTo(scaleCommand, setScaleCopied)}>
              {scaleCopied ? "Kopioitu" : "Kopioi"}
            </button>
          </div>
        )}
      </div>

      <div className="assignor-control">
        <span className="assignor-label">Rebalance-strategia (nykyinen: {snapshot.assignment_strategy})</span>
        <div className="assignor-buttons">
          <button onClick={() => handleStrategyChange("cooperative-sticky")}>
            Cooperative-sticky (moderni)
          </button>
          <button onClick={() => handleStrategyChange("range")}>Eager (classic)</button>
        </div>
        {assignorCommand && (
          <div className="command-box">
            <code>{assignorCommand}</code>
            <button onClick={() => copyTo(assignorCommand, setAssignorCopied)}>
              {assignorCopied ? "Kopioitu" : "Kopioi"}
            </button>
          </div>
        )}
      </div>

      <div className="export-video-control">
        <button
          className="export-video-button"
          onClick={handleExportVideo}
          disabled={exportStatus === "running"}
        >
          {exportStatus === "running" ? "Nauhoitetaan… (~50 s)" : "Export Video"}
        </button>
        {exportStatus === "done" && (
          <a className="export-video-download" href={`${API_BASE}/api/export-video/download`}>
            Lataa ruuhkavahti-demo.mp4
          </a>
        )}
        {exportStatus === "error" && (
          <p className="export-video-error">Virhe: {exportError}</p>
        )}
      </div>

      <div className="producer-status">
        Tila: <strong>{snapshot.producer_mode === "spike" ? "PIIKKI" : "normaali"}</strong>
        {" · "}~{snapshot.producer_rate.toLocaleString("fi-FI")} viestiä/s
      </div>
    </div>
  );
}
