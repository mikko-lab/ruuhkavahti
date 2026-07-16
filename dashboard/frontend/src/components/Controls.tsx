import { useState } from "react";
import { MetricsSnapshot } from "../types";

// Tyhjä = sama origin kuin sivu itse; Vite proxyaa /api:n backendille
// (ks. vite.config.ts), joten selain ei tarvitse koskaan backendin
// todellista osoitetta — toimii sellaisenaan myös Codespacesin kaltaisen
// forwarded-URL:n takana.
const API_BASE = (import.meta as any).env?.VITE_BACKEND_HTTP ?? "";

export function Controls({ snapshot }: { snapshot: MetricsSnapshot }) {
  const [desiredCount, setDesiredCount] = useState(1);
  const [command, setCommand] = useState("");
  const [copied, setCopied] = useState(false);
  const [triggering, setTriggering] = useState(false);

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
    setCommand(data.command);
    setCopied(false);
  }

  async function handleCopy() {
    if (!command) return;
    await navigator.clipboard.writeText(command);
    setCopied(true);
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
        {command && (
          <div className="command-box">
            <code>{command}</code>
            <button onClick={handleCopy}>{copied ? "Kopioitu" : "Kopioi"}</button>
          </div>
        )}
      </div>

      <div className="producer-status">
        Tila: <strong>{snapshot.producer_mode === "spike" ? "PIIKKI" : "normaali"}</strong>
        {" · "}~{snapshot.producer_rate.toLocaleString("fi-FI")} viestiä/s
      </div>
    </div>
  );
}
