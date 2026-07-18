import { useEffect, useRef, useState } from "react";
import { DEMO_CAPTIONS, DEMO_DURATION_MS, DEMO_TRIGGER_SPIKE_AT_MS } from "./demoScript";

// Sama origin-relatiivinen kutsutapa kuin Controls.tsx (ks. sen kommentti) —
// toimii paikallisesti ja Codespacesin kaltaisen forwarded-URL:n takana.
const API_BASE = (import.meta as any).env?.VITE_BACKEND_HTTP ?? "";

export interface DemoModeState {
  active: boolean;
  caption: string | null;
}

/**
 * ?demo=true käynnistää kiinteän aikajanan (ks. demoScript.ts): laukaisee
 * piikin automaattisesti t=5s, näyttää skriptin mukaiset tekstitykset.
 * Ei tee mitään jos parametria ei ole — täysin turvallinen lisäys normaaliin
 * käyttöön.
 */
export function useDemoMode(): DemoModeState {
  const [active] = useState<boolean>(
    () => new URLSearchParams(window.location.search).get("demo") === "true"
  );
  const [caption, setCaption] = useState<string | null>(null);
  const triggeredRef = useRef(false);

  useEffect(() => {
    if (!active) return;

    const start = performance.now();
    const interval = setInterval(() => {
      const elapsed = performance.now() - start;

      if (!triggeredRef.current && elapsed >= DEMO_TRIGGER_SPIKE_AT_MS) {
        triggeredRef.current = true;
        fetch(`${API_BASE}/api/trigger-spike`, { method: "POST" }).catch(() => {
          // Demo-kello jatkuu vaikka kutsu epäonnistuisi — tekstitykset eivät
          // saa jäädä riippumaan yhdestä verkkopyynnöstä.
        });
      }

      const current = DEMO_CAPTIONS.find((c) => elapsed >= c.start && elapsed < c.end);
      setCaption(current ? current.text : null);

      if (elapsed > DEMO_DURATION_MS + 2_000) {
        clearInterval(interval);
      }
    }, 150);

    return () => clearInterval(interval);
  }, [active]);

  return { active, caption };
}
