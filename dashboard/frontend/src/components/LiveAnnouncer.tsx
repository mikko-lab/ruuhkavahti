import { useEffect, useRef, useState } from "react";
import { MetricsSnapshot } from "../types";
import { severityLabel } from "./LagGauge";

const GAUGE_MAX = 6000;

/**
 * aria-live=polite -alue: ilmoittaa tekstinä lag-tason muutokset ja piikin
 * alkamisen/päättymisen. Ei kerro jokaista päivitystä (se hukuttaisi
 * ruudunlukijan) — vain kun jokin todella muuttuu: vakavuustaso tai
 * producer_mode.
 */
export function LiveAnnouncer({ snapshot }: { snapshot: MetricsSnapshot }) {
  const [message, setMessage] = useState("");
  const lastSeverity = useRef<string | null>(null);
  const lastMode = useRef<string | null>(null);

  useEffect(() => {
    const ratio = Math.min(1, snapshot.total_lag / (GAUGE_MAX * 2));
    const severity = severityLabel(ratio);

    if (lastMode.current !== null && lastMode.current !== snapshot.producer_mode) {
      setMessage(
        snapshot.producer_mode === "spike"
          ? "Piikki alkoi: viestien tuotantonopeus nousi merkittävästi."
          : "Piikki päättyi: tuotantonopeus palasi normaalille tasolle."
      );
    } else if (lastSeverity.current !== null && lastSeverity.current !== severity) {
      setMessage(`Jonopituuden taso muuttui: ${severity} (${snapshot.total_lag} viestiä jonossa).`);
    }

    lastSeverity.current = severity;
    lastMode.current = snapshot.producer_mode;
  }, [snapshot.total_lag, snapshot.producer_mode]);

  return (
    <div aria-live="polite" className="sr-only">
      {message}
    </div>
  );
}
