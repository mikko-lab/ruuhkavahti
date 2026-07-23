import { useEffect, useRef, useState } from "react";

// Sama origin-relatiivinen kutsutapa kuin Controls.tsx/useDemoMode.ts.
const API_BASE = (import.meta as any).env?.VITE_BACKEND_HTTP ?? "";
const POLL_INTERVAL_MS = 2_000;

export interface PlatformMetrics {
  window_s: number;
  bucket_s: number;
  total_consumed_lifetime: number;
  totals_in_window: { PASS: number; ESCALATE: number; BLOCK: number };
  block_rate_in_window: number;
}

export interface PlatformMetricsState {
  data: PlatformMetrics | null;
  // "loading" vain ennen ensimmäistä onnistunutta vastausta; sen jälkeen
  // vanha data jää näkyviin virhetilanteessa (ks. analytics-consumer voi olla
  // hetkellisesti pystyssä ilman että koko dashboard pitäisi näyttää tyhjänä).
  status: "loading" | "ok" | "unreachable";
}

/**
 * Hakee analytics-consumerin liukuvan ikkunan dashboard-backendin
 * allekirjoitetun proxy-päätepisteen kautta (ks. DEEP_DIVE.md "Core platform
 * -laajennus"). Polling, ei WebSocket — tämä on eri palvelu eri
 * päivitystiheydellä kuin /ws:n live-snapshot, ei ole tarkoituskaan olla yhtä
 * reaaliaikainen.
 */
export function usePlatformMetrics(): PlatformMetricsState {
  const [state, setState] = useState<PlatformMetricsState>({ data: null, status: "loading" });
  const dataRef = useRef<PlatformMetrics | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const res = await fetch(`${API_BASE}/api/platform-metrics`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = (await res.json()) as PlatformMetrics;
        if (cancelled) return;
        dataRef.current = json;
        setState({ data: json, status: "ok" });
      } catch {
        if (cancelled) return;
        // Säilytä viimeisin onnistunut data näkyvissä, merkitse vain tila.
        setState({ data: dataRef.current, status: "unreachable" });
      }
    }

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return state;
}
