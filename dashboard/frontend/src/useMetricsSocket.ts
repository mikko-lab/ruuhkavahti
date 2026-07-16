import { useEffect, useRef, useState } from "react";
import { EMPTY_SNAPSHOT, MetricsSnapshot } from "./types";

// Sama origin kuin sivu itse (Vite proxyaa /ws:n backendille, ks. vite.config.ts) —
// toimii sellaisenaan sekä paikallisesti että forwarded-URL:n takana (esim. Codespaces),
// koska selain ei tarvitse koskaan tietää backendin todellista osoitetta.
const WS_URL =
  (import.meta as any).env?.VITE_BACKEND_WS ??
  `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws`;

export function useMetricsSocket(): MetricsSnapshot {
  const [snapshot, setSnapshot] = useState<MetricsSnapshot>(EMPTY_SNAPSHOT);
  const retryDelay = useRef(1000);

  useEffect(() => {
    let socket: WebSocket;
    let cancelled = false;
    let retryTimeout: ReturnType<typeof setTimeout>;

    function connect() {
      socket = new WebSocket(WS_URL);
      socket.onmessage = (event) => {
        try {
          setSnapshot(JSON.parse(event.data));
          retryDelay.current = 1000;
        } catch {
          // ohitetaan yksittäinen viallinen kehys
        }
      };
      socket.onclose = () => {
        if (cancelled) return;
        retryTimeout = setTimeout(connect, retryDelay.current);
        retryDelay.current = Math.min(retryDelay.current * 2, 10_000);
      };
      socket.onerror = () => socket.close();
    }

    connect();
    return () => {
      cancelled = true;
      clearTimeout(retryTimeout);
      socket?.close();
    };
  }, []);

  return snapshot;
}
