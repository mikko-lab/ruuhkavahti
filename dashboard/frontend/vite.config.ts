import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Selain puhuu vain frontendin omalle, ulos avatulle portille (5173) —
// olipa kyse paikallisesta Dockerista tai etäympäristöstä kuten GitHub
// Codespaces, jossa backend saisi oman, eri isäntänimisen forwarded-URL:n.
// Vite-kehityspalvelin (joka on samassa docker-compose-verkossa kuin backend)
// proxyaa /ws ja /api -pyynnöt sisäisesti dashboard-backend:8000:aan, joten
// selaimen ei tarvitse koskaan tietää backendin osoitetta erikseen.
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      "/ws": {
        target: "ws://dashboard-backend:8000",
        ws: true,
      },
      "/api": {
        target: "http://dashboard-backend:8000",
        changeOrigin: true,
      },
    },
  },
});
