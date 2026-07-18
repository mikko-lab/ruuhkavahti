/**
 * Ruuhkavahti — Demo Mode -aikajana (?demo=true).
 *
 * Yksi kiinteä ~38s käsikirjoitus, jotta OBS-nauhoitus on toistettavissa
 * identtisenä joka kerta: sama tekstitysrytmi riippumatta siitä miten
 * nopeasti oikea Kafka-taustajärjestelmä reagoi. Kello käynnistyy sivun
 * latautuessa, ei ensimmäisestä metriikkapäivityksestä.
 */

export interface DemoCaption {
  start: number; // ms demo-kellon alusta
  end: number;
  text: string;
}

// Sama hetki kuin Controls.tsx:n "Laukaise piikki" -painike kutsuisi.
export const DEMO_TRIGGER_SPIKE_AT_MS = 5_000;

// Presenterin oma vihje ulkoiselle terminaalille — dashboard EI aja
// docker-komentoja itse (sama periaate kuin Controls.tsx:n kopioitavat
// komennot). Ajoita oma `docker compose up -d --scale guardrail-consumer=4`
// tähän kohtaan nauhoitusta.
export const DEMO_SCALE_CUE_AT_MS = 12_000;

export const DEMO_DURATION_MS = 38_000;

export const DEMO_CAPTIONS: DemoCaption[] = [
  { start: 0, end: 5_000, text: "Normal operation" },
  { start: 5_000, end: 7_000, text: "Traffic spike detected" },
  { start: 7_000, end: 12_000, text: "Consumer lag increasing" },
  { start: 12_000, end: 13_500, text: "Scaling consumer group…" },
  { start: 13_500, end: 16_000, text: "Kafka rebalance in progress" },
  { start: 16_000, end: 25_000, text: "Additional consumers processing backlog" },
  { start: 25_000, end: 31_000, text: "Lag recovering" },
  { start: 31_000, end: DEMO_DURATION_MS, text: "System stable" },
];
