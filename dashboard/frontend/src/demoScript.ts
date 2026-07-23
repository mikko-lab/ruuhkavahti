/**
 * Ruuhkavahti — Demo Mode -aikajana (?demo=true).
 *
 * Yksi kiinteä ~46s käsikirjoitus, jotta OBS-nauhoitus on toistettavissa
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
export const DEMO_TRIGGER_SPIKE_AT_MS = 9_000;

// Presenterin oma vihje ulkoiselle terminaalille — dashboard EI aja
// docker-komentoja itse (sama periaate kuin Controls.tsx:n kopioitavat
// komennot). Ajoita oma `docker compose up -d --scale guardrail-consumer=4`
// tähän kohtaan nauhoitusta.
export const DEMO_SCALE_CUE_AT_MS = 16_000;

export const DEMO_DURATION_MS = 46_000;

export const DEMO_CAPTIONS: DemoCaption[] = [
  // Avaustekstitys antaa katsojalle kontekstin heti — ilman tätä "Normal
  // operation" on merkityksetön, koska skenaario (TV-lähetys, katsojachat)
  // ei muuten näy kuvassa lainkaan.
  { start: 0, end: 4_000, text: "Simulating a live TV broadcast traffic spike" },
  { start: 4_000, end: 9_000, text: "Normal operation" },
  { start: 9_000, end: 11_000, text: "Traffic spike detected" },
  { start: 11_000, end: 16_000, text: "Consumer lag increasing" },
  { start: 16_000, end: 17_500, text: "Scaling consumer group…" },
  { start: 17_500, end: 20_000, text: "Kafka rebalance in progress" },
  { start: 20_000, end: 29_000, text: "Additional consumers processing backlog" },
  { start: 29_000, end: 35_000, text: "Lag recovering" },
  { start: 35_000, end: 38_000, text: "System stable" },
  // Ilman tätä riviä koko "riippumaton kolmas kuluttaja" -tarina näkyy vain
  // README:ssä/DEEP_DIVE.md:ssä, ei koskaan demossa itsessään — sidebarin
  // AnalyticsConsumerPanel on jo näkyvissä koko demon ajan, tämä vain
  // nimeää mitä siinä katsotaan juuri tässä kohtaa.
  { start: 38_000, end: 42_000, text: "Independent analytics consumer reading the same stream" },
  // Lopputekstitys kiteyttää pointin: järjestelmä ei vain palautunut, vaan
  // ei koskaan lakannut tekemästä turvapäätöksiä piikin aikana.
  { start: 42_000, end: DEMO_DURATION_MS, text: "Deterministic guardrails stayed online during the spike" },
];
