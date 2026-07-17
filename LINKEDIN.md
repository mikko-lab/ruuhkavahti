# LinkedIn-julkaisu — Ruuhkavahti

## Postauksen kuva

Pysäytyskuva huippuhetkestä demoa ajaessa: lag-mittari punaisella (piikin huipulla), 3D-partikkelivirta paksuimmillaan. Ota kuvakaappaus dashboardista hetkellä jolloin `total_lag` on korkeimmillaan yhdellä kuluttajalla, ennen kuin skaalaat.

## Postausteksti (qubit-harness-formaatti: 2-lauseen kontradiktio-hook, nuolilistat, kommentoiva lopetuskysymys, 3 kapeaa hashtagia)

```
Most "handle the traffic spike" demos show you a graph going up. This one shows you the exact number that tells you whether you're still in control — and what happens the moment you're not.

→ Kafka topic partitioned by viewer_id: same viewer's messages stay ordered, different viewers process in parallel across 4 partitions
→ A deterministic PASS/ESCALATE/BLOCK guardrail — reused verbatim from an earlier safety-layer project, not rebuilt — decides every message, no LLM call on the hot path
→ Consumer lag per partition is the one metric that proves it: spike hits, lag climbs red; scale consumers 1→4, lag drops live, same running system

What's the one metric you'd trust to tell you a system is falling behind before anyone complains?

#ApacheKafka #SystemDesign #DataEngineering
```

## Ensimmäinen kommentti / repo-README-upotus

10-15 sekunnin klippi jossa:
1. Piikki laukaistaan ("Laukaise piikki" -nappi) yhdellä kuluttajalla käynnissä — lag-mittari nousee punaiselle parissa sekunnissa
2. `docker compose up -d --scale guardrail-consumer=4` ajetaan toisessa terminaalissa
3. Lag-mittari palautuu vihreälle reaaliajassa, partikkelivirta tasoittuu

Tallenna esim. `asciinema` (terminaali) + näytön videokaappaus (dashboard) rinnakkain, tai pelkkä dashboard-ruutu jos komento näkyy jo kopioitavana laatikkona UI:ssa. Tämä on se osa joka todistaa pointin dynaamisesti — teksti ja pysäytyskuva vain herättävät kiinnostuksen.

## Toinen hook: "sama demo, prefers-reduced-motion päällä"

Yhdistää molemmat tavaramerkit (deterministiset guardrailit + a11y) yhdessä postauksessa. Julkaistaan erillisenä seurantapostauksena tai saman postauksen toisena kuvana/klippinä.

**Kuva/klippi:** käyttöjärjestelmän "vähennä liikettä" -asetus päällä, dashboard näyttää täsmälleen saman lagin ja päätösjakauman `LagGauge`-näkymänä ilman 3D-kohtausta — vieressä sama hetki oletusnäkymällä, jotta ero näkyy suoraan.

```
Most "accessible" dashboards mean a stripped-down version with less data. This one means the exact same data, just without the parts your vestibular system didn't ask for.

→ prefers-reduced-motion swaps the 3D particle scene for the same live lag/decision data, zero motion — not a lite mode, same WebSocket feed
→ Every metric also exists as real semantic HTML (a table, an aria-live region) — the canvas is aria-hidden because a screen reader can't read pixels no matter how good they look
→ Automated proof, not a claim: axe-core scans both modes in CI-style tests — 0 violations, checked, not assumed

Where else have you seen "flashy" and "accessible" treated as the same design problem instead of a trade-off?

#Accessibility #WebGL #FrontendEngineering
```

## Kolmas hook: "rebalancing, näkyvänä"

Kolmas, viimeinen postaus samasta demosta — Kafkan sisäinen mekaniikka, jota useimmat dashboardit eivät koskaan näytä.

**Kuva/klippi:** dashboardin tilabanneri ("Rebalancing partition assignments…") näkyvissä skaalaushetkellä, mieluiten kaksi rinnakkaista klippiä/kuvaa — sama skaalaus eager- ja cooperative-sticky-strategialla, jotta ero (kaikki putket pysähtyvät vs. vain kaksi) näkyy suoraan vierekkäin.

```
Most Kafka demos treat rebalancing as an implementation detail you're not supposed to see. This one puts it on screen — and shows you exactly which strategy freezes two pipes instead of four.

→ Cooperative-sticky (KIP-429) revokes only the partitions that actually change owners; classic eager assignors revoke everything, every time — toggle it live and watch the difference in the particle flow
→ At-least-once delivery makes duplicates a certainty, not an edge case — a (partition, offset)-keyed cache filters them before a moderation decision reaches a human twice, using Kafka's own coordinates instead of inventing a message ID
→ Both honestly scoped, not oversold: the strategy swap needs a restart (shown as a copy-paste command, no hidden automation), and the dedup cache is process-memory only — flagged in the README, not glossed over

What's one piece of distributed-systems "internals" you wish more dashboards actually showed instead of hiding?

#ApacheKafka #DistributedSystems #BackendEngineering
```

