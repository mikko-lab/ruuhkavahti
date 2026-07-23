# Ruuhkavahti — tekninen syväsukellus

Tämä dokumentti sisältää pääREADME:stä siirretyn syvällisen teknisen sisällön:
uudelleenkäytön rajanveto, konseptioppitunti, rebalance-strategioiden mekaniikka,
idempotenssin toteutus ja repo-rakenne. Ei lyhennelty — täysi versio.

---

## Uudelleenkäyttö vs. uusi — rehellinen rajanveto

Guardrail-logiikka pohjautuu repoon [`mikko-lab/refuse-dont-guess`](https://github.com/mikko-lab/refuse-dont-guess) (vendoroitu muuttumattomana `guardrail/vendor/refuse_dont_guess.py`:hen, commit `1f7e75b8`). Sieltä on **aidosti uudelleenkäytetty sellaisenaan**, koska se ei ole toimialasidonnaista:

- `scan_for_injection()` + `INJECTION_PATTERNS` — tunnistaa tekstiin upotetut ohjausyritykset ("ignore previous instructions", "älä eskaloi" jne.). Tämä pätee identtisesti TV-chatissa: katsoja voi yrittää huijata automoderaattorin päästämään viestinsä läpi samalla tempulla kuin alkuperäisen repon laskudokumentissa.
- `Decision` / `Result` -tyypit ja koko putken muoto: `input-guardrail (BLOCK) → poiminta → luottamuskynnys + sääntö (PASS/ESCALATE)`.

Mikä **ei** siirry: alkuperäinen `apply_rule` päättää rakennusalan käännetyn ALV:n soveltumisesta — se ei tarkoita mitään chat-viestin kohdalla. Tilalle on kirjoitettu `guardrail/chat_rule.py`, joka noudattaa **täsmälleen saman muotoisen** säännön (avainsanapohjainen lippulistaus + luottamuskynnys, palauttaa vain PASS/ESCALATE — ei koskaan BLOCK, sama kuin alkuperäisessä `apply_rule`:ssa). `guardrail/pipeline.py` yhdistää nämä kaksi täsmälleen alkuperäisen `process()`:n muotoiseksi putkeksi.

## Oppimispolku (konseptit koodin takana)

1. **Partitio ja partition key** — `viewer-messages` jaetaan 4 partitioon. `key = viewer_id` takaa, että saman katsojan viestit käsitellään aina järjestyksessä (osuvat samaan partitioon), mutta eri katsojat jakautuvat tasan 4 partitioon — järjestys JA rinnakkaisuus samaan aikaan.
2. **Consumer group ja rebalance** — kaikki `guardrail-consumer`-instanssit kuuluvat groupiin `guardrail-group`. Kafka takaa, ettei kaksi workeria koskaan lue samaa partitiota yhtä aikaa. Kun kuluttajien määrä muuttuu, Kafka **rebalancoi**: partitiot jaetaan uudelleen elossa olevien kuluttajien kesken — tämä on se hetki, kun lisätty kuluttaja alkaa oikeasti purkaa jonoa. Kaksi eri tapaa tehdä tämä — ks. "Rebalance-strategiat" alla.
3. **Offset ja at-least-once-semantiikka** — `enable.auto.commit: False`, commit vasta sen jälkeen kun päätös on kirjoitettu output-topiciin. Jos worker kaatuu ennen committia, viesti luetaan uudelleen — se ei koskaan katoa jäljettömiin (mahdollinen kaksoiskäsittely on hyväksytty kompromissi; kadonnut BLOCK-päätös ei olisi).
4. **Consumer lag mittarina** — `lag = partition_high_watermark - group_committed_offset`. Tämä on ainoa mittari, joka reagoi suoraan sekä tuotantonopeuteen että kuluttajien määrään: piikin aikana lag kasvaa, kuluttajien lisäys (rebalance) saa sen laskemaan reaaliajassa.
5. **Miksi WebGL vaatii aina rinnakkaisen semanttisen esityksen** — `<canvas>` on ruudunlukijalle läpinäkymätön pikselikartta. `prefers-reduced-motion` ei ole "kevyempi versio" vaan sama data ilman jatkuvaa animaatiota (liike voi aiheuttaa esim. vestibulaarioireita); ruudunlukijatuki taas vaatii saman datan olemassaolon oikeana HTML:nä riippumatta liikeasetuksesta. Kaksi eri ongelmaa, osittain päällekkäiset mutta erilliset ratkaisut — ks. pääREADME:n osio "Accessibility".
6. **Eager vs. cooperative-sticky rebalance (KIP-429)** — pysähtyykö rebalancen ajaksi koko kulutus vai vain ne partitiot jotka oikeasti vaihtavat omistajaa? Ks. "Rebalance-strategiat" alla.
7. **Idempotenssi (partition, offset)-avaimella** — miksi Kafkan oma koordinaatti riittää dedup-avaimeksi eikä erillistä viesti-ID:tä tarvitse keksiä, ja miksi duplikaatti ESCALATE/BLOCK olisi oikea guardrail-ongelma. Ks. "Idempotenssi ja duplikaattien suodatus" alla.

## Rebalance-strategiat: Eager vs Cooperative-sticky (KIP-429)

Kun consumer groupin jäsenmäärä muuttuu, kaikkien partitioiden pitää joskus vaihtaa omistajaa. Kysymys on: **pysähtyykö koko ryhmä joka kerta, vai vain se osa joka oikeasti muuttuu?**

- **Eager / klassinen** (`range`, `roundrobin`) — coordinator käskee *kaikkia* kuluttajia luopumaan *kaikista* partitioistaan ensin, jakaa uudelleen vasta sitten. Välissä yksikään partitio ei ole kenenkään luettavana — koko kulutus pysähtyy, vaikka muutos olisi pieni (esim. yksi worker neljästä lisätty).
- **Cooperative-sticky (KIP-429)** — vain partitiot, jotka oikeasti vaihtavat omistajaa, revokoidaan. Loput jatkavat keskeytyksettä. Pienempi, kohdistetumpi katko.

**Tärkeä rajaus mitatuille luvuille (ks. pääREADME "Tulokset"):** 1→4-skaalauksessa yhden kuluttajan omistamat 4 partitiota jakautuvat väistämättä 4:lle uudelle omistajalle KAIKKIEN strategioiden alla — cooperative-stickyn etu ("vain siirtyvät partitiot pysähtyvät") ei tule esiin tässä nimenomaisessa skenaariossa, koska mikään partitio ei voi "pysyä" alkuperäisellä omistajalla kun omistajia on 1→4. Etu näkyisi esim. 2→4- tai 3→4-skaalauksessa, joissa osa kuluttajista säilyttää osan partitioistaan.

Dashboard tekee tämän näkyväksi kahdella tavalla:
- **Tilabanneri** ("Rebalancing partition assignments…") ilmestyy aina rebalancen ajaksi, `aria-live="polite"`-yhteensopivana (sama kaava kuin muut tilailmoitukset, ks. pääREADME:n osio "Accessibility").
- **Partikkelivirran pysähdys** kohdistuu eager-tilassa kaikkiin neljään putkeen; cooperative-sticky-tilassa vain niihin partitioihin, jotka `guardrail_consumer.py`:n `on_assign`/`on_revoke`-callbackit raportoivat siirtyviksi (`rebalance-events`-topic).

Strategian vaihto (dashboardin "Rebalance-strategia"-valitsin) näyttää kopioitavan komennon samalla periaatteella kuin kuluttajaskaalaus — vaihto vaatii kontin uudelleenkäynnistyksen (`partition.assignment.strategy` on Consumer-konfiguraatio, ei ajonaikana muutettavissa):

```bash
ASSIGNMENT_STRATEGY=range docker compose up -d --build guardrail-consumer
```

(Tämä komento vaatii `docker-compose.yml`:n `ASSIGNMENT_STRATEGY: ${ASSIGNMENT_STRATEGY:-cooperative-sticky}`-interpolaation — versiossa jossa se oli kovakoodattu literaaliksi `cooperative-sticky`, komento ei tehnyt mitään. Korjattu.)

## Idempotenssi ja duplikaattien suodatus

At-least-once (ks. "Oppimispolku" kohta 3) tarkoittaa juuri sitä: viesti käsitellään *vähintään* kerran, joskus useammin. Tämä ei ole korjattava bugi — se on tietoinen valinta (mieluummin kaksoiskäsittely kuin kadonnut BLOCK-päätös). Mutta duplikaatin pitää olla harmiton, ei vain "toivottavasti ei tapahdu": duplikaatti `ESCALATE`/`BLOCK` menisi ihmismoderaattorille kahdesti — sama viesti kahdesti hänen jonossaan. Dedup kytkeytyy siis suoraan guardrail-logiikkaan, ei ole irrallinen Kafka-oppitunti.

`guardrail/dedup.py`:n `DedupCache` käyttää dedup-avaimena **(partition, offset)**-paria — ei erikseen keksittyä `message_id`-kenttää. Tämä on Kafkan oma, aidosti uniikki koordinaatti jokaiselle fyysiselle viestille, ja täsmälleen se koordinaatti jonka Kafka itse käyttää seuraamaan missä kohtaa lokia kuluttaja on. Kun `guardrail_consumer.py` havaitsee saman `(partition, offset)`-parin uudelleen, se ei julkaise päätöstä uudelleen output-topiciin — ainoastaan laskee sen `duplicate-events`-topiciin, jota dashboard näyttää laskurina ("Duplikaatteja suodatettu").

**Rehellinen rajaus (liputa, älä piilota):** `DedupCache` on rajattu (500 viestiä) LRU-joukko *prosessin muistissa*. Se **ei selviä** `guardrail-consumer`-kontin omasta uudelleenkäynnistyksestä — silloin cache tyhjenee. Tuotannossa tarvitsisi joko pysyvän dedup-tallennuksen (esim. Redis/tietokanta avaimella `partition:offset` + TTL) tai Kafkan transaktionaalisen/exactly-once-tuottajan. Tämä demo näyttää periaatteen, ei täydellistä ratkaisua.

**Mitattu käyttäytyminen (ks. pääREADME "Tulokset"):** koska `guardrail_consumer.py` commitoi jokaisen viestin heti sen tuoton jälkeen (ei batch-commit), ei-committoitujen viestien ikkuna pausen/session-timeoutin aikana on käytännössä korkeintaan yksi viesti per rebalance-tapahtuma. Todellisessa testissä (`docker pause` 50 s, ei restart) 12 251 käsitellystä viestistä 0 päätyi duplikaatiksi — mekanismi on olemassa ja yksikkötestattu (`tests/test_dedup.py`), mutta tämän arkkitehtuurin luonnollinen duplikaattitiheys on erittäin matala juuri per-viesti-committing-mallin ansiosta.

## Core platform -laajennus

Alkuperäinen Ruuhkavahti oli yksi tuottaja + yksi kuluttajaryhmä + yksi dashboard. Kolme lisäystä osoittavat, mitä tapahtuu kun useampi itsenäinen palvelu alkaa jakaa samaa tapahtumavirtaa — "core platform" -ajattelun ydin:

**1. Toinen, riippumaton kuluttaja samalle datalle.** `analytics-consumer` lukee samoja `approved/escalated/blocked-messages`-topiceja omalla consumer groupillaan (`analytics-group`), täysin riippumatta `dashboard-backend`:n omasta kulutuksesta. Se pitää eri retention-mallin (10s-ämpärit, 1h liukuva ikkuna) kuin dashboardin lyhyt live-näyttö — osoittaakseen että Kafka-topic on jaettu kontrakti, ei yhden kuluttajan yksityisomaisuutta. Tämä on täsmälleen se kuvio jolla toinen tiimi liittyisi olemassa olevaan tapahtumavirtaan koskematta alkuperäiseen putkeen.

**2. Sisäinen palvelu-palvelu-autentikointi.** Kun `dashboard-backend` alkoi kutsua `analytics-consumer`:ia suoraan HTTP:n yli (`GET /api/platform-metrics` → `GET analytics-consumer:8003/metrics`), syntyi ensimmäinen kutsu joka ei kulje Kafkan kautta. `shared/internal_auth.py` allekirjoittaa pyynnön HMAC-SHA256:lla jaetulla salaisuudella (`INTERNAL_SHARED_SECRET`) — menetelmä + polku + aikaleima, aikaikkuna estää suorimman replay-hyökkäyksen. Katso moduulin oma "liputa älä piilota" -osio tarkoista rajoituksista (ei rotaatiota, ei TLS:ää, ei nonce-tallennusta).

**3. Jaettu jäljitys (distributed tracing).** `shared/tracing.py` alustaa OpenTelemetrin OTLP-viejän Jaegeriin (`docker-compose.yml`:n `jaeger`-palvelu, UI portissa 16686). Koska Kafka ei kanna W3C trace-contextia natiivisti, traceparent kuljetetaan viestin headereissa producer → guardrail-consumer → analytics-consumer. Piikin ~8000 msg/s takia jäljitys on **head-based sampled** (`TRACE_SAMPLE_RATE`, oletus 2 %) — producer heittää kolikkoa per viesti ja downstream-palvelut jatkavat tracea vain jos header on läsnä, eivät koskaan avaa uutta itse. Näin Jaeger näkee edustavan otoksen koko putkesta ilman että se tukehtuu kuormaan.

**Rehellinen rajanveto:** nämä kolme yhdessä osoittavat platform-primitiivit (jaettu tapahtumavirta, sisäinen auth, jäljitettävyys), eivät tee Ruuhkavahdista tuotantovalmiin sisäisen alustan — ei palvelurekisteriä, ei mTLS:ää, ei Jaeger-datan pysyvyyttä. Ks. `shared/internal_auth.py` ja `shared/tracing.py` docstringit täydestä rajoituslistasta.

## Repo-rakenne

```
ruuhkavahti/
├── docker-compose.yml          # Kafka (KRaft) + producer + guardrail-consumer + analytics-consumer + jaeger + dashboard
├── shared/                      # Core-platform-laajennuksen kanoniset lähteet (vendoroidaan build-aikana)
│   ├── internal_auth.py         # HMAC-allekirjoitettu palvelu-palvelu-auth
│   └── tracing.py                # OTel-alustus + Kafka-header-propagointi + head-based sampling
├── scripts/
│   └── measure.py               # mittausskripti (ks. pääREADME "Tulokset") — ajaa oikeita
│                                  kokeita pyörivää stackia vasten, kirjoittaa results.json:iin
├── video-exporter/               # Export Video -napin backend: Playwright + Chromium + ffmpeg,
│   └── main.py                   # nauhoittaa ?demo=true:n 1920x1080 MP4:ksi (ks. "Demo Mode")
├── producer/
│   ├── producer.py              # baseline (~200 msg/s) / spike (~8000 msg/s, ~18s), traceparent-injektio 2%:iin
│   ├── vendor/tracing.py        # vendoroitu kopio shared/tracing.py:stä
│   └── requirements.txt
├── guardrail/
│   ├── vendor/refuse_dont_guess.py  # vendoroitu muuttumattomana, ks. "Uudelleenkäyttö vs. uusi"
│   ├── vendor/tracing.py        # vendoroitu kopio shared/tracing.py:stä
│   ├── chat_rule.py             # uusi, saman muotoinen chat-moderointisääntö
│   ├── pipeline.py              # koko päätösputki (ei Kafka-riippuvuutta, testattava)
│   ├── dedup.py                 # (partition, offset)-LRU, ei Kafka-riippuvuutta
│   └── guardrail_consumer.py    # Kafka-kuluttaja, consumer group + manual commit + rebalance-callbackit + trace-jatko
├── analytics-consumer/           # Core-platform-laajennus: kolmas, itsenäinen kuluttaja
│   ├── vendor/internal_auth.py  # vendoroitu kopio, palvelinpuoli (verify)
│   ├── vendor/tracing.py        # vendoroitu kopio
│   └── analytics_consumer.py    # analytics-group, 10s-ämpärit/1h ikkuna, /health + /metrics (auth)
├── dashboard/
│   ├── backend/                     # FastAPI: WebSocket-silta Kafka-mittareista selaimeen
│   │   ├── vendor/internal_auth.py  # vendoroitu kopio, asiakaspuoli (sign)
│   │   └── vendor/tracing.py        # vendoroitu kopio
│   └── frontend/src/
│       ├── demoScript.ts             # Demo Mode -aikajana (ks. pääREADME "Demo Mode")
│       ├── useDemoMode.ts            # ?demo=true -hook: auto-piikki + tekstitykset
│       └── components/
│           ├── LagGauge.tsx              # 2D-mittari — sekä oletusnäkymän sivupaneeli ETTÄ reduced-motion-fallback
│           ├── ParticleFlow3D.tsx        # Three.js-partikkelivirta, aria-hidden (data on muualla)
│           ├── AccessibleDataTable.tsx   # piilotettu mutta painikkeella avattava <table>
│           ├── LiveAnnouncer.tsx         # aria-live="polite" -ilmoitukset
│           ├── RebalanceBanner.tsx       # rebalance-tilabanneri
│           ├── DuplicateCounter.tsx      # duplikaattilaskuri
│           ├── DemoCaption.tsx           # tekstitysoverlay Demo Modelle
│           ├── DecisionBarChart.tsx
│           └── Controls.tsx
├── tests/
│   ├── test_guardrail_logic.py  # yksikkötestit ilman Kafka-riippuvuutta
│   ├── test_dedup.py            # DedupCache-yksikkötestit, ei Kafka-riippuvuutta
│   ├── test_platform_extension.py  # internal_auth + RollingAggregate + analytics-consumer HTTP-kerros
│   ├── test_a11y.py             # axe-core dashboardille, ei myöskään Kafka-riippuvuutta
│   └── requirements.txt
├── results.json                 # raakadata pääREADME:n "Tulokset"-osioon
└── README.md
```
