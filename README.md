# Ruuhkavahti

### Kafka-pohjainen reaaliaikainen guardrail-demo

> **TL;DR** — Simulated TV-broadcast viewer chat at 200 → 5 000-10 000 msg/s, moderated in real time by a deterministic PASS/ESCALATE/BLOCK safety layer running as a scalable Kafka consumer group. Live dashboard (React + Three.js) visualises the message flow as a 3D particle stream and, more importantly, **consumer lag per partition** — the one number that proves whether the system is keeping up with the spike or falling behind, and whether adding consumers actually fixes it. Second, equally-load-bearing claim: the same dashboard is WCAG 2.1/2.2 AA accessible — `prefers-reduced-motion` swaps the 3D scene for the same live data with no motion, every metric exists as real semantic HTML, not just pixels, and an automated axe-core scan (`tests/test_a11y.py`) reports zero serious/critical violations in both modes. Third: the demo visualises the difference between eager and cooperative-sticky rebalancing (KIP-429) live, and filters at-least-once duplicates with a (partition, offset)-keyed cache — both honestly scoped as demo-level, not production-complete.

**Skenaario:** Suora TV-lähetys, katsojaviestien määrä piikkaa hetkellisesti (esim. maalihetki). Jokainen viesti moderoidaan reaaliajassa ilman että ruuhka kaataa palvelun tai hukkaa viestejä.

**Periaate: "Liputa, älä piilota."** Mikään päätös ei häviä jäljettömiin — jokainen viesti päätyy jäljitettävästi yhteen kolmesta polusta: `approved`, `escalated` tai `blocked`. Sama periaate koskee käyttäjiä: mikään data ei ole olemassa vain visuaalisen tulkinnan (3D-kohtaus, väri) takana — ks. osa 4.

---

## 1. Arkkitehtuuri

```
[Viewer Simulator]  →  Kafka topic: viewer-messages  →  [Guardrail Consumer Group]
   (producer,             (4 partitiota,                  (1-4 rinnakkaista workeria,
    säädettävä              key = viewer_id                 deterministinen
    lähetysnopeus,           järjestyksen                    PASS/ESCALATE/BLOCK)
    "piikki"-tila)           säilyttämiseksi)
                                                                      │
                              ┌───────────────────────────────────────┼───────────────────────┐
                              ▼                                       ▼                       ▼
                    Kafka topic:                          Kafka topic:              Kafka topic:
                    approved-messages                      escalated-messages         blocked-messages
                    (näytölle)                              (ihmismoderaattorille)     (auditloki)
                              │
                              ▼
                    [Dashboard / Visualisointi]
                    - live consumer lag per partitio
                    - päätösjakauma (pass/escalate/block) reaaliajassa
                    - läpimenoaika (p50/p95)
```

## 2. Uudelleenkäyttö vs. uusi — rehellinen rajanveto

Guardrail-logiikka pohjautuu repoon [`mikko-lab/refuse-dont-guess`](https://github.com/mikko-lab/refuse-dont-guess) (vendoroitu muuttumattomana `guardrail/vendor/refuse_dont_guess.py`:hen, commit `1f7e75b8`). Sieltä on **aidosti uudelleenkäytetty sellaisenaan**, koska se ei ole toimialasidonnaista:

- `scan_for_injection()` + `INJECTION_PATTERNS` — tunnistaa tekstiin upotetut ohjausyritykset ("ignore previous instructions", "älä eskaloi" jne.). Tämä pätee identtisesti TV-chatissa: katsoja voi yrittää huijata automoderaattorin päästämään viestinsä läpi samalla tempulla kuin alkuperäisen repon laskudokumentissa.
- `Decision` / `Result` -tyypit ja koko putken muoto: `input-guardrail (BLOCK) → poiminta → luottamuskynnys + sääntö (PASS/ESCALATE)`.

Mikä **ei** siirry: alkuperäinen `apply_rule` päättää rakennusalan käännetyn ALV:n soveltumisesta — se ei tarkoita mitään chat-viestin kohdalla. Tilalle on kirjoitettu `guardrail/chat_rule.py`, joka noudattaa **täsmälleen saman muotoisen** säännön (avainsanapohjainen lippulistaus + luottamuskynnys, palauttaa vain PASS/ESCALATE — ei koskaan BLOCK, sama kuin alkuperäisessä `apply_rule`:ssa). `guardrail/pipeline.py` yhdistää nämä kaksi täsmälleen alkuperäisen `process()`:n muotoiseksi putkeksi.

## 3. Oppimispolku (konseptit koodin takana)

1. **Partitio ja partition key** — `viewer-messages` jaetaan 4 partitioon. `key = viewer_id` takaa, että saman katsojan viestit käsitellään aina järjestyksessä (osuvat samaan partitioon), mutta eri katsojat jakautuvat tasan 4 partitioon — järjestys JA rinnakkaisuus samaan aikaan.
2. **Consumer group ja rebalance** — kaikki `guardrail-consumer`-instanssit kuuluvat groupiin `guardrail-group`. Kafka takaa, ettei kaksi workeria koskaan lue samaa partitiota yhtä aikaa. Kun kuluttajien määrä muuttuu, Kafka **rebalancoi**: partitiot jaetaan uudelleen elossa olevien kuluttajien kesken — tämä on se hetki, kun lisätty kuluttaja alkaa oikeasti purkaa jonoa. Kaksi eri tapaa tehdä tämä — ks. osa 5.
3. **Offset ja at-least-once-semantiikka** — `enable.auto.commit: False`, commit vasta sen jälkeen kun päätös on kirjoitettu output-topiciin. Jos worker kaatuu ennen committia, viesti luetaan uudelleen — se ei koskaan katoa jäljettömiin (mahdollinen kaksoiskäsittely on hyväksytty kompromissi; kadonnut BLOCK-päätös ei olisi).
4. **Consumer lag mittarina** — `lag = partition_high_watermark - group_committed_offset`. Tämä on ainoa mittari, joka reagoi suoraan sekä tuotantonopeuteen että kuluttajien määrään: piikin aikana lag kasvaa, kuluttajien lisäys (rebalance) saa sen laskemaan reaaliajassa.
5. **Miksi WebGL vaatii aina rinnakkaisen semanttisen esityksen** — `<canvas>` on ruudunlukijalle läpinäkymätön pikselikartta. `prefers-reduced-motion` ei ole "kevyempi versio" vaan sama data ilman jatkuvaa animaatiota (liike voi aiheuttaa esim. vestibulaarioireita); ruudunlukijatuki taas vaatii saman datan olemassaolon oikeana HTML:nä riippumatta liikeasetuksesta. Kaksi eri ongelmaa, osittain päällekkäiset mutta erilliset ratkaisut — ks. osa 4.
6. **Eager vs. cooperative-sticky rebalance (KIP-429)** — pysähtyykö rebalancen ajaksi koko kulutus vai vain ne partitiot jotka oikeasti vaihtavat omistajaa? Ks. osa 5.
7. **Idempotenssi (partition, offset)-avaimella** — miksi Kafkan oma koordinaatti riittää dedup-avaimeksi eikä erillistä viesti-ID:tä tarvitse keksiä, ja miksi duplikaatti ESCALATE/BLOCK olisi oikea guardrail-ongelma. Ks. osa 6.

## 4. Saavutettavuus — toinen todistettava väite, ei jälkikäteen liimattu kerros

Sama data on olemassa kolmena rinnakkaisena esitysmuotona, ei yhtenä "pääversiona" + kevennettynä varana:

| Esitysmuoto | Milloin näkyy | Komponentti |
|---|---|---|
| 3D-partikkelivirta | oletus, `prefers-reduced-motion: no-preference` | `ParticleFlow3D.tsx` (`aria-hidden="true"` — data on muualla) |
| 2D-mittarinäkymä (staattiset päivitykset) | `prefers-reduced-motion: reduce` | `LagGauge.tsx` (sama komponentti kummassakin tilassa) |
| Semanttinen `<table>` | aina saatavilla, painikkeen takana | `AccessibleDataTable.tsx` |

Lisäksi:
- **`LiveAnnouncer.tsx`** — `aria-live="polite"`-alue ilmoittaa tekstinä lag-tason muutokset (matala/kohtalainen/korkea) ja piikin alkamisen/päättymisen, ei jokaista päivitystä (ei hukuttaisi ruudunlukijaa).
- **Väri ei ole ainoa signaali** — jokaisessa mittarissa on aina myös numero ja `role="img"` + `aria-label` kuvaamassa arvon ja tason tekstinä.
- **Näppäimistö** — kaikki kontrollit ovat natiiveja `<button>`/`<input type="range">`-elementtejä, näkyvä `:focus-visible`-tila (`outline: 3px solid #60a5fa`), ei mitään `outline: none`.
- **Automatisoitu todiste, ei väite** — `tests/test_a11y.py` käynnistää dashboardin (ilman Kafkaa — alkutila riittää), ajaa axe-coren sekä oletus- että `reduced-motion`-tilassa, ja varmistaa erikseen että `.particle-stream` on `aria-hidden` ja datataulukko on näppäimistöllä avattavissa. Tulos tätä kirjoitettaessa: **0 löydöstä (mukaan lukien kaikki vakavuustasot), 36 läpäisyä**, molemmissa tiloissa myös datataulukko avattuna.

## 5. Rebalance-strategiat: Eager vs Cooperative-sticky (KIP-429)

Kun consumer groupin jäsenmäärä muuttuu, kaikkien partitioiden pitää joskus vaihtaa omistajaa. Kysymys on: **pysähtyykö koko ryhmä joka kerta, vai vain se osa joka oikeasti muuttuu?**

- **Eager / klassinen** (`range`, `roundrobin`) — coordinator käskee *kaikkia* kuluttajia luopumaan *kaikista* partitioistaan ensin, jakaa uudelleen vasta sitten. Välissä yksikään partitio ei ole kenenkään luettavana — koko kulutus pysähtyy, vaikka muutos olisi pieni (esim. yksi worker neljästä lisätty).
- **Cooperative-sticky (KIP-429)** — vain partitiot, jotka oikeasti vaihtavat omistajaa, revokoidaan. Loput jatkavat keskeytyksettä. Pienempi, kohdistetumpi katko.

Dashboard tekee tämän näkyväksi kahdella tavalla:
- **Tilabanneri** ("Rebalancing partition assignments…") ilmestyy aina rebalancen ajaksi, `aria-live="polite"`-yhteensopivana (sama kaava kuin muut tilailmoitukset, ks. osa 4).
- **Partikkelivirran pysähdys** kohdistuu eager-tilassa kaikkiin neljään putkeen; cooperative-sticky-tilassa vain niihin partitioihin, jotka `guardrail_consumer.py`:n `on_assign`/`on_revoke`-callbackit raportoivat siirtyviksi (`rebalance-events`-topic).

Strategian vaihto (dashboardin "Rebalance-strategia"-valitsin) näyttää kopioitavan komennon samalla periaatteella kuin kuluttajaskaalaus — vaihto vaatii kontin uudelleenkäynnistyksen (`partition.assignment.strategy` on Consumer-konfiguraatio, ei ajonaikana muutettavissa):

```bash
ASSIGNMENT_STRATEGY=range docker compose up -d --build guardrail-consumer
```

## 6. Idempotenssi ja duplikaattien suodatus

At-least-once (osa 3) tarkoittaa juuri sitä: viesti käsitellään *vähintään* kerran, joskus useammin. Tämä ei ole korjattava bugi — se on tietoinen valinta (mieluummin kaksoiskäsittely kuin kadonnut BLOCK-päätös). Mutta duplikaatin pitää olla harmiton, ei vain "toivottavasti ei tapahdu": duplikaatti `ESCALATE`/`BLOCK` menisi ihmismoderaattorille kahdesti — sama viesti kahdesti hänen jonossaan. Dedup kytkeytyy siis suoraan guardrail-logiikkaan, ei ole irrallinen Kafka-oppitunti.

`guardrail/dedup.py`:n `DedupCache` käyttää dedup-avaimena **(partition, offset)**-paria — ei erikseen keksittyä `message_id`-kenttää. Tämä on Kafkan oma, aidosti uniikki koordinaatti jokaiselle fyysiselle viestille, ja täsmälleen se koordinaatti jonka Kafka itse käyttää seuraamaan missä kohtaa lokia kuluttaja on. Kun `guardrail_consumer.py` havaitsee saman `(partition, offset)`-parin uudelleen, se ei julkaise päätöstä uudelleen output-topiciin — ainoastaan laskee sen `duplicate-events`-topiciin, jota dashboard näyttää laskurina ("Duplikaatteja suodatettu").

**Rehellinen rajaus (liputa, älä piilota):** `DedupCache` on rajattu (500 viestiä) LRU-joukko *prosessin muistissa*. Se **ei selviä** `guardrail-consumer`-kontin omasta uudelleenkäynnistyksestä — silloin cache tyhjenee. Tuotannossa tarvitsisi joko pysyvän dedup-tallennuksen (esim. Redis/tietokanta avaimella `partition:offset` + TTL) tai Kafkan transaktionaalisen/exactly-once-tuottajan. Tämä demo näyttää periaatteen, ei täydellistä ratkaisua.

## 7. Repo-rakenne

```
ruuhkavahti/
├── docker-compose.yml          # Kafka (KRaft) + producer + guardrail-consumer + dashboard
├── producer/
│   ├── producer.py              # baseline (~200 msg/s) / spike (~5-10k msg/s, 15-20s)
│   └── requirements.txt
├── guardrail/
│   ├── vendor/refuse_dont_guess.py  # vendoroitu muuttumattomana, ks. osa 2
│   ├── chat_rule.py             # uusi, saman muotoinen chat-moderointisääntö
│   ├── pipeline.py              # koko päätösputki (ei Kafka-riippuvuutta, testattava)
│   ├── dedup.py                 # (partition, offset)-LRU, ei Kafka-riippuvuutta (ks. osa 6)
│   └── guardrail_consumer.py    # Kafka-kuluttaja, consumer group + manual commit + rebalance-callbackit
├── dashboard/
│   ├── backend/                     # FastAPI: WebSocket-silta Kafka-mittareista selaimeen
│   └── frontend/src/
│       ├── demoScript.ts             # Demo Mode -aikajana (ks. osa 9)
│       ├── useDemoMode.ts            # ?demo=true -hook: auto-piikki + tekstitykset
│       └── components/
│           ├── LagGauge.tsx              # 2D-mittari — sekä oletusnäkymän sivupaneeli ETTÄ reduced-motion-fallback
│           ├── ParticleFlow3D.tsx        # Three.js-partikkelivirta, aria-hidden (data on muualla)
│           ├── AccessibleDataTable.tsx   # piilotettu mutta painikkeella avattava <table>
│           ├── LiveAnnouncer.tsx         # aria-live="polite" -ilmoitukset
│           ├── RebalanceBanner.tsx       # rebalance-tilabanneri (ks. osa 5)
│           ├── DuplicateCounter.tsx      # duplikaattilaskuri (ks. osa 6)
│           ├── DemoCaption.tsx           # tekstitysoverlay (ks. osa 9)
│           ├── DecisionBarChart.tsx
│           └── Controls.tsx
├── tests/
│   ├── test_guardrail_logic.py  # yksikkötestit ilman Kafka-riippuvuutta
│   ├── test_dedup.py            # DedupCache-yksikkötestit, ei Kafka-riippuvuutta
│   ├── test_a11y.py             # axe-core dashboardille, ei myöskään Kafka-riippuvuutta
│   └── requirements.txt
└── README.md
```

## 8. Ajaminen

### Ilman omaa konetta (esim. iPad / Chromebook) — GitHub Codespaces

Docker ei toimi tableteilla tai muilla laitteilla joilla ei ole omaa Linux-ympäristöä. Codespaces antaa selainpohjaisen VS Coden + terminaalin, jossa Docker toimii normaalisti — riittää pelkkä selain, ei asennuksia:

1. Avaa repo GitHubissa: `github.com/mikko-lab/ruuhkavahti`
2. **Code** → **Codespaces**-välilehti → **Create codespace on main**
3. Kun selainpohjainen editori + terminaali avautuu, aja terminaalissa tämän osion Docker-komennot normaalisti (`docker compose up -d --build` jne.)
4. Kun palvelut ovat käynnissä, avaa alalaidan **Ports**-välilehti — portit `5173` (dashboard) ja `8000` (backend) ilmestyvät sinne automaattisesti. Paina niiden kohdalla maapallo/selain-ikonia avataksesi toimivan julkisen esikatselulinkin selaimessa.
5. Kuluttajien skaalaus (`docker compose up -d --scale guardrail-consumer=4`) ja piikin laukaisu toimivat samasta terminaalista/dashboardista täysin normaalisti — Codespace on vain tavallinen Linux-kontti, ei rajoitettu ympäristö.

### Paikallisesti

```bash
docker compose up -d --build
# odota että kafka-init on luonut topicit (docker compose logs kafka-init)

# avaa dashboard
open http://localhost:5173

# aja pelkät guardrail-testit ilman Kafkaa
python3 -m unittest tests/test_guardrail_logic.py -v
python3 -m unittest tests/test_dedup.py -v

# aja saavutettavuustesti (ei myöskään vaadi Kafkaa/backendia)
cd dashboard/frontend && npm install && cd ../..
pip install -r tests/requirements.txt && playwright install chromium
python3 -m pytest tests/test_a11y.py -v

# skaalaa kuluttajia elävässä demossa (dashboardin liukusäädin generoi tämän komennon)
docker compose up -d --scale guardrail-consumer=4
```

Dashboardin "Laukaise piikki" -nappi kutsuu producerin `/trigger-spike`-päätepistettä suoraan (aito live-kontrolli). Kuluttajamäärä-liukusäädin sen sijaan näyttää kopioitavan `docker compose --scale`-komennon eikä ohjaa Dockeria konttien sisältä — tietoinen valinta: `docker.sock`-mountti taustapalveluun olisi turvallisuusmielessä huono ratkaisu portfoliodemolle eikä olisi yhtä luotettava livetilanteessa. Dashboard näyttää silti aktiivisten kuluttajien todellisen, Kafkan ryhmämetadatasta luetun määrän.

## 9. Demo Mode (yhden oton nauhoitusta varten)

`http://localhost:5173/?demo=true` käynnistää kiinteän ~38 sekunnin käsikirjoituksen (`dashboard/frontend/src/demoScript.ts`), jotta OBS-nauhoitus toistuu identtisenä joka kerta:

| Aika | Tapahtuu |
|---|---|
| 0–5 s | Normaalitila, ei toimenpiteitä |
| 5 s | Piikki laukeaa **automaattisesti** (sama kutsu kuin "Laukaise piikki") |
| 5–12 s | Lag kasvaa, partikkelivirta tihenee |
| 12 s | Tekstitys "Scaling consumer group…" — presenterin oma vihje ajaa **ennalta valmisteltu** `docker compose up -d --scale guardrail-consumer=4` toisessa terminaalissa |
| 13–16 s | Kafka rebalancoi — RebalanceBanner ja partikkelivirran pysähdys näkyvät oikeasti, eivät lavastettuna |
| 16–31 s | Lag purkautuu, palautuminen |
| 31–38 s | "System stable" |

Demo Mode korostaa lag-mittaria ja rebalance-banneria (sininen/keltainen hehku), piilottaa manuaaliset kontrollit (napit, sliderit, kopioitavat komennot, datataulukon avauspainike) siistiä nauhoitusta varten, ja jäädyttää 3D-kameran hitaan kiertoliikkeen — liike syntyy vain datasta, ei kamerasta. Tekstitykset ovat `aria-hidden` (puhtaasti visuaalinen nauhoituslisä; dashboard on jo saavutettava ilman niitä) ja kunnioittavat `prefers-reduced-motion`-asetusta samoin kuin muu UI.

**Huom:** skaalauskomento on yhä presenterin oma manuaalinen askel (ks. osa 8 — ei `docker.sock`-automaatiota), joten harjoittele ajoitus kerran ennen varsinaista ottoa.

## 10. Tietoisesti rajattu ulos (liputa, älä piilota)

- **Todellinen toksisuusluokitin** → korvattu yksinkertaisella avainsanaskannauksella (`chat_rule.py`). Sama rajaus kuin alkuperäisessä repossa: ydin on turvakerros ja sen rakenne, ei sisällönluokittelun tarkkuus.
- **Kafka-transaktiot / exactly-once** → tietoinen valinta at-least-once-semantiikan puolesta (ks. osa 3). Kaksoiskäsittely on hyväksyttävä riski tässä skenaariossa.
- **Duplikaattisuodatus ei selviä consumerin uudelleenkäynnistyksestä** → `DedupCache` on prosessin muistissa, rajattu 500 viestiin. Tuotantotason vaihtoehto: pysyvä dedup-tallennus (Redis/tietokanta) tai Kafkan transaktionaalinen/exactly-once-tuottaja. Ks. osa 6.
- **Autentikointi, TLS, tuotantotason monitorointi (Prometheus/Grafana)** → ei mukana, demo keskittyy yhteen tarinaan (lag + rebalance), ei yleiskäyttöiseen Kafka-hallintapaneeliin.
- **axe-core kattaa automatisoidusti havaittavan** → automaattitestit löytävät n. 30-50 % WCAG-ongelmista tyypillisesti; manuaalinen ruudunlukijatestaus (VoiceOver/NVDA) puuttuu tästä repositoriosta, liputettu tässä eikä piiloteltu.

---

*Katso myös: [mikko-lab/refuse-dont-guess](https://github.com/mikko-lab/refuse-dont-guess) — deterministinen turvakerros, josta tämän demon guardrail-logiikka on peräisin.*
