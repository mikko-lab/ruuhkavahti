# Ruuhkavahti

*Kafka-pohjainen reaaliaikainen guardrail-demo*

> **TL;DR** — Simuloitu TV-lähetyksen katsojachat, joka moderoidaan reaaliajassa deterministisellä PASS/ESCALATE/BLOCK-turvakerroksella skaalautuvassa Kafka consumer groupissa. Dashboard näyttää elävänä **consumer lagin partitioittain** — ainoan luvun joka todistaa pysyykö järjestelmä piikin tahdissa ja auttaako kuluttajien lisääminen oikeasti — ja on samalla WCAG 2.1/2.2 AA -saavutettava, ei jälkikäteen liimattuna vaan yhtä todistettuna väitteenä kuin lag-metriikka itse.

**Skenaario:** Suora TV-lähetys, katsojaviestien määrä piikkaa hetkellisesti (esim. maalihetki). Jokainen viesti moderoidaan reaaliajassa ilman että ruuhka kaataa palvelun tai hukkaa viestejä.

**Periaate: "Liputa, älä piilota."** Mikään päätös ei häviä jäljettömiin — jokainen viesti päätyy jäljitettävästi yhteen kolmesta polusta: `approved`, `escalated` tai `blocked`. Sama periaate koskee mittareita: alla olevat luvut ovat oikeasta, ajettavasta stackista mitattuja, ei arvioita (ks. "Tulokset").

---

![Demo: piikki + lag-palautuminen](docs/demo.gif)

*(GIF puuttuu vielä toistaiseksi. Sisältö on nyt tuotettavissa "Export Video" -napilla (ks. "Demo Mode" alla), joka nauhoittaa `?demo=true`-käsikirjoituksen automaattisesti 1080p MP4:ksi — GIF-muunnos MP4:stä on jäljellä oleva viimeistelyaskel. Liputettu, ei piilotettu.)*

## Mitä tämä osoittaa

- **Consumer lag on ainoa mittari joka ei valehtele.** Piikin aikana se kasvaa, ja kuluttajien lisäys näkyy siinä suoraan reaaliajassa — muut mittarit (esim. kokonaisläpimeno) voivat pysyä lähes vakioina vaikka lag ei pysy (ks. "Tulokset").
- **Saavutettavuus on toinen, yhtä painava väite.** `prefers-reduced-motion` vaihtaa 3D-partikkelivirran samaan dataan ilman jatkuvaa liikettä, jokainen mittari on olemassa oikeana semanttisena HTML:nä pikselien lisäksi, ja `tests/test_a11y.py` todistaa tämän automaattisesti (axe-core, molemmat tilat).
- **Eager vs. cooperative-sticky (KIP-429) tehdään näkyväksi elävästi**, mukaan lukien rehellinen rajaus siitä milloin ero oikeasti näkyy (ks. "Tulokset" ja DEEP_DIVE.md).
- **At-least-once + idempotenssi on ratkaistu suunnittelutasolla**, ei ohitettu: (partition, offset)-pohjainen duplikaattisuodatus, mitattu oikealla fault-injection-kokeella, ei väitteellä.

## Arkkitehtuuri

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

Guardrail-logiikka pohjautuu (osittain vendoroituna, osittain uutena) repoon [`mikko-lab/refuse-dont-guess`](https://github.com/mikko-lab/refuse-dont-guess) — tarkka rajanveto: DEEP_DIVE.md.

## Ajaminen

### Ilman omaa konetta (esim. iPad / Chromebook) — GitHub Codespaces

1. Avaa repo GitHubissa: `github.com/mikko-lab/ruuhkavahti`
2. **Code** → **Codespaces**-välilehti → **Create codespace on main**
3. Aja terminaalissa alla olevat Docker-komennot normaalisti.
4. **Ports**-välilehdeltä avautuvat `5173` (dashboard) ja `8000` (backend) julkisina esikatselulinkkeinä.

### Paikallisesti

```bash
docker compose up -d --build
# odota että kafka-init on luonut topicit (docker compose logs kafka-init)

open http://localhost:5173

# yksikkö- ja saavutettavuustestit ilman Kafkaa
python3 -m unittest tests/test_guardrail_logic.py tests/test_dedup.py -v
cd dashboard/frontend && npm install && cd ../..
pip install -r tests/requirements.txt && playwright install chromium
python3 -m pytest tests/test_a11y.py -v

# skaalaa kuluttajia elävässä demossa
docker compose up -d --scale guardrail-consumer=4
```

Dashboardin "Laukaise piikki" -nappi kutsuu producerin `/trigger-spike`-päätepistettä suoraan (aito live-kontrolli). Kuluttajamäärä- ja strategiavalinnat näyttävät kopioitavan komennon sen sijaan että ohjaisivat Dockeria kontin sisältä — tietoinen turvallisuusvalinta, ei `docker.sock`-mounttia taustapalveluun.

### Demo Mode (yhden oton nauhoitusta varten)

`http://localhost:5173/?demo=true` käynnistää kiinteän ~46 sekunnin käsikirjoituksen (`dashboard/frontend/src/demoScript.ts`), jotta OBS-nauhoitus toistuu identtisenä joka kerta: avaustekstitys ("Simulating a live TV broadcast traffic spike") antaa katsojalle kontekstin heti, piikki laukeaa automaattisesti t=9s, fade-tekstitykset seuraavat skriptiä (`aria-hidden`, eivät toistu ruudunlukijalle), manuaaliset kontrollit piiloutuvat, 3D-kameran kiertoliike jäädytetään — liike syntyy vain datasta — ja lopputekstitys ("Deterministic guardrails stayed online during the spike") kiteyttää pointin. Kunnioittaa `prefers-reduced-motion`-asetusta normaalisti. Kuluttajaskaalaus (`docker compose up -d --scale guardrail-consumer=4`) on yhä presenterin oma manuaalinen askel toisessa terminaalissa — tekstitys "Scaling consumer group…" on ajoitusvihje, ei automaatio (sama `docker.sock`-rajaus kuin yllä). Harjoittele ajoitus kerran ennen varsinaista ottoa.

**Export Video -nappi** (sidebarin alaosassa, ei näy demo-tilassa itsessään) tekee OBS:n tarpeettomaksi: se kutsuu erillistä `video-exporter`-palvelua (oma kontti, Playwright + Chromium + ffmpeg), joka ajaa `?demo=true`-käsikirjoituksen oikealla selaimella 1920×1080-resoluutiolla, nauhoittaa sen ja muuntaa H.264-MP4:ksi (~50 s kokonaiskesto, valmis tiedosto latautuu automaattisesti nappiin ilmestyvästä linkistä). Sama komento tuottaa identtisen videon joka kerta — ei manuaalista nauhoitusta, ei kameran/mikin asetteluja. `dashboard-backend` toimii ohuena proxynä (`/api/export-video`), sama periaate kuin muillekin kontrolleille.

## Tulokset

Mitattu oikeaa pyörivää stackia vasten `scripts/measure.py`:llä (ei simulaatiota) paikallisella koneella (Docker 29.6.1, KRaft-Kafka, 4 partitiota), piikki 8000 msg/s / ~18 s, baseline 200 msg/s. **Yhden ajon tuloksia** (n=1 per skenaario), ei toistettuja mittauksia keskihajontoineen — raakadata ja menetelmä: `results.json`.

**Läpimeno ja latenssi piikin aikana, kuluttajamäärän funktiona:**

| Kuluttajia | Läpimeno (msg/s) | p50 (ms) | p95 (ms) | Piikin huippulag | Palautuminen piikin jälkeen |
|---|---|---|---|---|---|
| 1 | 7719 | 8.9 | 13.3 | 1489 | 3.35 s |
| 2 | 7830 | 7.8 | 12.2 | 659 | 3.88 s |
| 4 | 7704 | 7.0 | 9.2 | 400 | 4.43 s |

**Huomio — liputettu, ei piilotettu:** kokonaisläpimeno ja palautumisaika pysyvät lähes vakioina kuluttajamäärästä riippumatta: 8000 msg/s piikki ja kevyt avainsanaskannaus eivät riitä tekemään yhdestä kuluttajasta pullonkaulaa tässä ympäristössä. Todellinen, mitattava hyöty näkyy **piikin aikaisessa huippulagissa**, joka laskee lähes lineaarisesti kuluttajamäärän kasvaessa (1489 → 659 → 400) — useampi kuluttaja pitää jonon lyhyempänä koko piikin ajan, vaikka lopputulos piikin jälkeen on sama.

**Rebalance-pausi skaalattaessa 1 → 4 kuluttajaa:**

| Strategia | Ryhmän koordinaattoripausi | Partitioita pysähtyi |
|---|---|---|
| cooperative-sticky | 2.79 s | 4 / 4 |
| eager (range) | 2.65 s | 4 / 4 |

**Huomio:** 1→4-skaalauksessa kaikki 4 partitiota vaihtavat väistämättä omistajaa riippumatta strategiasta (yhdellä alkuperäisellä kuluttajalla oli kaikki neljä) — cooperative-stickyn "vain siirtyvät partitiot pysähtyvät" -etu ei siis pääse tässä konkreettisesti näkyviin. Alustavissa instrumentoimattomissa ajoissa koordinaattoripausin hajonta oli suurta (0.76 s – 6.57 s); n=1 per strategia, ei tarkka benchmark. Per-partitio-data ja selitys: DEEP_DIVE.md.

**Duplikaattisuodatus** (`docker pause` 50 s yksittäiselle kuluttajalle, ei restart — DedupCache säilyy muistissa):

| Viestejä käsitelty testin aikana | Duplikaatteja suodatettu |
|---|---|
| 12 251 | 0 |

**Huomio:** nolla ei ole mittausvirhe — per-viesti-synkroninen commit (ks. DEEP_DIVE.md) tekee ei-committoitujen viestien ikkunasta niin kapean, ettei tämä koe tuottanut yhtään duplikaattia. Mekanismi on todistettu yksikkötasolla (`tests/test_dedup.py`); tämä koe todistaa sen sijaan miten harvoin at-least-once-uudelleentoimitus oikeasti laukeaa.

## Accessibility

Sama data kolmena rinnakkaisena esitysmuotona, ei "pääversiona" + kevennettynä varana:

| Esitysmuoto | Milloin näkyy | Komponentti |
|---|---|---|
| 3D-partikkelivirta | oletus, `prefers-reduced-motion: no-preference` | `ParticleFlow3D.tsx` (`aria-hidden="true"`) |
| 2D-mittarinäkymä | `prefers-reduced-motion: reduce` | `LagGauge.tsx` (sama komponentti kummassakin tilassa) |
| Semanttinen `<table>` | aina saatavilla, painikkeen takana | `AccessibleDataTable.tsx` |

Lisäksi: `LiveAnnouncer.tsx` ilmoittaa lag-tason muutokset ja piikin alun/lopun tekstinä (`aria-live="polite"`, ei jokaista päivitystä); väri ei ole koskaan ainoa signaali (aina numero + `aria-label`); kaikki kontrollit näppäimistökäytettäviä, näkyvä `:focus-visible`. **Todiste, ei väite:** `tests/test_a11y.py` ajaa axe-coren oletus- ja `reduced-motion`-tilassa — **0 löydöstä, 36 läpäisyä**, molemmissa tiloissa.

## Limitations (liputa, älä piilota)

- **Todellinen toksisuusluokitin** → korvattu avainsanaskannauksella (`chat_rule.py`); ydin on turvakerroksen rakenne, ei sisällönluokittelun tarkkuus.
- **Kafka-transaktiot / exactly-once** → tietoinen valinta at-least-once-semantiikan puolesta. Kaksoiskäsittely hyväksytty riski.
- **Duplikaattisuodatus ei selviä consumerin uudelleenkäynnistyksestä** → `DedupCache` on prosessin muistissa, rajattu 500 viestiin. Mitattu duplikaattitiheys (0/12 251) johtuu osin juuri per-viesti-committing-mallista — ei tarkoita että mekanismia ei tarvittaisi, vain että sen luonnollinen laukaisutaajuus on matala tässä arkkitehtuurissa. Tuotantotason vaihtoehto: pysyvä dedup-tallennus (Redis/tietokanta) tai Kafkan transaktionaalinen tuottaja. Ks. DEEP_DIVE.md.
- **Läpimeno/palautumisaika eivät erottele kuluttajamäärää tässä ympäristössä** → ks. "Tulokset": 8000 msg/s piikki + kevyt moderointilogiikka eivät riitä pullonkauloittamaan yhtä kuluttajaa. Piikin huippulag sen sijaan erottelee selvästi.
- **Rebalance-pausimittaus on n=1 per strategia, ei toistettu** → havaittu run-to-run-hajonta oli merkittävää alustavissa ajoissa. Aja `scripts/measure.py rebalance` uudelleen useampaan kertaan ennen kuin lukuja käyttää tarkkana benchmarkina.
- **Autentikointi, TLS, tuotantotason monitorointi** → ei mukana, demo keskittyy yhteen tarinaan (lag + rebalance).
- **axe-core kattaa automatisoidusti havaittavan** → n. 30-50 % WCAG-ongelmista tyypillisesti; manuaalinen ruudunlukijatestaus (VoiceOver/NVDA) puuttuu, liputettu tässä.

---

*Katso myös: [mikko-lab/refuse-dont-guess](https://github.com/mikko-lab/refuse-dont-guess) — deterministinen turvakerros, josta tämän demon guardrail-logiikka on peräisin.*

**Full technical deep dive:** [DEEP_DIVE.md](DEEP_DIVE.md)
