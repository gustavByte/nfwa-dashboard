# Norsk friidrettsstatistikk (NFWA Dashboard)

[![MIT-lisens](https://img.shields.io/badge/lisens-MIT-blue.svg)](LICENSE)

Statistikkpipeline for norsk friidrett. Henter sesongresultater fra flere
kilder, beregner World Athletics (WA)-poeng, lagrer i SQLite, og publiserer
et statisk dashboard via GitHub Pages.

**[Se live dashboard](https://gustavByte.github.io/nfwa-dashboard/)**

---

## Innhold

- [Arkitektur](#arkitektur)
- [Kom i gang](#kom-i-gang)
- [Kommandoer](#kommandoer)
- [Gateløp (Kondis)](#gateløp-kondis)
- [Web-dashboard](#web-dashboard)
- [Publisering (GitHub Pages)](#publisering-github-pages)
- [Legge til legacy-år](#legge-til-legacy-år)
- [Datamodell](#datamodell)
- [CI/CD](#cicd)
- [Bidra](#bidra)
- [Lisens](#lisens)

---

## Arkitektur

Prosjektet består av to deler:

1. **`nfwa/`** — Hovedpakke. Scraping, innlasting, spørringer, web-dashboard
   og statisk site-eksport.
   Inngang: `python -m nfwa` → `nfwa/__main__.py` → `nfwa/cli.py`.

2. **[`WA Poeng/`](https://github.com/gustavByte/world-athletics-points-calculator)** —
   Selvstendig WA-poengkalkulator. Parser WA Scoring Tables-PDF til
   `wa_scoring.db` og tilbyr `ScoreCalculator` for poengoppslag.
   Importeres av `nfwa` via `sys.path` (se `nfwa/wa.py`).

### Dataflyt

```
Kilder (HTML)
  ├── minfriidrett.py     — minfriidrettsstatistikk.info (2011+)
  ├── friidrett_legacy.py — friidrett.no (2000–2010)
  └── kondis.py           — kondis.no (gateløp)
        │
        ▼
    ingest.py  — sync_landsoversikt() / sync_kondis()
        │  bruker: event_mapping.py (norsk øvelsesnavn → WA-kode)
        │  bruker: util.py (rensing/normalisering av prestasjoner)
        │  bruker: wa.py (WA ScoreCalculator-bro)
        ▼
    db.py  — SQLite-skjema + upserts → data/nfwa_results.sqlite3
        │
        ▼
    queries.py — event_summary, event_trend, event_results, athlete_results
        │
        ├── webapp.py      — lokal utviklerserver (JSON API)
        ├── export_site.py — pre-rendrer JSON API til docs/api/*.json
        └── site_build.py  — orkestrerer sync + eksport (brukes av CI)
```

### Frontend

`nfwa/web_static/` inneholder `index.html`, `app.js`, `styles.css`. Ren
JavaScript uten byggesteg. Både den lokale utviklerserveren og den statiske
eksporten bruker disse filene.

---

## Kom i gang

### Forutsetninger

- Python 3.10+ (anbefalt 3.12)
- Git

### 1. Klon repoet

```powershell
git clone https://github.com/gustavByte/nfwa-dashboard.git
cd nfwa-dashboard
```

### 2. Klon WA Poeng-repoet

```powershell
git clone https://github.com/gustavByte/world-athletics-points-calculator.git "WA Poeng"
```

### 3. Installer avhengigheter

```powershell
python -m pip install -r requirements.txt
```

### 4. Verifiser oppsettet

```powershell
python -m nfwa sync --years 2025
python -m nfwa web
```

Åpne `http://127.0.0.1:8000/` i nettleseren — du skal se dashboardet.

---

## Kommandoer

### Synkroniser friidrett (bane og felt)

```powershell
python -m nfwa sync --years 2023 2024 2025
```

For å hente flere år (t.o.m. 2000), kan du i PowerShell bruke en range:

```powershell
python -m nfwa sync --years (2000..2025)
```

Merk: For sesongene 2000–2010 brukes legacy-sidene på friidrett.no (annen
HTML-struktur). Utøver-IDer genereres lokalt (negative heltalls-IDer).

### Synkroniser gateløp (Kondis)

```powershell
python -m nfwa sync-kondis --years 2023 2024 2025
```

Henter topp-lister for gateløp fra kondis.no (5 km, 10 km, halvmaraton,
maraton). Utøver-IDer genereres lokalt basert på kjønn + navn + fødselsår.

### CSV-eksport (top-N per øvelse)

```powershell
python -m nfwa event-summary --season 2025
```

Skriver f.eks. `data/event_summary_2025_both.csv` med snitt av WA-poeng og
prestasjoner for ulike top-N-verdier.

### Slå opp utøver

```powershell
python -m nfwa athlete --athlete-id 29273 --since 2024
```

`athlete-id` er `showathl=...`-tallet i lenkene på minfriidrettsstatistikk.

### Full pipeline (sync + eksport)

```powershell
python -m nfwa build-site --min-year 1997 --out docs
```

Alle kommandoer aksepterer `--db`, `--wa-db`, `--wa-root`, `--cache-dir` og
`--refresh`.

---

## Gateløp (Kondis)

```powershell
python -m nfwa sync-kondis --years (2011..2025)
```

Data lagres i samme database (`data/nfwa_results.sqlite3`) og vises i
dashboardet sammen med bane- og feltresultater.

Kondis-listene har ikke `athlete-id` slik minfriidrettsstatistikk har.
Det genereres derfor en stabil, lokal utøver-id (negativ heltalls-id)
basert på kjønn + navn + fødselsår.

---

## Web-dashboard

```powershell
python -m nfwa web
```

Starter en lokal nettside (default `http://127.0.0.1:8000/`) med:

- Trend per øvelse mellom år (snitt WA-poeng og snitt resultat)
- Sesongoversikt (sorter på poeng/resultat)
- Utøveroppslag (athlete-id)

---

## Publisering (GitHub Pages)

Den statiske siden lages lokalt i `docs/` med:

```powershell
python -m nfwa export-site --out docs
```

### Oppsett

1. Lag en **public** repo på GitHub (eller bruk eksisterende).
2. Push koden til repoen.
3. I GitHub: **Settings → Pages → Source: GitHub Actions**.

Da blir siden tilgjengelig på `https://<brukernavn>.github.io/<repo>/`.

---

## Legge til legacy-år

### friidrett.no (2000–2010)

1. Legg inn årets URLer i `FRIIDRETT_PAGES_<YEAR>` i `nfwa/friidrett_legacy.py`.
2. Koble året inn i `FRIIDRETT_PAGES` i samme fil.
3. Kjør sync med refresh:

```powershell
python -m nfwa sync --years 2000 --refresh
```

4. Kjør rask kvalitetskontroll (rader, duplikater, WA-feil):

```powershell
@'
import sqlite3
year = 2000
con = sqlite3.connect("data/nfwa_results.sqlite3")
cur = con.cursor()
cur.execute("select count(*) from results where season=?", (year,))
print("rows", cur.fetchone()[0])
cur.execute("""
select count(*) from (
  select gender, event_id, athlete_id, count(*) c
  from results
  where season=?
  group by gender, event_id, athlete_id
  having c > 1
) t
""", (year,))
print("dup_event_person", cur.fetchone()[0])
cur.execute("select count(*) from results where season=? and wa_error is not null", (year,))
print("wa_errors", cur.fetchone()[0])
con.close()
'@ | python -
```

5. Rebygg og publiser:

```powershell
python -m nfwa export-site --out docs
git add nfwa/friidrett_legacy.py docs/api README.md
git commit -m "Add 2000 legacy data"
git push origin main
```

### Kondis (gateløp)

1. Legg inn `sesong → url`-par i riktig dict i `nfwa/kondis.py`
   (f.eks. `_FIVE_KM_WOMEN_LEGACY_URLS`).
2. Kjør sync:

```powershell
python -m nfwa sync-kondis --years (1997..2010) --gender Women --refresh
```

For maraton menn (1997-2003) med manuell korreksjonsfil:

```powershell
python scripts/generate_kondis_maraton_menn_csv.py `
  --xlsx "data/manual_sources/kondis/2003 -1997 Norgesstatistikk maraton menn.xlsx"
python -m nfwa sync-kondis --years (1997..2003) --gender Men --refresh
```

Scriptet eksporterer kun sesongkolonnen `Tid...` fra arket og ignorerer PB/PR-kolonner.

3. Kjør kontroll:

```powershell
@'
import sqlite3
event_name = "5 km gateløp"  # Bytt ved behov, f.eks. "Halvmaraton"
con = sqlite3.connect("data/nfwa_results.sqlite3")
cur = con.cursor()
cur.execute("""
select season, count(*) as n
from results
where gender='Women'
  and event_id = (select id from events where gender='Women' and name_no=?)
  and source_url like '%kondis.no%'
  and season between 1997 and 2010
group by season
order by season desc
""", (event_name,))
for season, n in cur.fetchall():
    print(season, n)
con.close()
'@ | python -
```

---

## Datamodell

SQLite-database: `data/nfwa_results.sqlite3`

| Tabell         | Beskrivelse                                          |
|----------------|------------------------------------------------------|
| `athletes`     | Utøver-ID, kjønn, navn, fødselsdato                 |
| `events`       | Norsk øvelsesnavn, WA-kode, retning (lower/higher)  |
| `results`      | Alle resultater + WA-poeng der mulig                 |
| `clubs`        | Klubbnavn (normalisert)                              |
| `competitions` | Stevnemetadata (navn, by, stadion)                   |

Fullt skjema finnes i `nfwa/db.py`.

---

## CI/CD

`.github/workflows/update-site.yml` kjører:

- **Ukentlig** (mandager kl. 04:15 UTC)
- **Ved push til `main`**
- **Manuelt** (workflow_dispatch)

Workflowen synkroniserer data fra alle kilder, genererer `docs/`, og
publiserer til GitHub Pages via `actions/deploy-pages`.

Lokalt kan du kjøre samme flyt med:

```powershell
python -m nfwa build-site --min-year 1997 --out docs
```

---

## Bidra

Vi tar gjerne imot bidrag! Se [CONTRIBUTING.md](CONTRIBUTING.md) for
retningslinjer og oppsett av utviklingsmiljø.

## Lisens

Distribuert under [MIT-lisensen](LICENSE).

## Datakilder

- [minfriidrettsstatistikk.info](https://minfriidrettsstatistikk.info) — bane og felt (2011+)
- [kondis.no](https://kondis.no) — gateløp
- [friidrett.no](https://friidrett.no) — legacy bane og felt (2000–2010)
