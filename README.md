# Norsk friidrett (landsoversikt) + WA-poeng (lokalt)

Dette er et lite Python-oppsett som:
- henter landsoversikt (utendørs) for Kvinner/Menn Senior fra minfriidrettsstatistikk.info
- lagrer alt i en lokal SQLite-database
- regner ut World Athletics-poeng (WA) for de øvelsene/resultatene der dette er mulig (via `WA Poeng/wa_scoring.db`)

## Kom i gang

Installer avhengigheter:

```powershell
python -m pip install -r requirements.txt
```

Krav:
- Python 3.10+ (anbefalt 3.12)
- `WA Poeng/wa_scoring.db` (eller angi egen sti med `--wa-db`, og mappe med `wa_poeng/` via `--wa-root`)

Kjør fra prosjektroten (mappa du står i nå):

```powershell
python -m nfwa sync --years 2023 2024 2025
```

For å hente flere år (t.o.m. 2003), kan du i PowerShell bruke en range:

```powershell
python -m nfwa sync --years (2003..2025)
```

Dette lager/oppdaterer:
- resultatdatabase: `data/nfwa_results.sqlite3`
- cache av HTML-sider: `data/cache/minfriidrett/`

Merk: For sesongene `2003`, `2004`, `2005`, `2006`, `2007`, `2008`, `2009` og `2010` brukes legacy-sidene på `friidrett.no` (annen HTML-struktur enn minfriidrettsstatistikk).
De caches i samme cache-mappe, og utøver-id'er genereres lokalt (negative heltalls-id'er).

### Hurtigmetode for nye legacy-år (f.eks. 2003/2002/2001)

1. Legg inn årets 13 URL-er i `FRIIDRETT_PAGES_<YEAR>` i `nfwa/friidrett_legacy.py`.
2. Koble året inn i `FRIIDRETT_PAGES` i samme fil.
3. Kjør sync med refresh:

```powershell
python -m nfwa sync --years 2003 --refresh
```

4. Kjør rask kvalitetskontroll (rader, duplikater per person/øvelse, WA-feil):

```powershell
@'
import sqlite3
year = 2003
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

5. Rebygg statiske filer og publiser:

```powershell
python -m nfwa export-site --out docs
git add nfwa/friidrett_legacy.py docs/api README.md
git commit -m "Add 2003 legacy data"
git push origin main
```

Tips: Samme navn kan være ulike personer. Bruk deduplisering per `gender + event_id + athlete_id` (ikke bare navn).

## Gateløp (Kondis)

Henter topp-lister for gateløp fra kondis.no (5 km, 10 km, halvmaraton, maraton) og legger dem inn i samme database
slik at de blir med i `event-summary` og web-dashboardet:

```powershell
python -m nfwa sync-kondis --years 2023 2024 2025
```

For å hente flere år (t.o.m. 2011), kan du i PowerShell bruke en range:

```powershell
python -m nfwa sync-kondis --years (2011..2025)
```

Dette lager/oppdaterer:
- resultatdatabase: `data/nfwa_results.sqlite3`
- cache av HTML-sider: `data/cache/kondis/`

Merk: Kondis-listene har ikke en «athlete-id» som minfriidrettsstatistikk gjør. Det genereres derfor en stabil, lokal
utøver-id (negativ heltalls-id) basert på kjønn + navn + fødselsår (slik det står i listene).

## Top3/5/10/20/50/100/150/200 per øvelse (CSV)

```powershell
python -m nfwa event-summary --season 2025
```

Skriver f.eks.:
- `data/event_summary_2025_both.csv`

Kolonnene inkluderer bl.a. snitt av `wa_points` (top-N) og snitt av prestasjon:
- `avg_value_top_n_perf` (tall: sekunder for løp/gange, meter/poeng for andre)
- `avg_perf_top_n` (formatert streng, f.eks. `4,36,23` for 1500m)

## Web-dashboard (lokalt)

```powershell
python -m nfwa web
```

Dette starter en liten lokal nettside (default `http://127.0.0.1:8000/`) med:
- trend per øvelse mellom år (snitt WA-poeng og snitt resultat)
- sesongoversikt (sorter på poeng/resultat)
- utøveroppslag (athlete-id)

## Publisering (GitHub Pages)

Denne repoen kan publiseres som en statisk side (ingen server) via GitHub Pages.
Den statiske siden lages lokalt i `docs/` med:

```powershell
python -m nfwa export-site --out docs
```

### Raskeste vei ut (offentlig, anbefalt)

1. Lag en ny **public** repo på GitHub
2. Push denne mappa til repoen
3. I GitHub: **Settings → Pages**
   - Source: **GitHub Actions**

Da blir siden tilgjengelig på `https://<brukernavn>.github.io/<repo>/`.

### Ukentlig oppdatering (GitHub Actions)

Det er lagt ved en workflow: `.github/workflows/update-site.yml` som:
- kjører ukentlig (mandager)
- oppdaterer databasen fra kildene
- re-genererer `docs/`
- publiserer siden direkte med GitHub Pages Actions (ingen commit av `docs/`)

Du må:
- ha repoen på GitHub (public)
- ha GitHub Pages satt til **GitHub Actions**
- evt. endre branch i workflowen hvis du bruker `master` i stedet for `main`
- hvis `world-athletics-points-calculator` (WA Poeng) er **private**: legg inn en repo-secret `WA_POENG_TOKEN` (PAT med tilgang til å lese repoen)

Lokalt kan du kjøre samme flyt med:

```powershell
python -m nfwa build-site --out docs
```

## Slå opp utøver

```powershell
python -m nfwa athlete --athlete-id 29273 --since 2024
```

`athlete-id` er `showathl=...`-tallet i lenkene på minfriidrettsstatistikk.

## Datamodell (kort)

SQLite-tabeller:
- `athletes` (utøver-id, navn, fødselsdato)
- `events` (norsk øvelsesnavn, evt. WA-øvelse, orientering)
- `results` (alle rader/resultater + WA-poeng der mulig)
- `clubs`, `competitions` (normalisering/metadata)

## Kilder som brukes

Koden bygger URL-er tilsvarende:
- Kvinner Senior utendørs: `showclass=22`
- Menn Senior utendørs: `showclass=11`

…med `outdoor=Y` og `showseason=YYYY`.
