# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Norwegian athletics statistics pipeline. Scrapes season results from minfriidrettsstatistikk.info (track & field, 2011+), friidrett.no (legacy 2000–2010), and kondis.no (road running), stores them in SQLite, calculates World Athletics (WA) points, and publishes a static dashboard via GitHub Pages.

## Commands

```powershell
# Install dependencies
python -m pip install -r requirements.txt

# Sync track & field results into database
python -m nfwa sync --years 2023 2024 2025

# Sync road running (Kondis) results
python -m nfwa sync-kondis --years 2023 2024 2025

# Generate CSV summary
python -m nfwa event-summary --season 2025

# Run local web dashboard (opens browser at http://127.0.0.1:8000/)
python -m nfwa web

# Export static site to docs/
python -m nfwa export-site --out docs

# Full pipeline: sync all sources + export (used by CI)
python -m nfwa build-site --min-year 1997 --out docs

# Look up athlete results
python -m nfwa athlete --athlete-id 29273 --since 2024
```

PowerShell range for many years: `--years (2000..2025)`

## Requirements

- Python 3.10+ (3.12 recommended)
- `requests`, `lxml` (see requirements.txt)
- `WA Poeng/wa_scoring.db` must exist (WA scoring database built from the `WA Poeng/` sub-project)

## Architecture

### Two sub-projects in one repo

1. **`nfwa/`** — Main package. Scraping, ingestion, queries, web dashboard, static site export. Entry point: `python -m nfwa` → `nfwa/__main__.py` → `nfwa/cli.py`.
2. **`WA Poeng/wa_poeng/`** — Standalone WA points toolkit. Parses the WA Scoring Tables PDF into `wa_scoring.db` and provides `ScoreCalculator` for points lookups. Imported at runtime by `nfwa` via `sys.path` insertion (see `nfwa/wa.py:ensure_wa_poeng_importable`).

### Data flow

```
Sources (HTML)
  ├── minfriidrett.py  — scrapes minfriidrettsstatistikk.info (2011+)
  ├── friidrett_legacy.py — scrapes friidrett.no legacy pages (2000–2010)
  └── kondis.py — scrapes kondis.no road race lists
        │
        ▼
    ingest.py  — sync_landsoversikt() / sync_kondis()
        │  uses: event_mapping.py (Norwegian event name → WA event code)
        │  uses: util.py (performance cleaning/normalisation)
        │  uses: wa.py (WA ScoreCalculator bridge)
        ▼
    db.py  — SQLite schema + upserts → data/nfwa_results.sqlite3
        │
        ▼
    queries.py — event_summary, event_trend, event_results, athlete_results
        │
        ├── webapp.py — local dev server (stdlib http.server + JSON API)
        ├── export_site.py — pre-renders JSON API to docs/api/*.json
        └── site_build.py — orchestrates sync + export (used by CI)
```

### Frontend

`nfwa/web_static/` contains `index.html`, `app.js`, `styles.css`. Vanilla JS, no build step. Both the live dev server (`webapp.py`) and the static export (`export_site.py`) serve these files. The static export writes pre-rendered JSON under `docs/api/`.

### Database schema (data/nfwa_results.sqlite3)

Tables: `athletes`, `events`, `results`, `clubs`, `competitions`. The `results` table has a composite unique constraint for deduplication. Schema is in `nfwa/db.py:SCHEMA`.

### Key conventions

- Gender is always `"Women"` or `"Men"` (English, title case) throughout the codebase.
- Event names are stored in Norwegian (`name_no`), mapped to WA codes via `event_mapping.py` (e.g., "100 meter" → "100m", "Kule 4,00kg" → "SP").
- Event orientation: `"lower"` = time-based (lower is better), `"higher"` = distance/points-based.
- Legacy athletes (friidrett.no, kondis.no) get negative integer IDs generated locally.
- HTML is cached under `data/cache/` to avoid re-fetching. Use `--refresh` to bypass.
- Performance normalisation (commas, colons, dots) is handled in `util.py:normalize_performance` — this is where most edge-case complexity lives.

### Adding legacy year data

For friidrett.no: add URLs to `FRIIDRETT_PAGES_<YEAR>` tuples in `nfwa/friidrett_legacy.py`, wire into `FRIIDRETT_PAGES` dict, then `sync --years <YEAR> --refresh`.

For Kondis: add season→URL pairs to the appropriate `_*_LEGACY_URLS` dict in `nfwa/kondis.py`, then `sync-kondis --years <YEAR> --refresh`.

### CI/CD

`.github/workflows/update-site.yml` runs weekly (Monday 04:15 UTC) and on push to main. It syncs data, exports `docs/`, and deploys to GitHub Pages via `actions/deploy-pages`. The WA Poeng repo (`gustavByte/world-athletics-points-calculator`) is public and cloned directly by the workflow.

## Human-readable docs

`README.md` and `CONTRIBUTING.md` (both in Norwegian) cover setup, usage, architecture, and contribution guidelines for human contributors.

## No tests

There is no test suite in this repo.
