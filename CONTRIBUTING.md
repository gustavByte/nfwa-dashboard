# Bidra til NFWA Dashboard

Takk for at du vil bidra! Her er retningslinjene for prosjektet.

## Rapportere feil eller foreslå forbedringer

- Bruk [issue-malene](https://github.com/gustavByte/nfwa-dashboard/issues/new/choose) på GitHub.
- Sjekk først om det finnes en eksisterende issue.

## Utviklingsmiljø

### Forutsetninger

- Python 3.10+ (anbefalt 3.12)
- Git

### Oppsett

1. Fork og klon repoet:

```powershell
git clone https://github.com/<ditt-brukernavn>/nfwa-dashboard.git
cd nfwa-dashboard
```

2. Klon WA Poeng-repoet (offentlig) inn i prosjektmappen:

```powershell
git clone https://github.com/gustavByte/world-athletics-points-calculator.git "WA Poeng"
```

3. Installer avhengigheter:

```powershell
python -m pip install -r requirements.txt
```

4. Verifiser oppsettet:

```powershell
python -m nfwa sync --years 2025
python -m nfwa web
```

Åpne `http://127.0.0.1:8000/` i nettleseren — du skal se dashboardet.

## Arbeidsflyt for pull requests

1. Lag en ny branch fra `main`:

```powershell
git checkout -b din-endring
```

2. Gjør endringene dine.
3. Test lokalt (se sjekklisten i PR-malen).
4. Commit og push:

```powershell
git push origin din-endring
```

5. Opprett en pull request mot `main`.

## Kodekonvensjoner

### Kjønn

Kjønn er alltid `"Women"` eller `"Men"` (engelsk, stor forbokstav) gjennom hele kodebasen.

### Øvelsesnavn

Øvelser lagres med norske navn (`name_no`) i databasen og mappes til WA-koder
via `nfwa/event_mapping.py`. Eksempler:

| Norsk (`name_no`)   | WA-kode |
|----------------------|---------|
| `100 meter`          | `100m`  |
| `Kule 4,00kg`        | `SP`    |
| `5 km gateløp`       | `5RR`   |
| `Halvmaraton`         | `Half`  |

### Øvelsesretning

- `"lower"` = tidsbasert (lavere er bedre) — løp, kappgang
- `"higher"` = lengde/høyde/poengbasert (høyere er bedre)

### Utøver-IDer

- Utøvere fra minfriidrettsstatistikk.info: positive heltalls-IDer (fra kilden)
- Legacy-utøvere (friidrett.no, kondis.no): negative heltalls-IDer (generert lokalt)

### Prestasjonsformat

Rensing og normalisering av prestasjoner (komma, kolon, punktum) håndteres
i `nfwa/util.py:normalize_performance`. Dette er den mest komplekse delen
av kodebasen når det gjelder kanttilfeller.

## Legge til nye legacy-år

### friidrett.no (2000–2010)

1. Legg inn årets URLer i `FRIIDRETT_PAGES_<YEAR>` i `nfwa/friidrett_legacy.py`.
2. Koble året inn i `FRIIDRETT_PAGES`-dicten i samme fil.
3. Kjør: `python -m nfwa sync --years <ÅR> --refresh`

### Kondis (gateløp)

1. Legg inn `sesong → URL`-par i riktig dict i `nfwa/kondis.py`
   (f.eks. `_FIVE_KM_WOMEN_LEGACY_URLS`).
2. Kjør: `python -m nfwa sync-kondis --years <ÅR> --refresh`

## Lisens

Ved å bidra godtar du at bidraget ditt lisensieres under
[PolyForm Noncommercial 1.0.0](LICENSE).
