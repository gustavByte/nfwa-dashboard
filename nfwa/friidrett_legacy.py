from __future__ import annotations

from collections import defaultdict
import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

import requests
from lxml import html

from .minfriidrett import ScrapedResult
from .util import clean_performance, parse_ddmmyy


@dataclass(frozen=True)
class FriidrettPage:
    season: int
    gender: str  # "Women" | "Men"
    url: str


FRIIDRETT_PAGES_2008: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2008
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/ededda76178747499ab11bea8ebaa930.aspx"),  # Sprint
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/47d7e60b56c24727b14b0df456ebb049.aspx"),  # Distanse
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/0329d071badb421ebdbae98d140c7ccf.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/d00ff6eaace545ffaa3e97f7f2a658be.aspx"),  # Hoppøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/14ef5ded64ec4edc84de594fc0929cab.aspx"),  # Kastøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/2b28b7d7700a496794d78ebd385aaacd.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2008
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/3ff25f4a57bb445c9643a678d7dc259e.aspx"),  # Sprint
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/7d498b1130774467a50e2918667213df.aspx"),  # Distanse
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/62dec3aef5414932af0395bf434f4f21.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/1d94cc63d00f48cebe4be05fec33aa9a.aspx"),  # Hoppøvelser
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/8a94b13eb9d34f1b8e1864b7b6bb67b9.aspx"),  # Kastøvelser
    FriidrettPage(
        season=2008,
        gender="Women",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-ksmk08.htm",
    ),  # Mangekamp (dead on current site, kept as fallback URL)
    # Senior Kappgang 2008 (URL currently appears dead, but kept so ingest can recover if source returns later)
    FriidrettPage(
        season=2008,
        gender="Men",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-kappgangs2008.pdf",
    ),
    FriidrettPage(
        season=2008,
        gender="Women",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-kappgangs2008.pdf",
    ),
)

FRIIDRETT_PAGES_2010: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2010
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/9f75977878cc4932809862cd399e435c.aspx"),  # Sprint
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/ef7554091d4f4e3eb3d27159365e2f82.aspx"),  # Distanse
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/01774b1d5d9842ddb8622316090d03b7.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/580473c8526f4e0d879df48950427fe0.aspx"),  # Hoppøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/97eefbd05e3b4b7aad6f13569801a065.aspx"),  # Kastøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/2d3b2204f863462c8b3f79a57010357d.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2010
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/e21697d0f7db47fcb77d6825cda87118.aspx"),  # Sprint
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/38589b538d324a7eacfd96e33ac85316.aspx"),  # Distanse
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/3a3ffae3dd724e7f89ebfe9555ef561a.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/24faa01d343a4e25807beddb39f4b73b.aspx"),  # Hoppøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/5be74b7a9c3a4d9089371d20f19fb7d5.aspx"),  # Kastøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/2f5b992e90744492b8a25ad530088cd2.aspx"),  # Mangekamp
)

FRIIDRETT_PAGES: tuple[FriidrettPage, ...] = FRIIDRETT_PAGES_2008 + FRIIDRETT_PAGES_2010


def pages_for_years(*, years: Iterable[int], gender: str = "Both") -> list[FriidrettPage]:
    ys = {int(y) for y in years}
    pages = [p for p in FRIIDRETT_PAGES if int(p.season) in ys]
    if gender == "Both":
        return pages
    return [p for p in pages if p.gender == gender]


def fetch_page(
    *,
    url: str,
    cache_dir: Path,
    refresh: bool = False,
    session: Optional[requests.Session] = None,
) -> bytes:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / _safe_cache_filename(url)
    if cache_path.exists() and not refresh:
        return cache_path.read_bytes()

    sess = session or requests.Session()
    headers = {"User-Agent": "nfwa-local/0.1 (contact: local)"}
    resp = sess.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    content = resp.content
    cache_path.write_bytes(content)
    return content


def parse_page(*, html_bytes: bytes, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    """Parse friidrett.no Word-HTML pages (legacy) and return best-per-athlete rows per event."""
    if html_bytes.lstrip().startswith(b"%PDF"):
        # 2008 race-walk currently points to a PDF; keep a hard-fail-safe here so sync can continue.
        return []

    doc = html.fromstring(html_bytes)
    if _looks_like_not_found_page(doc):
        return []

    out: list[ScrapedResult] = []

    for h2 in doc.xpath("//h2"):
        heading_raw = (h2.text_content() or "").strip()
        event_no = _canonical_event_no(heading_raw, gender=gender)
        if not event_no:
            continue

        tables = _tables_until_next_h2(h2)
        if not tables:
            continue

        # 2010 usually has records+results, while 2008 often has a single table.
        # Parse all candidates and keep the one with most valid result rows.
        best: list[ScrapedResult] = []
        for table in tables:
            parsed = _parse_results_table(
                table=table,
                season=season,
                gender=gender,
                event_no=event_no,
                source_url=source_url,
            )
            if len(parsed) > len(best):
                best = parsed
        out.extend(best)

    if out:
        return out

    # Fallback for pages with no <h2>-per-event structure (e.g. 2008 women throws).
    return _parse_sectioned_table_page(doc=doc, season=season, gender=gender, source_url=source_url)


_WIND_CELL_RE = re.compile(r"^[+\-–−]?\s*\d+(?:[.,]\d+)?$")
_PLACEMENT_CELL_RE = re.compile(r"^\(?\d+[A-Za-z0-9/.-]*\)?[A-Za-z0-9/.-]*$")

_HURDLE_HEIGHT_CM: dict[tuple[str, int], str] = {
    ("Women", 60): "84,0",
    ("Women", 100): "84,0",
    ("Women", 200): "76,2",
    ("Women", 300): "76,2",
    ("Women", 400): "76,2",
    ("Men", 110): "106,7",
    ("Men", 200): "76,2",
    ("Men", 300): "91,4",
    ("Men", 400): "91,4",
}

_STEEPLE_HEIGHT_CM: dict[tuple[str, int], str] = {
    ("Women", 2000): "76,2",
    ("Women", 3000): "76,2",
    ("Men", 2000): "91,4",
    ("Men", 3000): "91,4",
}


def _parse_results_table(*, table: html.HtmlElement, season: int, gender: str, event_no: str, source_url: str) -> list[ScrapedResult]:
    seen: set[int] = set()
    out: list[ScrapedResult] = []

    last_full: Optional[tuple[str, Optional[str], Optional[str]]] = None  # (name, club, birth_iso)
    rank = 0

    for tr in table.xpath(".//tr"):
        cells = [_norm_cell(c.text_content()) for c in tr.xpath("./td|./th")]
        if not cells:
            continue

        parsed = _parse_result_cells(cells=cells, season=season, last_full=last_full)
        if not parsed:
            continue

        (
            cleaned,
            wind,
            athlete_name,
            club_name,
            birth_iso,
            placement,
            competition_code,
            venue_city,
            result_date,
            next_last_full,
        ) = parsed
        last_full = next_last_full

        athlete_id = _friidrett_athlete_id(gender=gender, name=athlete_name, birth_date=birth_iso)
        if athlete_id in seen:
            continue
        seen.add(athlete_id)
        rank += 1

        out.append(
            ScrapedResult(
                season=int(season),
                gender=gender,
                event_no=event_no,
                rank_in_list=rank,
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=wind,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=club_name,
                birth_date=birth_iso,
                placement_raw=placement,
                venue_city=venue_city,
                stadium=None,
                competition_id=None,
                competition_name=competition_code,
                result_date=result_date,
                source_url=source_url,
            )
        )

    return out


def _parse_sectioned_table_page(*, doc: html.HtmlElement, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    best: list[ScrapedResult] = []
    for table in doc.xpath("//table"):
        parsed = _parse_sectioned_table(table=table, season=season, gender=gender, source_url=source_url)
        if len(parsed) > len(best):
            best = parsed
    return best


def _parse_sectioned_table(*, table: html.HtmlElement, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    out: list[ScrapedResult] = []
    seen_by_event: dict[str, set[int]] = defaultdict(set)
    rank_by_event: dict[str, int] = defaultdict(int)
    last_full_by_event: dict[str, tuple[str, Optional[str], Optional[str]]] = {}

    current_event: Optional[str] = None
    for tr in table.xpath(".//tr"):
        cells = [_norm_cell(c.text_content()) for c in tr.xpath("./td|./th")]
        if not cells:
            continue

        heading = _section_heading_candidate(cells)
        if heading is not None:
            current_event = _canonical_event_no(heading, gender=gender)
            continue

        if not current_event:
            continue

        parsed = _parse_result_cells(cells=cells, season=season, last_full=last_full_by_event.get(current_event))
        if not parsed:
            continue

        (
            cleaned,
            wind,
            athlete_name,
            club_name,
            birth_iso,
            placement,
            competition_code,
            venue_city,
            result_date,
            next_last_full,
        ) = parsed
        last_full_by_event[current_event] = next_last_full

        athlete_id = _friidrett_athlete_id(gender=gender, name=athlete_name, birth_date=birth_iso)
        if athlete_id in seen_by_event[current_event]:
            continue
        seen_by_event[current_event].add(athlete_id)

        rank_by_event[current_event] += 1
        out.append(
            ScrapedResult(
                season=int(season),
                gender=gender,
                event_no=current_event,
                rank_in_list=int(rank_by_event[current_event]),
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=wind,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=club_name,
                birth_date=birth_iso,
                placement_raw=placement,
                venue_city=venue_city,
                stadium=None,
                competition_id=None,
                competition_name=competition_code,
                result_date=result_date,
                source_url=source_url,
            )
        )

    return out


def _parse_result_cells(
    *,
    cells: list[str],
    season: int,
    last_full: Optional[tuple[str, Optional[str], Optional[str]]],
) -> Optional[tuple]:
    if not cells:
        return None

    cleaned = clean_performance(cells[0])
    if not cleaned or not cleaned.clean or not any(ch.isdigit() for ch in cleaned.clean):
        return None

    has_wind = len(cells) >= 2 and _looks_like_wind(cells[1])
    wind = _parse_wind(cells[1]) if has_wind else None

    idx_ath = _guess_athlete_index(cells=cells, has_wind=has_wind, last_full=last_full)
    if idx_ath is None or idx_ath >= len(cells):
        return None

    athlete_cell = (cells[idx_ath] or "").strip()
    birth_raw = (cells[idx_ath + 1] or "").strip() if len(cells) > idx_ath + 1 else ""

    if _is_abbreviated_repeat(athlete_cell, birth_raw=birth_raw, last_full=last_full):
        athlete_name, club_name, prev_birth = last_full  # type: ignore[misc]
        birth_iso = _parse_birth_date(birth_raw) or prev_birth
        next_last_full = (athlete_name, club_name, birth_iso)
    else:
        athlete_name, club_name = _split_name_and_club(athlete_cell)
        if not athlete_name:
            return None
        birth_iso = _parse_birth_date(birth_raw)
        next_last_full = (athlete_name, club_name, birth_iso)

    placement = _extract_placement(cells=cells, idx_ath=idx_ath)
    result_date, date_idx = _extract_result_date(cells=cells, idx_ath=idx_ath, season=season)
    competition_code, venue_city = _extract_comp_and_venue(cells=cells, idx_ath=idx_ath, date_idx=date_idx)

    return (
        cleaned,
        wind,
        athlete_name,
        club_name,
        birth_iso,
        placement,
        competition_code,
        venue_city,
        result_date,
        next_last_full,
    )


def _guess_athlete_index(*, cells: list[str], has_wind: bool, last_full: Optional[tuple[str, Optional[str], Optional[str]]]) -> Optional[int]:
    start = 2 if has_wind else 1
    for cand in (start, start + 1):
        if cand < len(cells) and _is_likely_athlete_cell(cells[cand], last_full=last_full):
            return cand

    # Fallback for odd column layouts: scan only the early columns (before venue/date).
    for cand in range(1, min(len(cells), 6)):
        if _is_likely_athlete_cell(cells[cand], last_full=last_full):
            return cand
    return None


def _is_likely_athlete_cell(text: str, *, last_full: Optional[tuple[str, Optional[str], Optional[str]]]) -> bool:
    s = _norm_cell(text)
    if not s:
        return False
    if not any(ch.isalpha() for ch in s):
        return False
    if _looks_like_wind(s) or _looks_like_placement(s):
        return False
    if "," in s:
        return True
    if len(s.split()) >= 2:
        return True
    return bool(last_full and _looks_like_abbrev_name(s))


def _looks_like_abbrev_name(text: str) -> bool:
    s = _norm_cell(text)
    if not s or any(ch.isdigit() for ch in s):
        return False
    parts = s.split()
    if len(parts) != 1:
        return False
    token = parts[0]
    return bool(token) and any(ch.islower() for ch in token[1:])


def _is_abbreviated_repeat(
    athlete_cell: str,
    *,
    birth_raw: str,
    last_full: Optional[tuple[str, Optional[str], Optional[str]]],
) -> bool:
    if not last_full:
        return False
    if birth_raw:
        return False
    s = _norm_cell(athlete_cell)
    if "," in s:
        return False
    return _looks_like_abbrev_name(s)


def _extract_placement(*, cells: list[str], idx_ath: int) -> Optional[str]:
    if idx_ath > 1 and _looks_like_placement(cells[idx_ath - 1]):
        return _none_if_empty(cells[idx_ath - 1])
    if len(cells) > idx_ath + 2 and _looks_like_placement(cells[idx_ath + 2]):
        return _none_if_empty(cells[idx_ath + 2])
    return None


def _extract_result_date(*, cells: list[str], idx_ath: int, season: int) -> tuple[Optional[str], Optional[int]]:
    for i in range(idx_ath + 2, len(cells)):
        parsed = _parse_result_date(cells[i], season=season)
        if parsed:
            return parsed, i
    return None, None


def _extract_comp_and_venue(*, cells: list[str], idx_ath: int, date_idx: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    if date_idx is None:
        return None, None

    mids = [i for i in range(idx_ath + 2, date_idx) if _none_if_empty(cells[i])]
    if not mids:
        return None, None

    non_place = [i for i in mids if not _looks_like_placement(cells[i])]
    if not non_place:
        return None, None

    venue_idx = non_place[-1]
    venue_city = _clean_venue(cells[venue_idx])

    comp_candidates = [i for i in non_place if i < venue_idx]
    competition_code = _none_if_empty(cells[comp_candidates[-1]]) if comp_candidates else None
    return competition_code, venue_city


def _canonical_event_no(heading: str, *, gender: str) -> Optional[str]:
    text = _norm_cell(heading)
    if not text:
        return None

    # Strip standards/notes, keep the Norwegian label (left of "/" if present)
    base = text.split("(")[0]
    base = re.split(r"\s+[–—-]\s+", base, maxsplit=1)[0]
    base = base.split("/")[0]
    base = _norm_cell(base).upper()

    # Combined events
    base_norm = base.replace("-", " ").strip()
    if base_norm.startswith("10 KAMP"):
        return "10 kamp"
    if base_norm.startswith("7 KAMP"):
        return "7 kamp"
    if base_norm.startswith("5 KAMP"):
        return "5 kamp"
    if base_norm.startswith("KAST 5 KAMP") or base_norm.startswith("KAST 5-KAMP"):
        return "Kast 5 Kamp (Slegge-Kule-Diskos-Spyd-Vektkast)"

    # Field events
    if base.startswith("HØYDE") or base.startswith("HOYDE"):
        return "Høyde"
    if base.startswith("STAV"):
        return "Stav"
    if base.startswith("LENGDE"):
        return "Lengde"
    if base.startswith("TRESTEG"):
        return "Tresteg"

    # Throws (canonicalize to the same event names used by minfriidrett)
    if base.startswith("KULE"):
        return "Kule 7,26kg" if gender == "Men" else "Kule 4,0kg"
    if base.startswith("DISKOS"):
        return "Diskos 2,0kg" if gender == "Men" else "Diskos 1,0kg"
    if base.startswith("SLEGGE"):
        return "Slegge 7,26kg/121,5cm" if gender == "Men" else "Slegge 4,0kg/119,5cm"
    if base.startswith("SPYD"):
        return "Spyd 800gram" if gender == "Men" else "Spyd 600gram"
    if base.startswith("VEKTKAST"):
        return "VektKast 15,88Kg" if gender == "Men" else "VektKast 9,08Kg"
    if base.startswith("SUPERVEKTKAST"):
        return "SuperVektKast 25,4Kg" if gender == "Men" else "SuperVektKast 15,88Kg"

    # Track events: distance, hurdles, steeplechase
    m = re.match(r"^(?P<num>[\d ]+)\s*METER\b", base)
    if m:
        num = int(m.group("num").replace(" ", ""))
        if "HEKK" in base:
            height = _HURDLE_HEIGHT_CM.get((gender, num))
            if height:
                return f"{num} meter hekk ({height}cm)"
            return f"{num} meter hekk"
        if "HINDER" in base:
            height = _STEEPLE_HEIGHT_CM.get((gender, num))
            if height:
                return f"{num} meter hinder ({height}cm)"
            return f"{num} meter hinder"
        return f"{num} meter"

    return None


def _friidrett_athlete_id(*, gender: str, name: str, birth_date: Optional[str]) -> int:
    key = f"friidrett|{gender}|{(name or '').strip().lower()}|{birth_date or ''}"
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    return -1 - int(n)


def _safe_cache_filename(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    path = re.sub(r"^https?://", "", url)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", path).strip("_").lower()
    slug = slug[:80] if slug else "friidrett"
    return f"{slug}_{digest}.html"


def _norm_cell(text: str) -> str:
    s = (text or "").replace("\u00a0", " ").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", s)


def _split_name_and_club(text: str) -> tuple[str, Optional[str]]:
    s = _norm_cell(text)
    if not s:
        return ("", None)
    # Some friidrett.no legacy pages contain placeholder rows where the athlete cell is e.g. "–––"
    # (no actual name). Treat any cell without letters as missing.
    if not any(ch.isalpha() for ch in s):
        return ("", None)
    if "," not in s:
        return (s, None)
    name, rest = s.split(",", 1)
    return (name.strip(), (rest.strip() or None))


def _looks_like_wind(text: str) -> bool:
    s = _norm_cell(text).replace("−", "-").replace("–", "-").replace("—", "-")
    if not s or s == "-":
        return False
    return bool(_WIND_CELL_RE.match(s))


def _parse_wind(text: str) -> Optional[float]:
    s = _norm_cell(text).replace("−", "-").replace("–", "-").replace("—", "-")
    if not s or s == "-":
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _parse_result_date(text: str, *, season: int) -> Optional[str]:
    s = _norm_cell(text).strip().rstrip(".")
    if not s:
        return None

    # Full date dd.mm.yy
    if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{2}", s):
        dt = parse_ddmmyy(s)
        return dt.isoformat() if dt else None

    # Date range: 28/29.07
    m = re.fullmatch(r"(?P<d1>\d{1,2})(?:/\d{1,2})\.(?P<m>\d{1,2})", s)
    if m:
        try:
            return date(int(season), int(m.group("m")), int(m.group("d1"))).isoformat()
        except ValueError:
            return None

    # dd.mm (with or without trailing dot)
    m = re.fullmatch(r"(?P<d>\d{1,2})\.(?P<m>\d{1,2})", s)
    if m:
        try:
            return date(int(season), int(m.group("m")), int(m.group("d"))).isoformat()
        except ValueError:
            return None

    return None


def _none_if_empty(text: str) -> Optional[str]:
    s = _norm_cell(text)
    return s if s else None


def _section_heading_candidate(cells: list[str]) -> Optional[str]:
    non_empty = [c for c in cells if c]
    if not non_empty:
        return None
    if len(non_empty) == 1 and any(ch.isalpha() for ch in non_empty[0]):
        return non_empty[0]
    first = cells[0]
    if first and any(ch.isalpha() for ch in first) and all(not c for c in cells[1:]):
        return first
    return None


def _looks_like_placement(text: str) -> bool:
    s = _norm_cell(text)
    if not s:
        return False
    if _looks_like_wind(s):
        return False
    return bool(_PLACEMENT_CELL_RE.fullmatch(s))


def _parse_birth_date(text: str) -> Optional[str]:
    s = _norm_cell(text)
    if not s:
        return None

    # Legacy pages sometimes use "dd.mm yy" for births.
    s = re.sub(r"^(\d{1,2}\.\d{1,2})\s+(\d{2})$", r"\1.\2", s)
    dt = parse_ddmmyy(s)
    return dt.isoformat() if dt else None


def _clean_venue(text: str) -> Optional[str]:
    s = _none_if_empty(text)
    if not s:
        return None
    s = s.rstrip(",").strip()
    return s or None


def _tables_until_next_h2(h2: html.HtmlElement) -> list[html.HtmlElement]:
    tables: list[html.HtmlElement] = []
    seen: set[int] = set()
    sib = h2.getnext()
    while sib is not None:
        if isinstance(sib.tag, str) and sib.tag.lower() == "h2":
            break
        candidates = [sib] if isinstance(sib.tag, str) and sib.tag.lower() == "table" else sib.xpath(".//table")
        for table in candidates:
            key = id(table)
            if key in seen:
                continue
            seen.add(key)
            tables.append(table)
        sib = sib.getnext()
    return tables


def _looks_like_not_found_page(doc: html.HtmlElement) -> bool:
    title = _norm_cell(" ".join(doc.xpath("//title/text()"))).lower()
    if "vi fant ikke siden" in title:
        return True
    body = _norm_cell(doc.text_content()).lower()
    if "microsoftonline.com" in body and "oauth2/authorize" in body:
        return True
    return False
