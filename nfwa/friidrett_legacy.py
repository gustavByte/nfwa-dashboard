from __future__ import annotations

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


def pages_for_years(*, years: Iterable[int], gender: str = "Both") -> list[FriidrettPage]:
    ys = {int(y) for y in years}
    pages = [p for p in FRIIDRETT_PAGES_2010 if int(p.season) in ys]
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
    doc = html.fromstring(html_bytes)
    out: list[ScrapedResult] = []

    for h2 in doc.xpath("//h2"):
        heading_raw = (h2.text_content() or "").strip()
        event_no = _canonical_event_no(heading_raw, gender=gender)
        if not event_no:
            continue

        following_tables = h2.xpath("following::table")
        if not following_tables:
            continue

        # The pages are structured as: h2 -> (records table) -> (results table)
        results_table = following_tables[1] if len(following_tables) >= 2 else following_tables[0]
        parsed = _parse_results_table(
            table=results_table,
            season=season,
            gender=gender,
            event_no=event_no,
            source_url=source_url,
        )
        out.extend(parsed)

    return out


_WIND_CELL_RE = re.compile(r"^[+\-–−]?\s*\d+(?:,\d+)?$")


def _parse_results_table(*, table: html.HtmlElement, season: int, gender: str, event_no: str, source_url: str) -> list[ScrapedResult]:
    seen: set[int] = set()
    out: list[ScrapedResult] = []

    last_full: Optional[tuple[str, Optional[str], Optional[str]]] = None  # (name, club, birth_iso)
    rank = 0

    for tr in table.xpath(".//tr"):
        cells = [_norm_cell(c.text_content()) for c in tr.xpath("./td")]
        if not cells:
            continue

        perf_raw = cells[0]
        cleaned = clean_performance(perf_raw)
        if not cleaned or not cleaned.clean or not any(ch.isdigit() for ch in cleaned.clean):
            continue

        wind: Optional[float] = None
        has_wind = len(cells) >= 2 and _looks_like_wind(cells[1])
        idx_ath = 2 if has_wind else 1
        if has_wind:
            wind = _parse_wind(cells[1])

        if len(cells) <= idx_ath:
            continue

        athlete_cell = (cells[idx_ath] or "").strip()
        birth_raw = (cells[idx_ath + 1] or "").strip() if len(cells) > idx_ath + 1 else ""

        # Handle repeated rows: some pages abbreviate repeated athlete rows to just surname (and blank birth).
        if last_full and not birth_raw and "," not in athlete_cell:
            athlete_name, club_name, birth_iso = last_full
        else:
            athlete_name, club_name = _split_name_and_club(athlete_cell)
            birth_dt = parse_ddmmyy(birth_raw)
            birth_iso = birth_dt.isoformat() if birth_dt else None
            if athlete_name:
                last_full = (athlete_name, club_name, birth_iso)

        if not athlete_name:
            continue

        athlete_id = _friidrett_athlete_id(gender=gender, name=athlete_name, birth_date=birth_iso)
        if athlete_id in seen:
            continue
        seen.add(athlete_id)
        rank += 1

        placement = _none_if_empty(cells[idx_ath + 2] if len(cells) > idx_ath + 2 else "")
        competition_code = _none_if_empty(cells[idx_ath + 3] if len(cells) > idx_ath + 3 else "")
        venue_city = _none_if_empty(cells[idx_ath + 4] if len(cells) > idx_ath + 4 else "")
        result_date = _parse_result_date(cells[idx_ath + 5] if len(cells) > idx_ath + 5 else "", season=season)

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


def _canonical_event_no(heading: str, *, gender: str) -> Optional[str]:
    text = _norm_cell(heading)
    if not text:
        return None

    # Strip standards/notes, keep the Norwegian label (left of "/" if present)
    base = text.split("(")[0]
    base = re.split(r"\s+[–-]\s+", base, maxsplit=1)[0]
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
    if base.startswith("H\u00d8YDE") or base.startswith("HOYDE"):
        return "H\u00f8yde"
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
            return f"{num} meter hekk"
        if "HINDER" in base:
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
    if "," not in s:
        return (s, None)
    name, rest = s.split(",", 1)
    return (name.strip(), (rest.strip() or None))


def _looks_like_wind(text: str) -> bool:
    s = _norm_cell(text).replace("−", "-").replace("–", "-")
    if not s or s in {"-", "–", "—"}:
        return False
    return bool(_WIND_CELL_RE.match(s))


def _parse_wind(text: str) -> Optional[float]:
    s = _norm_cell(text).replace("−", "-").replace("–", "-")
    if not s or s in {"-", "–", "—"}:
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

