"""Parser for pre-2000 manual data files (friidrett_data_old_1999_and_older/).

These are hand-transcribed CSV-style .txt files with season results from years
before the digital friidrett.no era. Each file contains multiple event sections
separated by blank lines, with an event header, a column header, and data rows.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import date
from pathlib import Path
from typing import Optional

from .minfriidrett import ScrapedResult
from .util import clean_performance, parse_ddmmyy


_HANDTID_MARKERS = re.compile(r"(?:Manuell\s+tid|Håndtid)", re.IGNORECASE)

# Standard hurdle/steeple heights for canonical event names
_HURDLE_HEIGHTS: dict[tuple[str, int], str] = {
    ("Men", 110): "106,7cm", ("Men", 200): "76,2cm",
    ("Men", 300): "91,4cm", ("Men", 400): "91,4cm",
    ("Women", 100): "84,0cm", ("Women", 200): "76,2cm",
    ("Women", 300): "76,2cm", ("Women", 400): "76,2cm",
}
_STEEPLE_HEIGHTS: dict[tuple[str, int], str] = {
    ("Men", 2000): "91,4cm", ("Men", 3000): "91,4cm",
    ("Women", 2000): "76,2cm", ("Women", 3000): "76,2cm",
}

# Keywords found in column headers
_COL_HEADER_WORDS = frozenset({
    "rank_in_list", "athlete_name", "club_name", "performance_raw",
    "plassering", "utøver", "klubb", "resultat", "sted", "dato",
    "birth_date", "birth_year", "fødselsår", "fødselsdato", "venue_city",
})


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def parse_old_data_dir(*, data_dir: Path, season: int) -> list[ScrapedResult]:
    """Parse all .txt files for a given season from the old data directory."""
    results: list[ScrapedResult] = []
    season_dir = data_dir / str(season)
    if not season_dir.exists():
        return results
    for dir_name, gender in [("menn", "Men"), ("kvinner", "Women")]:
        gender_dir = season_dir / dir_name
        if not gender_dir.exists():
            continue
        kilde_url = _read_kilde_url(gender_dir)
        for txt_file in sorted(gender_dir.glob("*.txt")):
            results.extend(
                parse_old_data_file(
                    filepath=txt_file, season=season, gender=gender,
                    kilde_url=kilde_url,
                )
            )
    return results


def parse_old_data_file(
    *, filepath: Path, season: int, gender: str, kilde_url: Optional[str] = None,
) -> list[ScrapedResult]:
    """Parse a single old data .txt file and return ScrapedResult rows."""
    text = filepath.read_text(encoding="utf-8-sig")
    if kilde_url:
        source_url = kilde_url  # Use the actual external URL as source reference
    else:
        source_url = f"old_data:{season}/{filepath.parent.name}/{filepath.name}"

    sections = _split_into_sections(text)
    results: list[ScrapedResult] = []
    prev_event: Optional[str] = None

    for raw_header, col_header, data_lines in sections:
        event_name, is_handtid = _resolve_event_name(
            raw_header=raw_header, gender=gender, prev_event=prev_event,
        )
        if not event_name:
            continue

        has_date = _col_header_has_date(col_header)
        section_results = _parse_section(
            data_lines=data_lines, season=season, gender=gender,
            event_name=event_name, is_handtid=is_handtid,
            has_date_col=has_date, source_url=source_url,
        )
        results.extend(section_results)
        if not is_handtid:
            prev_event = event_name

    return results


# ---------------------------------------------------------------------------
# Section splitting
# ---------------------------------------------------------------------------

def _split_into_sections(text: str) -> list[tuple[Optional[str], str, list[str]]]:
    """Split file into (event_header_or_None, col_header, [data_lines])."""
    lines = text.splitlines()
    sections: list[tuple[Optional[str], str, list[str]]] = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if _is_col_header(line):
            col_header = line
            i += 1
            data = _collect_data_lines(lines, i)
            i += len(data)
            if data:
                sections.append((None, col_header, data))
        elif _is_event_header(line):
            event_header = line
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            if i < len(lines) and _is_col_header(lines[i].strip()):
                col_header = lines[i].strip()
                i += 1
                data = _collect_data_lines(lines, i)
                i += len(data)
                if data:
                    sections.append((event_header, col_header, data))
        else:
            i += 1

    return sections


def _collect_data_lines(lines: list[str], start: int) -> list[str]:
    data: list[str] = []
    i = start
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            break
        data.append(line)
        i += 1
    return data


def _is_col_header(line: str) -> bool:
    low = line.lower()
    parts = {p.strip() for p in low.split(",")}
    return len(parts & _COL_HEADER_WORDS) >= 3 or "_in_list" in low


def _is_event_header(line: str) -> bool:
    if not line or _is_col_header(line):
        return False
    if re.match(r"^[\d-]", line) and "," in line:
        return False
    return any(c.isalpha() for c in line)


def _col_header_has_date(col_header: str) -> bool:
    parts = [p.strip().lower() for p in col_header.split(",")]
    return "dato" in parts


# ---------------------------------------------------------------------------
# Event name resolution
# ---------------------------------------------------------------------------

def _resolve_event_name(
    *, raw_header: Optional[str], gender: str, prev_event: Optional[str],
) -> tuple[Optional[str], bool]:
    """Return (canonical_event_name, is_handtid)."""
    if raw_header:
        return _parse_event_header(raw_header, gender)
    # Unnamed section after 5000 meter → 10000 meter
    if prev_event and "5000 meter" in prev_event:
        return "10000 meter", False
    return None, False


def _parse_event_header(header: str, gender: str) -> tuple[Optional[str], bool]:
    text = header.strip()
    is_handtid = bool(_HANDTID_MARKERS.search(text))

    # Strip timing-method suffix ("– Elektronisk tid", "– Manuell tid (Håndtid)")
    text = re.split(r"\s*[–—-]\s*(?:Elektronisk|Manuell)", text, maxsplit=1)[0].strip()
    # Strip English descriptions in parens: "(High Jump)", etc.
    text = re.split(
        r"\s*\((?:High|Pole|Long|Triple|Shot|Discus|Hammer|Javelin|Decathlon|Heptathlon)",
        text, maxsplit=1,
    )[0].strip()

    upper = text.upper().strip()

    # Hurdles
    m = re.match(r"^(\d+)\s*METER\s+HEKK", upper)
    if m:
        num = int(m.group(1))
        if is_handtid:
            return f"{num} meter hekk (Håndtid)", True
        height = _HURDLE_HEIGHTS.get((gender, num))
        return (f"{num} meter hekk ({height})" if height else f"{num} meter hekk"), False

    # Steeplechase
    m = re.match(r"^(\d+)\s*METER\s+HINDER", upper)
    if m:
        num = int(m.group(1))
        height = _STEEPLE_HEIGHTS.get((gender, num))
        return (f"{num} meter hinder ({height})" if height else f"{num} meter hinder"), False

    # Track distances
    m = re.match(r"^(\d+)\s*METER\b", upper)
    if m:
        num = int(m.group(1))
        if is_handtid:
            return f"{num} meter (Håndtid)", True
        return f"{num} meter", False

    # Field events
    if upper.startswith(("HØYDE", "HOYDE")):
        return "Høyde", False
    if upper.startswith("STAVSPRANG") or re.match(r"^STAV\b", upper):
        return "Stav", False
    if upper.startswith("LENGDE"):
        return "Lengde", False
    if upper.startswith("TRESTEG"):
        return "Tresteg", False

    # Throws (canonical names must match existing DB events exactly)
    if upper.startswith("KULE"):
        return ("Kule 7,26kg" if gender == "Men" else "Kule 4,0kg"), False
    if upper.startswith("DISKOS"):
        return ("Diskos 2,0kg" if gender == "Men" else "Diskos 1,0kg"), False
    if upper.startswith("SLEGGE"):
        return ("Slegge 7,26kg/121,5cm" if gender == "Men" else "Slegge 4,0kg/119,5cm"), False
    if upper.startswith("SPYD"):
        return ("Spyd 800gram" if gender == "Men" else "Spyd 600gram"), False

    # Combined events
    if re.match(r"^10[\s-]*KAMP", upper):
        return "10 kamp", False
    if re.match(r"^7[\s-]*KAMP", upper):
        return "7 kamp", False

    # Road running
    if upper.startswith("HALVMARATON"):
        return "Halvmaraton", False
    if upper.startswith("MARATON"):
        return "Maraton", False

    return None, False


# ---------------------------------------------------------------------------
# Data row parsing
# ---------------------------------------------------------------------------

def _parse_section(
    *, data_lines: list[str], season: int, gender: str,
    event_name: str, is_handtid: bool, has_date_col: bool,
    source_url: str,
) -> list[ScrapedResult]:
    results: list[ScrapedResult] = []
    seen_ids: set[int] = set()
    rank = 0
    prev_clean: Optional[str] = None

    for line in data_lines:
        parsed = _parse_data_row(line, has_date_col=has_date_col, season=season)
        if parsed is None:
            continue

        rank_raw, athlete_name, club_name, birth_str, venue_city, result_date, perf_raw, nationality = parsed

        # Skip non-NFIF entries
        if rank_raw == "-":
            continue
        if not athlete_name or not any(c.isalpha() for c in athlete_name):
            continue

        cleaned = clean_performance(perf_raw)
        if not cleaned or not cleaned.clean or not any(c.isdigit() for c in cleaned.clean):
            continue

        birth_date = _parse_birth(birth_str)
        athlete_id = _old_data_athlete_id(gender=gender, name=athlete_name, birth_date=birth_date)
        if athlete_id in seen_ids:
            continue
        seen_ids.add(athlete_id)

        # Competition-style ranking: tied performances share the same rank
        if cleaned.clean != prev_clean:
            rank = len(results) + 1
            prev_clean = cleaned.clean
        results.append(ScrapedResult(
            season=season,
            gender=gender,
            event_no=event_name,
            rank_in_list=rank,
            performance_raw=cleaned.raw,
            performance_clean=cleaned.clean,
            wind=cleaned.wind,
            athlete_id=athlete_id,
            athlete_name=athlete_name,
            club_name=club_name or None,
            birth_date=birth_date,
            nationality=nationality,
            placement_raw=None,
            venue_city=venue_city or None,
            stadium=None,
            competition_id=None,
            competition_name=None,
            result_date=result_date,
            source_url=source_url,
        ))

    return results


def _parse_data_row(
    line: str, *, has_date_col: bool, season: int,
) -> Optional[tuple[str, str, str, str, str, Optional[str], str, Optional[str]]]:
    """Parse a single CSV data row.

    Returns (rank, name, club, birth_str, venue, result_date_iso, performance, nationality)
    or None if unparseable.
    """
    # Replace commas inside parentheses with placeholder (handles wind like (-0,6))
    processed = _shield_parens_commas(line)

    try:
        reader = csv.reader(io.StringIO(processed))
        fields = next(reader)
    except (StopIteration, csv.Error):
        return None

    # Restore shielded commas
    fields = [f.replace("\x00", ",") for f in fields]

    if len(fields) < 5:
        return None

    # Fixed positions from left: rank(0), name(1), club(2), birth(3)
    # From right: perf(-1), and optionally date(-2) if has_date_col
    rank_s = fields[0].strip()
    name, nationality = _clean_athlete_name(fields[1].strip())
    club = fields[2].strip()
    birth = fields[3].strip()

    if has_date_col:
        if len(fields) < 6:
            return None
        perf = fields[-1].strip()
        date_raw = fields[-2].strip()
        venue = ", ".join(f.strip() for f in fields[4:-2] if f.strip())
        result_date = _parse_result_date(date_raw, season=season)
    else:
        perf = fields[-1].strip()
        venue = ", ".join(f.strip() for f in fields[4:-1] if f.strip())
        result_date = None

    return rank_s, name, club, birth, venue, result_date, perf, nationality


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _shield_parens_commas(text: str) -> str:
    """Replace commas inside parentheses with null byte for safe CSV splitting."""
    result: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
            result.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            result.append(ch)
        elif ch == "," and depth > 0:
            result.append("\x00")
        else:
            result.append(ch)
    return "".join(result)


_NATIONALITY_RE = re.compile(r"\s*\(([A-Z]{2,3})\)\s*$")


def _clean_athlete_name(name: str) -> tuple[str, Optional[str]]:
    """Extract nationality and clean athlete name.

    Returns (clean_name, nationality_code).  E.g. "John Doe (ETH)" -> ("John Doe", "ETH").
    """
    m = _NATIONALITY_RE.search(name)
    if m:
        return (name[: m.start()].strip(), m.group(1))
    return (name.strip(), None)


def _parse_birth(text: str) -> Optional[str]:
    s = text.strip()
    if not s or s.lower() in ("", "ukjent dato", "ukjent"):
        return None
    dt = parse_ddmmyy(s)
    if dt:
        return dt.isoformat()
    if re.fullmatch(r"\d{4}", s):
        return s  # Store year only as "YYYY"
    return None


def _parse_result_date(text: str, *, season: int) -> Optional[str]:
    s = text.strip().rstrip(".")
    if not s:
        return None
    dt = parse_ddmmyy(s)
    if dt:
        return dt.isoformat()
    m = re.fullmatch(r"(?P<d>\d{1,2})\.(?P<m>\d{1,2})", s)
    if m:
        try:
            return date(season, int(m.group("m")), int(m.group("d"))).isoformat()
        except ValueError:
            return None
    return None


def _old_data_athlete_id(*, gender: str, name: str, birth_date: Optional[str]) -> int:
    """Generate a stable negative athlete ID (same scheme as friidrett_legacy)."""
    key = f"friidrett|{gender}|{(name or '').strip().lower()}|{birth_date or ''}"
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    return -1 - int(n)


def _read_kilde_url(gender_dir: Path) -> Optional[str]:
    """Read the source URL from kilder/*_kilde.txt if it exists."""
    kilder_dir = gender_dir / "kilder"
    if not kilder_dir.exists():
        return None
    for f in kilder_dir.glob("*_kilde.txt"):
        text = f.read_text(encoding="utf-8-sig").strip()
        m = re.search(r"https?://\S+", text)
        if m:
            return m.group(0)
    return None
