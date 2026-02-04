from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional


_WIND_RE = re.compile(r"\((?P<wind>[+-]\d+(?:,\d+)?)\)")
_HANDTIMED_RE = re.compile(r"(?P<perf>.+?)(?:\s*[hH])$")
_PARENS_RE = re.compile(r"\([^)]*\)")
_TRAILING_LETTERS_RE = re.compile(r"(?P<perf>.+?)(?:\s*[A-Za-z]{1,3})$")


@dataclass(frozen=True)
class CleanPerformance:
    raw: str
    clean: str
    wind: Optional[float]


def parse_ddmmyy(value: str, *, pivot_year: int | None = None) -> Optional[date]:
    text = (value or "").strip()
    if not text:
        return None
    parts = text.split(".")
    if len(parts) != 3:
        return None
    try:
        day = int(parts[0])
        month = int(parts[1])
        year_2 = int(parts[2])
    except ValueError:
        return None
    if pivot_year is None:
        pivot_year = date.today().year % 100
    year = 2000 + year_2 if year_2 <= pivot_year else 1900 + year_2
    try:
        return date(year, month, day)
    except ValueError:
        return None


def clean_performance(raw_value: str) -> Optional[CleanPerformance]:
    raw = (raw_value or "").strip()
    if not raw or raw == "-----":
        return None

    wind: Optional[float] = None
    wind_match = _WIND_RE.search(raw)
    if wind_match:
        try:
            wind = float(wind_match.group("wind").replace(",", "."))
        except ValueError:
            wind = None

    clean = _WIND_RE.sub("", raw).strip()

    # Remove other annotations like "(ok)" etc.
    clean = _PARENS_RE.sub("", clean).strip()

    # Normalise odd time separators seen in some sources, e.g.:
    #   1´11,50 / 1'11,50 / 1′11,50  -> 1:11,50
    clean = clean.translate(
        str.maketrans(
            {
                "\u00b4": ":",  # acute accent often used as minute marker
                "\u2032": ":",  # prime
                "\u2019": ":",  # right single quote
                "\u2018": ":",  # left single quote
                "\u02bc": ":",  # modifier letter apostrophe
                "'": ":",
            }
        )
    )

    # Some sources (incl. some Kondis tables) use "-" / "–" between digits as a time separator, e.g. "3.33-07".
    # Normalise these to dots so time normalisation can handle them consistently.
    clean = re.sub(r"(?<=\d)[-–](?=\d)", ".", clean)

    # Strip common suffixes like hand-timed "h"
    ht_match = _HANDTIMED_RE.match(clean)
    if ht_match:
        clean = ht_match.group("perf").strip()

    # Strip other short suffixes like "mx", "A", etc.
    suffix_match = _TRAILING_LETTERS_RE.match(clean)
    if suffix_match and any(ch.isdigit() for ch in suffix_match.group("perf")):
        clean = suffix_match.group("perf").strip()

    # Strip trailing junk (e.g. "+", trailing ","), but keep the original in `raw`.
    while clean and not clean[-1].isdigit():
        clean = clean[:-1].strip()

    # Strip leading junk (seen rarely, e.g. ",´11,50" in the source HTML).
    if clean and not clean[0].isdigit() and any(ch.isdigit() for ch in clean):
        while clean and not clean[0].isdigit():
            clean = clean[1:].strip()

    # Fix obvious separator glitches like "12,,07" and "12..07"
    clean = re.sub(r",{2,}", ",", clean)
    clean = re.sub(r"\.{2,}", ".", clean)

    # Normalise whitespace
    clean = re.sub(r"\s+", " ", clean)
    return CleanPerformance(raw=raw, clean=clean, wind=wind)


def normalize_performance(*, performance: str, orientation: str, wa_event: str | None = None) -> str:
    """Make a performance string parseable for WA-poeng + numeric normalisation."""
    text = (performance or "").strip()
    if not text:
        return ""

    # Time-like strings can appear as:
    # - 1,05,71  => 1:05.71
    # - 11,15,59 => 11:15.59
    # - 1,12,54  => 1:12:54   (long events)
    # - 2.22,28  => 2:22.28
    if orientation == "lower":
        text = _normalize_time_like(text, wa_event=wa_event)
    else:
        # For distances/points, standardise decimals but keep it readable.
        text = text.replace(",", ".") if _looks_like_number(text) else text

    return text


def _normalize_time_like(text: str, *, wa_event: str | None) -> str:
    text = text.strip()
    if not text:
        return text

    # Dot-separated segments (e.g. 29.11.45 => 29:11.45)
    if ":" not in text and "." in text and "," not in text and text.count(".") >= 2:
        parts = [p.strip() for p in text.split(".") if p.strip()]
        if parts and all(p.isdigit() for p in parts):
            nums = [int(p) for p in parts]
            hours_likely = _event_likely_has_hours(wa_event)

            if len(nums) == 3:
                a, b, c = nums
                if hours_likely and a <= 9 and b <= 59 and c <= 59:
                    return f"{a}:{b:02d}:{c:02d}"
                return f"{a}:{b:02d}.{c:02d}"
            if len(nums) == 4:
                a, b, c, d = nums
                if hours_likely and a <= 9 and b <= 59 and c <= 59:
                    return f"{a}:{b:02d}:{c:02d}.{d:02d}"
                return f"{a}:{b:02d}:{c:02d}.{d:02d}"

    # Single-dot format can be used as mm.ss (e.g. 15.45 => 15:45).
    if ":" not in text and text.count(".") == 1 and "," not in text and wa_event:
        parts = [p.strip() for p in text.split(".")]
        if len(parts) == 2 and all(p.isdigit() for p in parts) and len(parts[1]) == 2:
            if _event_likely_minsec_sep(wa_event) and int(parts[1]) <= 59:
                a, b = (int(parts[0]), int(parts[1]))
                return f"{a}:{b:02d}"

    # Dot-separated segments + decimal comma (e.g. 2.22,28)
    if ":" not in text and "." in text and "," in text and text.count(",") == 1:
        text = text.replace(".", ":")
        text = text.replace(",", ".")
        return text

    # Comma-separated segments (e.g. 11,05,98 or 1,12,54)
    if ":" not in text and text.count(",") >= 2 and "." not in text:
        parts = [p.strip() for p in text.split(",")]
        if all(p.isdigit() for p in parts):
            nums = [int(p) for p in parts]
            hours_likely = _event_likely_has_hours(wa_event)

            if len(nums) == 3:
                a, b, c = nums
                if hours_likely and a <= 9 and b <= 59 and c <= 59:
                    return f"{a}:{b:02d}:{c:02d}"
                return f"{a}:{b:02d}.{c:02d}"

            if len(nums) == 4:
                a, b, c, d = nums
                if hours_likely and a <= 9 and b <= 59 and c <= 59:
                    return f"{a}:{b:02d}:{c:02d}.{d:02d}"
                # Fall back to treating last as hundredths.
                return f"{a}:{b:02d}:{c:02d}.{d:02d}"

    # Single comma decimal (e.g. 7,48)
    if ":" not in text and text.count(",") == 1 and "." not in text:
        parts = [p.strip() for p in text.split(",")]
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            a, b = (int(parts[0]), int(parts[1]))
            if wa_event and _event_likely_minsec_sep(wa_event) and 0 <= b <= 59 and len(parts[1]) == 2:
                return f"{a}:{b:02d}"
        return text.replace(",", ".")

    return text.replace(",", ".")


def _looks_like_number(text: str) -> bool:
    return bool(re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", text.strip()))


def _event_likely_has_hours(wa_event: str | None) -> bool:
    if not wa_event:
        return False

    # Obvious long events
    if wa_event in {"Marathon", "MarW", "HM", "HMW", "100 km", "100 Miles"}:
        return True

    # km-based (walk + road)
    m = re.match(r"^(?P<km>\d+)\s*km\b", wa_event)
    if m:
        return int(m.group("km")) >= 10
    m = re.match(r"^(?P<km>\d+)km\s+W$", wa_event)
    if m:
        return int(m.group("km")) >= 10

    # Meter-based walk with thousand separators, e.g. 10,000mW
    m = re.match(r"^(?P<m>\d{1,3}(?:,\d{3})*)mW$", wa_event)
    if m:
        meters = int(m.group("m").replace(",", ""))
        return meters >= 10000

    return False


def _event_likely_minsec_sep(wa_event: str) -> bool:
    if wa_event in {"Marathon", "HM", "HMW", "MarW"}:
        return True

    # km-based (walk + road)
    if re.match(r"^(?P<km>\d+)\s*km\b", wa_event) or re.match(r"^(?P<km>\d+)km\b", wa_event):
        return True
    if re.match(r"^(?P<km>\d+)km\s+W$", wa_event):
        return True

    # Walk meters (e.g. 3000mW, 10,000mW)
    m = re.match(r"^(?P<m>\d[\d,]*)mW$", wa_event)
    if m:
        return True

    # Track distances 600m+ and steeplechase are typically minute-based.
    if wa_event in {"Mile", "2 Miles"}:
        return True
    m = re.match(r"^(?P<m>\d[\d,]*)m(?:\s+SC)?$", wa_event)
    if m:
        try:
            meters = int(m.group("m").replace(",", ""))
        except ValueError:
            meters = 0
        return meters >= 600

    return False


def performance_to_value(clean_value: str) -> Optional[float]:
    """Normalise a performance to a sortable float.

    - Times: seconds (supports hh:mm:ss, mm:ss, ss.xx)
    - Distances/points: float
    """
    text = (clean_value or "").strip()
    if not text:
        return None
    text = text.replace(",", ".")
    parts = text.split(":")
    try:
        seconds = 0.0
        for part in parts:
            seconds = seconds * 60 + float(part)
        return seconds
    except ValueError:
        return None


def format_value_no(value: float, *, orientation: str, decimals: int = 2) -> str:
    if orientation == "lower":
        return format_time_no(value, precision=decimals)
    return format_decimal_no(value, decimals=decimals)


def format_decimal_no(value: float, *, decimals: int = 2) -> str:
    decimals = max(0, int(decimals))
    if decimals == 0:
        return str(int(round(float(value))))
    text = f"{float(value):.{decimals}f}"
    return text.replace(".", ",")


def format_time_no(seconds: float, *, precision: int = 2) -> str:
    """Format time (seconds) in Norwegian-like style.

    - < 60s: ss,cc
    - >= 60s: m,ss,cc
    - >= 3600s: h,mm,ss,cc
    """
    precision = max(0, int(precision))
    seconds = max(0.0, float(seconds))

    scale = 10**precision
    total = int(round(seconds * scale))

    total_seconds = total // scale
    frac = total % scale

    hours = total_seconds // 3600
    rem = total_seconds % 3600
    minutes = rem // 60
    sec = rem % 60

    if precision > 0:
        frac_s = str(frac).zfill(precision)
        if hours > 0:
            return f"{hours},{minutes:02d},{sec:02d},{frac_s}"
        if minutes > 0:
            return f"{minutes},{sec:02d},{frac_s}"
        return f"{sec},{frac_s}"

    if hours > 0:
        return f"{hours},{minutes:02d},{sec:02d}"
    if minutes > 0:
        return f"{minutes},{sec:02d}"
    return str(sec)
