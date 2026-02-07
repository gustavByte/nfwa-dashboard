from __future__ import annotations

import re
from typing import Optional


_DIST_METER_RE = re.compile(r"^(?P<m>\d+)\s+meter$")
_DIST_MILE_RE = re.compile(r"^1\s+mile$", re.IGNORECASE)
_DIST_MILES_RE = re.compile(r"^(?P<miles>\d+)\s+miles?$", re.IGNORECASE)
_HURDLES_RE = re.compile(r"^(?P<m>\d+)\s+meter\s+hekk\b", re.IGNORECASE)
_STEEPLE_RE = re.compile(r"^(?P<m>\d+)\s+meter\s+hinder\b", re.IGNORECASE)

_SHOT_RE = re.compile(r"^Kule\s+(?P<kg>\d+(?:,\d+)?)kg\b", re.IGNORECASE)
_DISCUS_RE = re.compile(r"^Diskos\s+(?P<kg>\d+(?:,\d+)?)kg\b", re.IGNORECASE)
_HAMMER_RE = re.compile(r"^Slegge\s+(?P<kg>\d+(?:,\d+)?)kg\b", re.IGNORECASE)
_JAVELIN_RE = re.compile(r"^Spyd\s+(?P<g>\d+)\s*gram\b", re.IGNORECASE)

_WALK_KM_RE = re.compile(r"^Kappgang\s+(?P<km>\d+)\s*km$", re.IGNORECASE)
_WALK_KM_SHORT_RE = re.compile(r"^Kappgang\s+(?P<km>\d+)\s*km\b", re.IGNORECASE)
_WALK_M_RE = re.compile(r"^Kappgang\s+(?P<m>\d+)\s+meter$", re.IGNORECASE)


def infer_orientation(event_no: str) -> str:
    name = (event_no or "").strip()
    if not name:
        return "higher"

    low = name.lower()
    # Road running / road races
    if "gatel\u00f8p" in low or "landevei" in low:
        return "lower"
    if re.match(r"^\d+\s*km\b", low):
        return "lower"
    if "halvmaraton" in low or "half marathon" in low:
        return "lower"
    if low.startswith(("maraton", "marathon")):
        return "lower"

    if name.startswith("Kappgang "):
        return "lower"
    if " meter" in name.lower() or "mile" in name.lower():
        return "lower"
    if any(
        name.lower().startswith(prefix)
        for prefix in (
            "diskos",
            "kule",
            "slegge",
            "spyd",
            "vektkast",
            "lengde",
            "tresteg",
            "h\u00f8yde",
            "stav",
        )
    ):
        return "higher"
    if "kamp" in name.lower():
        return "higher"
    return "higher"


def map_event_to_wa(*, event_no: str, gender: str, wa_events: set[str]) -> Optional[str]:
    name = (event_no or "").strip()
    if not name:
        return None
    # Hand-timed events should never map to WA scoring
    if "Håndtid" in name:
        return None

    low = name.lower()
    if low.startswith(("maraton", "marathon")):
        return "Marathon" if "Marathon" in wa_events else None
    if "halvmaraton" in low or "half marathon" in low:
        return "HM" if "HM" in wa_events else None

    m = re.match(r"^(?P<km>\d+)\s*km\b", name, re.IGNORECASE)
    if m:
        cand = f"{int(m.group('km'))} km"
        return cand if cand in wa_events else None

    # Track distances
    if _DIST_MILE_RE.match(name):
        return "Mile" if "Mile" in wa_events else None

    m = _DIST_MILES_RE.match(name)
    if m:
        miles = int(m.group("miles"))
        if miles == 1:
            return "Mile" if "Mile" in wa_events else None
        cand = f"{miles} Miles"
        return cand if cand in wa_events else None

    m = _DIST_METER_RE.match(name)
    if m:
        cand = f"{int(m.group('m'))}m"
        return cand if cand in wa_events else None

    # Hurdles
    m = _HURDLES_RE.match(name)
    if m:
        cand = f"{int(m.group('m'))}mH"
        return cand if cand in wa_events else None

    # Steeplechase
    m = _STEEPLE_RE.match(name)
    if m:
        cand = f"{int(m.group('m'))}m SC"
        return cand if cand in wa_events else None

    # Race walk
    m = _WALK_KM_RE.match(name) or _WALK_KM_SHORT_RE.match(name)
    if m:
        km = int(m.group("km"))
        cand = f"{km}km W"
        if cand in wa_events:
            return cand
        meters = km * 1000
        cand2 = _walk_meters_to_wa(meters)
        return cand2 if cand2 in wa_events else None

    m = _WALK_M_RE.match(name)
    if m:
        cand = _walk_meters_to_wa(int(m.group("m")))
        return cand if cand in wa_events else None

    # Field events (standard senior weights only)
    if name.lower() == "lengde" and "LJ" in wa_events:
        return "LJ"
    if name.lower() == "tresteg" and "TJ" in wa_events:
        return "TJ"
    if name.lower() == "h\u00f8yde" and "HJ" in wa_events:
        return "HJ"
    if name.lower() == "stav" and "PV" in wa_events:
        return "PV"

    m = _SHOT_RE.match(name)
    if m and "SP" in wa_events:
        kg = float(m.group("kg").replace(",", "."))
        return "SP" if _is_standard_shot_weight(kg, gender) else None

    m = _DISCUS_RE.match(name)
    if m and "DT" in wa_events:
        kg = float(m.group("kg").replace(",", "."))
        return "DT" if _is_standard_discus_weight(kg, gender) else None

    m = _HAMMER_RE.match(name)
    if m and "HT" in wa_events:
        kg = float(m.group("kg").replace(",", "."))
        return "HT" if _is_standard_hammer_weight(kg, gender) else None

    m = _JAVELIN_RE.match(name)
    if m and "JT" in wa_events:
        grams = int(m.group("g"))
        return "JT" if _is_standard_javelin_weight(grams, gender) else None

    # Combined events
    if name.lower().startswith("7 kamp") and gender == "Women" and "Hept." in wa_events:
        return "Hept."
    if name.lower().startswith("10 kamp") and gender == "Men" and "Dec." in wa_events:
        return "Dec."

    return None


def _walk_meters_to_wa(meters: int) -> str:
    if meters in {3000, 5000}:
        return f"{meters}mW"
    if meters in {10000, 15000, 20000, 30000, 35000, 50000}:
        # Scoring DB uses thousand separators for these.
        return f"{meters:,}mW"
    return f"{meters}mW"


def _is_standard_shot_weight(kg: float, gender: str) -> bool:
    return _approx_equal(kg, 4.0) if gender == "Women" else _approx_equal(kg, 7.26)


def _is_standard_discus_weight(kg: float, gender: str) -> bool:
    return _approx_equal(kg, 1.0) if gender == "Women" else _approx_equal(kg, 2.0)


def _is_standard_hammer_weight(kg: float, gender: str) -> bool:
    return _approx_equal(kg, 4.0) if gender == "Women" else _approx_equal(kg, 7.26)


def _is_standard_javelin_weight(grams: int, gender: str) -> bool:
    return grams == 600 if gender == "Women" else grams == 800


def _approx_equal(a: float, b: float, tol: float = 0.03) -> bool:
    return abs(a - b) <= tol
