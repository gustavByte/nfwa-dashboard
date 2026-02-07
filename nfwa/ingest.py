from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from . import db as results_db
from .config import SOURCES, Source
from .event_mapping import infer_orientation, map_event_to_wa
from .friidrett_legacy import fetch_page as fetch_friidrett_page
from .friidrett_legacy import pages_for_years as friidrett_pages_for_years
from .friidrett_legacy import parse_page as parse_friidrett_page
from .kondis import fetch_kondis_stats, pages_for_years, parse_kondis_stats
from .minfriidrett import build_landsstatistikk_url, fetch_landsstatistikk, parse_landsstatistikk
from .old_data import parse_old_data_dir
from .util import normalize_performance, performance_to_value
from .wa import ensure_wa_poeng_importable, wa_event_meta, wa_event_names


_JUMP_CM_RE = re.compile(r"^\d{3,4}$")
_MIXED_DOT_COMMA_RE = re.compile(r"^\d+\.\d{1,2},\d{1,2}$")


def _display_raw_performance(
    *, performance_raw: str, wa_event: str | None, performance_norm: str | None = None
) -> str:
    raw = (performance_raw or "").strip()
    if not raw:
        return performance_raw

    # Keep display format aligned with long-event interpretation when source mixes
    # dot+comma separators (e.g. "3.12,43" -> "3.12.43").
    norm = (performance_norm or "").strip()
    if norm and _MIXED_DOT_COMMA_RE.fullmatch(raw) and norm.count(":") >= 2 and "." not in norm:
        return norm.replace(":", ".")

    if wa_event not in {"HJ", "PV"}:
        return performance_raw
    if any(sep in raw for sep in (".", ",", ":")):
        return performance_raw
    if not _JUMP_CM_RE.fullmatch(raw):
        return performance_raw
    try:
        cm = int(raw)
    except ValueError:
        return performance_raw

    if wa_event == "HJ" and not (100 <= cm <= 280):
        return performance_raw
    if wa_event == "PV" and not (100 <= cm <= 700):
        return performance_raw

    return f"{cm / 100:.2f}".replace(".", ",")


@dataclass(frozen=True)
class SyncSummary:
    pages: int
    rows_seen: int
    rows_inserted: int
    wa_points_ok: int
    wa_points_failed: int
    wa_points_missing: int


def sync_landsoversikt(
    *,
    db_path: Path,
    wa_db_path: Path,
    wa_poeng_root: Path,
    cache_dir: Path,
    years: Iterable[int],
    sources: Iterable[Source] = SOURCES,
    refresh: bool = False,
    polite_delay_s: float = 0.5,
) -> SyncSummary:
    if not wa_db_path.exists():
        raise FileNotFoundError(f"Fant ikke WA scoring-db: {wa_db_path}")

    con = results_db.connect(db_path)
    try:
        results_db.init_db(con)

        ensure_wa_poeng_importable(wa_poeng_root=wa_poeng_root)
        from wa_poeng import ScoreCalculator  # type: ignore

        wa_events_by_gender = {src.gender: wa_event_names(wa_db_path=wa_db_path, gender=src.gender) for src in sources}

        pages = 0
        rows_seen = 0
        rows_inserted = 0
        wa_ok = 0
        wa_failed = 0
        wa_missing = 0

        with ScoreCalculator(wa_db_path) as calc:
            for year in years:
                for src in sources:
                    pages_to_fetch = friidrett_pages_for_years(years=[int(year)], gender=src.gender)
                    if pages_to_fetch:
                        wa_events = wa_events_by_gender.get(src.gender, set())
                        for page in pages_to_fetch:
                            try:
                                html_bytes = fetch_friidrett_page(url=page.url, cache_dir=cache_dir, refresh=refresh)
                                parsed_rows = parse_friidrett_page(
                                    html_bytes=html_bytes,
                                    season=int(year),
                                    gender=src.gender,
                                    source_url=page.url,
                                )
                            except Exception as exc:  # noqa: BLE001 - robust fallback for inconsistent legacy pages
                                print(f"Advarsel: hoppet over legacy-side {page.url} ({src.gender} {year}): {type(exc).__name__}: {exc}")
                                continue
                            if not parsed_rows:
                                continue

                            # Rebuild page deterministically: parser tweaks can change keys (e.g. dedup strategy).
                            con.execute(
                                "DELETE FROM results WHERE source_url = ? AND gender = ? AND season = ?",
                                (page.url, src.gender, int(year)),
                            )
                            pages += 1

                            for row in parsed_rows:
                                rows_seen += 1

                                wa_event = map_event_to_wa(event_no=row.event_no, gender=row.gender, wa_events=wa_events)
                                meta = wa_event_meta(wa_db_path=wa_db_path, gender=row.gender, event=wa_event) if wa_event else None
                                orientation = meta.orientation if meta else infer_orientation(row.event_no)

                                results_db.upsert_athlete(
                                    con=con,
                                    athlete_id=row.athlete_id,
                                    gender=row.gender,
                                    name=row.athlete_name,
                                    birth_date=row.birth_date,
                                )
                                club_id = results_db.get_or_create_club(con=con, club_name=row.club_name)
                                event_id = results_db.get_or_create_event(
                                    con=con,
                                    gender=row.gender,
                                    name_no=row.event_no,
                                    wa_event=wa_event,
                                    orientation=orientation,
                                )

                                perf_norm = normalize_performance(
                                    performance=row.performance_clean or "",
                                    orientation=orientation,
                                    wa_event=wa_event,
                                )
                                value = performance_to_value(perf_norm)

                                wa_points: Optional[int] = None
                                wa_exact: Optional[int] = None
                                wa_error: Optional[str] = None

                                if wa_event and perf_norm:
                                    try:
                                        res = calc.points_for_performance(row.gender, wa_event, perf_norm)
                                        wa_points = int(res["points"])
                                        wa_exact = 1 if bool(res["exact"]) else 0
                                        wa_ok += 1
                                    except Exception as exc:  # noqa: BLE001 - loggable error detail
                                        wa_failed += 1
                                        wa_error = f"{type(exc).__name__}: {exc}"
                                else:
                                    wa_missing += 1

                                results_db.upsert_result(
                                    con=con,
                                    season=row.season,
                                    gender=row.gender,
                                    event_id=event_id,
                                    athlete_id=row.athlete_id,
                                    club_id=club_id,
                                    rank_in_list=row.rank_in_list,
                                    performance_raw=_display_raw_performance(
                                        performance_raw=row.performance_raw,
                                        wa_event=wa_event,
                                        performance_norm=perf_norm,
                                    ),
                                    performance_clean=perf_norm or None,
                                    value=value,
                                    wind=row.wind,
                                    placement_raw=row.placement_raw,
                                    competition_id=None,
                                    competition_name=row.competition_name,
                                    venue_city=row.venue_city,
                                    stadium=None,
                                    result_date=row.result_date,
                                    wa_points=wa_points,
                                    wa_exact=wa_exact,
                                    wa_event=wa_event,
                                    wa_error=wa_error,
                                    source_url=row.source_url,
                                )
                                rows_inserted += 1

                            con.commit()
                            time.sleep(max(0.0, polite_delay_s))
                        continue

                    url = build_landsstatistikk_url(showclass=src.showclass, season=year)
                    html_bytes = fetch_landsstatistikk(url=url, cache_dir=cache_dir, refresh=refresh)
                    pages += 1

                    wa_events = wa_events_by_gender.get(src.gender, set())
                    parsed_rows = list(parse_landsstatistikk(html_bytes=html_bytes, season=year, gender=src.gender, source_url=url))
                    if not parsed_rows:
                        continue

                    # Rebuild page deterministically: parser tweaks can change keys (e.g. performance normalisation).
                    con.execute(
                        "DELETE FROM results WHERE source_url = ? AND gender = ? AND season = ?",
                        (url, src.gender, int(year)),
                    )

                    for row in parsed_rows:
                        rows_seen += 1

                        wa_event = map_event_to_wa(event_no=row.event_no, gender=row.gender, wa_events=wa_events)
                        meta = wa_event_meta(wa_db_path=wa_db_path, gender=row.gender, event=wa_event) if wa_event else None
                        orientation = meta.orientation if meta else infer_orientation(row.event_no)

                        results_db.upsert_athlete(
                            con=con,
                            athlete_id=row.athlete_id,
                            gender=row.gender,
                            name=row.athlete_name,
                            birth_date=row.birth_date,
                        )
                        club_id = results_db.get_or_create_club(con=con, club_name=row.club_name)
                        event_id = results_db.get_or_create_event(
                            con=con,
                            gender=row.gender,
                            name_no=row.event_no,
                            wa_event=wa_event,
                            orientation=orientation,
                        )
                        competition_id = results_db.upsert_competition(
                            con=con,
                            competition_id=row.competition_id,
                            name=row.competition_name,
                            city=row.venue_city,
                            stadium=row.stadium,
                        )

                        perf_norm = normalize_performance(
                            performance=row.performance_clean or "",
                            orientation=orientation,
                            wa_event=wa_event,
                        )
                        value = performance_to_value(perf_norm)

                        wa_points: Optional[int] = None
                        wa_exact: Optional[int] = None
                        wa_error: Optional[str] = None

                        if wa_event and perf_norm:
                            try:
                                res = calc.points_for_performance(row.gender, wa_event, perf_norm)
                                wa_points = int(res["points"])
                                wa_exact = 1 if bool(res["exact"]) else 0
                                wa_ok += 1
                            except Exception as exc:  # noqa: BLE001 - loggable error detail
                                wa_failed += 1
                                wa_error = f"{type(exc).__name__}: {exc}"
                        else:
                            wa_missing += 1

                        results_db.upsert_result(
                            con=con,
                            season=row.season,
                            gender=row.gender,
                            event_id=event_id,
                            athlete_id=row.athlete_id,
                            club_id=club_id,
                            rank_in_list=row.rank_in_list,
                            performance_raw=_display_raw_performance(
                                performance_raw=row.performance_raw,
                                wa_event=wa_event,
                                performance_norm=perf_norm,
                            ),
                            performance_clean=perf_norm or None,
                            value=value,
                            wind=row.wind,
                            placement_raw=row.placement_raw,
                            competition_id=competition_id,
                            competition_name=row.competition_name,
                            venue_city=row.venue_city,
                            stadium=row.stadium,
                            result_date=row.result_date,
                            wa_points=wa_points,
                            wa_exact=wa_exact,
                            wa_event=wa_event,
                            wa_error=wa_error,
                            source_url=row.source_url,
                        )
                        rows_inserted += 1

                    con.commit()
                    time.sleep(max(0.0, polite_delay_s))

        return SyncSummary(
            pages=pages,
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            wa_points_ok=wa_ok,
            wa_points_failed=wa_failed,
            wa_points_missing=wa_missing,
        )
    finally:
        con.close()


def sync_kondis(
    *,
    db_path: Path,
    wa_db_path: Path,
    wa_poeng_root: Path,
    cache_dir: Path,
    years: Iterable[int],
    gender: str = "Both",
    refresh: bool = False,
    polite_delay_s: float = 0.5,
) -> SyncSummary:
    if not wa_db_path.exists():
        raise FileNotFoundError(f"Fant ikke WA scoring-db: {wa_db_path}")

    pages_to_fetch = pages_for_years(years=years, gender=gender)
    if not pages_to_fetch:
        return SyncSummary(pages=0, rows_seen=0, rows_inserted=0, wa_points_ok=0, wa_points_failed=0, wa_points_missing=0)

    con = results_db.connect(db_path)
    try:
        results_db.init_db(con)

        ensure_wa_poeng_importable(wa_poeng_root=wa_poeng_root)
        from wa_poeng import ScoreCalculator  # type: ignore

        genders = {p.gender for p in pages_to_fetch}
        wa_events_by_gender = {g: wa_event_names(wa_db_path=wa_db_path, gender=g) for g in genders}

        pages = 0
        rows_seen = 0
        rows_inserted = 0
        wa_ok = 0
        wa_failed = 0
        wa_missing = 0

        with ScoreCalculator(wa_db_path) as calc:
            for page in pages_to_fetch:
                # Some historical pages are known-bad/missing. Keep them in the list so sync can purge any previously
                # ingested rows, but skip fetching/ingesting.
                if not getattr(page, "enabled", True):
                    con.execute("DELETE FROM results WHERE source_url = ?", (page.url,))
                    con.commit()
                    continue

                html_bytes = fetch_kondis_stats(url=page.url, cache_dir=cache_dir, refresh=refresh)
                parsed_rows = list(parse_kondis_stats(html_bytes=html_bytes, page=page))
                if not parsed_rows:
                    continue

                # Rebuild page data deterministically: parser tweaks can change keys (e.g. ranks), so delete old rows.
                con.execute("DELETE FROM results WHERE source_url = ?", (page.url,))
                pages += 1

                wa_events = wa_events_by_gender.get(page.gender, set())
                for row in parsed_rows:
                    rows_seen += 1

                    wa_event = map_event_to_wa(event_no=row.event_no, gender=row.gender, wa_events=wa_events)
                    meta = wa_event_meta(wa_db_path=wa_db_path, gender=row.gender, event=wa_event) if wa_event else None
                    orientation = meta.orientation if meta else infer_orientation(row.event_no)

                    results_db.upsert_athlete(
                        con=con,
                        athlete_id=row.athlete_id,
                        gender=row.gender,
                        name=row.athlete_name,
                        birth_date=row.birth_date,
                    )
                    club_id = results_db.get_or_create_club(con=con, club_name=row.club_name)
                    event_id = results_db.get_or_create_event(
                        con=con,
                        gender=row.gender,
                        name_no=row.event_no,
                        wa_event=wa_event,
                        orientation=orientation,
                    )

                    wa_event_hint = wa_event
                    if wa_event_hint is None and row.event_no.lower().startswith("halvmaraton"):
                        wa_event_hint = "HM"

                    perf_norm = normalize_performance(
                        performance=row.performance_clean or "",
                        orientation=orientation,
                        wa_event=wa_event_hint,
                    )
                    value = performance_to_value(perf_norm)

                    wa_points: Optional[int] = None
                    wa_exact: Optional[int] = None
                    wa_error: Optional[str] = None

                    if wa_event and perf_norm:
                        try:
                            res = calc.points_for_performance(row.gender, wa_event, perf_norm)
                            wa_points = int(res["points"])
                            wa_exact = 1 if bool(res["exact"]) else 0
                            wa_ok += 1
                        except Exception as exc:  # noqa: BLE001 - loggable error detail
                            wa_failed += 1
                            wa_error = f"{type(exc).__name__}: {exc}"
                    else:
                        wa_missing += 1

                    results_db.upsert_result(
                        con=con,
                        season=row.season,
                        gender=row.gender,
                        event_id=event_id,
                        athlete_id=row.athlete_id,
                        club_id=club_id,
                        rank_in_list=row.rank_in_list,
                        performance_raw=_display_raw_performance(
                            performance_raw=row.performance_raw,
                            wa_event=wa_event,
                            performance_norm=perf_norm,
                        ),
                        performance_clean=perf_norm or None,
                        value=value,
                        wind=row.wind,
                        placement_raw=row.placement_raw,
                        competition_id=None,
                        competition_name=row.competition_name,
                        venue_city=row.venue_city,
                        stadium=None,
                        result_date=row.result_date,
                        wa_points=wa_points,
                        wa_exact=wa_exact,
                        wa_event=wa_event,
                        wa_error=wa_error,
                        source_url=row.source_url,
                    )
                    rows_inserted += 1

                con.commit()
                time.sleep(max(0.0, polite_delay_s))

        return SyncSummary(
            pages=pages,
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            wa_points_ok=wa_ok,
            wa_points_failed=wa_failed,
            wa_points_missing=wa_missing,
        )
    finally:
        con.close()


def sync_old_data(
    *,
    db_path: Path,
    wa_db_path: Path,
    wa_poeng_root: Path,
    data_dir: Path,
    years: Iterable[int],
) -> SyncSummary:
    """Sync pre-2000 hand-transcribed data files into the database."""
    if not wa_db_path.exists():
        raise FileNotFoundError(f"Fant ikke WA scoring-db: {wa_db_path}")

    con = results_db.connect(db_path)
    try:
        results_db.init_db(con)

        ensure_wa_poeng_importable(wa_poeng_root=wa_poeng_root)
        from wa_poeng import ScoreCalculator  # type: ignore

        wa_events_by_gender = {
            g: wa_event_names(wa_db_path=wa_db_path, gender=g)
            for g in ("Men", "Women")
        }

        pages = 0
        rows_seen = 0
        rows_inserted = 0
        wa_ok = 0
        wa_failed = 0
        wa_missing = 0

        with ScoreCalculator(wa_db_path) as calc:
            for year in years:
                parsed_rows = parse_old_data_dir(data_dir=data_dir, season=int(year))
                if not parsed_rows:
                    continue

                # Delete old rows from this source to allow idempotent re-import
                source_prefix = f"file://old_data/{year}/"
                con.execute(
                    "DELETE FROM results WHERE source_url LIKE ?",
                    (source_prefix + "%",),
                )
                pages += 1

                for row in parsed_rows:
                    rows_seen += 1

                    wa_events = wa_events_by_gender.get(row.gender, set())
                    wa_event = map_event_to_wa(event_no=row.event_no, gender=row.gender, wa_events=wa_events)
                    meta = wa_event_meta(wa_db_path=wa_db_path, gender=row.gender, event=wa_event) if wa_event else None
                    orientation = meta.orientation if meta else infer_orientation(row.event_no)

                    results_db.upsert_athlete(
                        con=con,
                        athlete_id=row.athlete_id,
                        gender=row.gender,
                        name=row.athlete_name,
                        birth_date=row.birth_date,
                    )
                    club_id = results_db.get_or_create_club(con=con, club_name=row.club_name)
                    event_id = results_db.get_or_create_event(
                        con=con,
                        gender=row.gender,
                        name_no=row.event_no,
                        wa_event=wa_event,
                        orientation=orientation,
                    )

                    perf_norm = normalize_performance(
                        performance=row.performance_clean or "",
                        orientation=orientation,
                        wa_event=wa_event,
                    )
                    value = performance_to_value(perf_norm)

                    wa_points: Optional[int] = None
                    wa_exact: Optional[int] = None
                    wa_error: Optional[str] = None

                    if wa_event and perf_norm:
                        try:
                            res = calc.points_for_performance(row.gender, wa_event, perf_norm)
                            wa_points = int(res["points"])
                            wa_exact = 1 if bool(res["exact"]) else 0
                            wa_ok += 1
                        except Exception as exc:  # noqa: BLE001 - loggable error detail
                            wa_failed += 1
                            wa_error = f"{type(exc).__name__}: {exc}"
                    else:
                        wa_missing += 1

                    results_db.upsert_result(
                        con=con,
                        season=row.season,
                        gender=row.gender,
                        event_id=event_id,
                        athlete_id=row.athlete_id,
                        club_id=club_id,
                        rank_in_list=row.rank_in_list,
                        performance_raw=_display_raw_performance(
                            performance_raw=row.performance_raw,
                            wa_event=wa_event,
                            performance_norm=perf_norm,
                        ),
                        performance_clean=perf_norm or None,
                        value=value,
                        wind=row.wind,
                        placement_raw=row.placement_raw,
                        competition_id=None,
                        competition_name=row.competition_name,
                        venue_city=row.venue_city,
                        stadium=None,
                        result_date=row.result_date,
                        wa_points=wa_points,
                        wa_exact=wa_exact,
                        wa_event=wa_event,
                        wa_error=wa_error,
                        source_url=row.source_url,
                    )
                    rows_inserted += 1

                con.commit()

        return SyncSummary(
            pages=pages,
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            wa_points_ok=wa_ok,
            wa_points_failed=wa_failed,
            wa_points_missing=wa_missing,
        )
    finally:
        con.close()
