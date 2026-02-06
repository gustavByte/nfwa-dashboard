from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

from .config import SOURCES, Source
from .export_site import export_site
from .ingest import SyncSummary, sync_kondis, sync_landsoversikt
from .queries import DEFAULT_TOP_NS, available_seasons


@dataclass(frozen=True)
class BuildSiteSummary:
    min_year: int
    max_year: int
    years_refreshed: list[int]
    years_filled: list[int]
    landsoversikt: SyncSummary
    kondis: Optional[SyncSummary]
    out_dir: Path


def build_site(
    *,
    db_path: Path,
    wa_db_path: Path,
    wa_poeng_root: Path,
    cache_dir: Path,
    kondis_cache_dir: Path,
    min_year: int = 2002,
    max_year: Optional[int] = None,
    gender: str = "Both",
    refresh_years: int = 2,
    include_kondis: bool = True,
    out_dir: Path = Path("docs"),
    top_ns: Iterable[int] = DEFAULT_TOP_NS,
    include_athlete_index: bool = True,
    polite_delay_s: float = 0.5,
) -> BuildSiteSummary:
    max_y = int(max_year) if max_year is not None else int(date.today().year)
    min_y = int(min_year)
    if min_y > max_y:
        raise ValueError(f"min_year ({min_y}) kan ikke være større enn max_year ({max_y})")

    years_all = list(range(min_y, max_y + 1))

    # Prefer quick incremental updates: refresh only recent seasons, but backfill any missing seasons.
    seasons_present = _safe_available_seasons(db_path=db_path)

    # Cold start (no DB yet): do a single pass, avoid re-fetching recent years twice.
    if not seasons_present:
        years_refresh: list[int] = []
    else:
        years_refresh = years_all[-max(0, int(refresh_years)) :] if refresh_years else []
    refresh_set = set(years_refresh)

    missing = [y for y in years_all if y not in seasons_present]
    years_fill = sorted(set(missing) - refresh_set)

    sources = _sources_for_gender(gender)

    lands_fill = (
        sync_landsoversikt(
            db_path=db_path,
            wa_db_path=wa_db_path,
            wa_poeng_root=wa_poeng_root,
            cache_dir=cache_dir,
            years=years_fill,
            sources=sources,
            refresh=False,
            polite_delay_s=polite_delay_s,
        )
        if years_fill
        else SyncSummary(pages=0, rows_seen=0, rows_inserted=0, wa_points_ok=0, wa_points_failed=0, wa_points_missing=0)
    )

    lands_refresh = (
        sync_landsoversikt(
            db_path=db_path,
            wa_db_path=wa_db_path,
            wa_poeng_root=wa_poeng_root,
            cache_dir=cache_dir,
            years=years_refresh,
            sources=sources,
            refresh=True,
            polite_delay_s=polite_delay_s,
        )
        if years_refresh
        else SyncSummary(pages=0, rows_seen=0, rows_inserted=0, wa_points_ok=0, wa_points_failed=0, wa_points_missing=0)
    )

    lands_total = _sum_sync_summaries(lands_fill, lands_refresh)

    kondis_total: Optional[SyncSummary] = None
    if include_kondis:
        kondis_fill = (
            sync_kondis(
                db_path=db_path,
                wa_db_path=wa_db_path,
                wa_poeng_root=wa_poeng_root,
                cache_dir=kondis_cache_dir,
                years=years_fill,
                gender=gender,
                refresh=False,
                polite_delay_s=polite_delay_s,
            )
            if years_fill
            else SyncSummary(pages=0, rows_seen=0, rows_inserted=0, wa_points_ok=0, wa_points_failed=0, wa_points_missing=0)
        )
        kondis_refresh = (
            sync_kondis(
                db_path=db_path,
                wa_db_path=wa_db_path,
                wa_poeng_root=wa_poeng_root,
                cache_dir=kondis_cache_dir,
                years=years_refresh,
                gender=gender,
                refresh=True,
                polite_delay_s=polite_delay_s,
            )
            if years_refresh
            else SyncSummary(pages=0, rows_seen=0, rows_inserted=0, wa_points_ok=0, wa_points_failed=0, wa_points_missing=0)
        )
        kondis_total = _sum_sync_summaries(kondis_fill, kondis_refresh)

    export_site(
        db_path=db_path,
        out_dir=out_dir,
        top_ns=top_ns,
        include_athlete_index=include_athlete_index,
    )

    return BuildSiteSummary(
        min_year=min_y,
        max_year=max_y,
        years_refreshed=years_refresh,
        years_filled=years_fill,
        landsoversikt=lands_total,
        kondis=kondis_total,
        out_dir=out_dir,
    )


def _safe_available_seasons(*, db_path: Path) -> set[int]:
    if not db_path.exists():
        return set()

    con = sqlite3.connect(db_path)
    try:
        con.row_factory = sqlite3.Row
        seasons = available_seasons(con=con)
        return {int(s) for s in seasons}
    except sqlite3.Error:
        return set()
    finally:
        con.close()


def _sources_for_gender(gender: str) -> list[Source]:
    if gender == "Both":
        return list(SOURCES)
    return [s for s in SOURCES if s.gender == gender]


def _sum_sync_summaries(a: SyncSummary, b: SyncSummary) -> SyncSummary:
    return SyncSummary(
        pages=int(a.pages) + int(b.pages),
        rows_seen=int(a.rows_seen) + int(b.rows_seen),
        rows_inserted=int(a.rows_inserted) + int(b.rows_inserted),
        wa_points_ok=int(a.wa_points_ok) + int(b.wa_points_ok),
        wa_points_failed=int(a.wa_points_failed) + int(b.wa_points_failed),
        wa_points_missing=int(a.wa_points_missing) + int(b.wa_points_missing),
    )
