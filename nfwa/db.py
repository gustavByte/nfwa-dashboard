from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

SCHEMA_VERSION = 2

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

CREATE TABLE IF NOT EXISTS athletes (
    id INTEGER PRIMARY KEY,
    gender TEXT NOT NULL CHECK(gender IN ('Men','Women')),
    name TEXT NOT NULL,
    birth_date TEXT,
    nationality TEXT NOT NULL DEFAULT 'NOR',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clubs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    gender TEXT NOT NULL CHECK(gender IN ('Men','Women')),
    name_no TEXT NOT NULL,
    wa_event TEXT,
    orientation TEXT NOT NULL CHECK(orientation IN ('lower','higher')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(gender, name_no)
);

CREATE TABLE IF NOT EXISTS competitions (
    id INTEGER PRIMARY KEY,
    name TEXT,
    city TEXT,
    stadium TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,
    gender TEXT NOT NULL CHECK(gender IN ('Men','Women')),
    event_id INTEGER NOT NULL REFERENCES events(id),
    athlete_id INTEGER NOT NULL REFERENCES athletes(id),
    club_id INTEGER REFERENCES clubs(id),
    rank_in_list INTEGER,
    performance_raw TEXT NOT NULL,
    performance_clean TEXT,
    value REAL,
    wind REAL,
    placement_raw TEXT,
    competition_id INTEGER REFERENCES competitions(id),
    competition_name TEXT,
    venue_city TEXT,
    stadium TEXT,
    result_date TEXT,
    wa_points INTEGER,
    wa_exact INTEGER CHECK(wa_exact IN (0, 1)),
    wa_event TEXT,
    wa_error TEXT,
    source_url TEXT NOT NULL,
    source_type TEXT,
    scraped_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(
        season,
        gender,
        event_id,
        athlete_id,
        result_date,
        performance_raw,
        competition_id,
        placement_raw
    )
);

CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id, season);
CREATE INDEX IF NOT EXISTS idx_results_event ON results(event_id, season, gender);
CREATE INDEX IF NOT EXISTS idx_results_points ON results(season, gender, event_id, wa_points);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    source_type TEXT NOT NULL,
    url TEXT,
    internal_ref TEXT,
    description TEXT,
    season INTEGER,
    gender TEXT,
    last_synced_at TEXT,
    row_count INTEGER,
    UNIQUE(source_type, url)
);

CREATE TABLE IF NOT EXISTS change_log (
    id INTEGER PRIMARY KEY,
    change_type TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id INTEGER,
    field_name TEXT,
    old_value TEXT,
    new_value TEXT,
    reason TEXT,
    changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_changelog_record ON change_log(table_name, record_id);

CREATE TABLE IF NOT EXISTS athlete_aliases (
    id INTEGER PRIMARY KEY,
    canonical_id INTEGER NOT NULL REFERENCES athletes(id),
    alias_id INTEGER NOT NULL REFERENCES athletes(id),
    confidence TEXT NOT NULL CHECK(confidence IN ('confirmed','suggested')),
    reason TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(canonical_id, alias_id)
);
"""

NATURAL_DEDUP_SQL = """
DELETE FROM results
WHERE id NOT IN (
    SELECT MAX(id) FROM results
    GROUP BY
        season,
        gender,
        event_id,
        athlete_id,
        IFNULL(result_date, ''),
        performance_raw,
        IFNULL(competition_id, -1),
        IFNULL(placement_raw, '')
);
"""

NATURAL_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uix_results_natural
ON results(
    season,
    gender,
    event_id,
    athlete_id,
    IFNULL(result_date, ''),
    performance_raw,
    IFNULL(competition_id, -1),
    IFNULL(placement_raw, '')
);
"""


@dataclass(frozen=True)
class UpsertedIds:
    athlete_id: int
    club_id: Optional[int]
    event_id: int
    competition_id: Optional[int]


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_to_v2(con: sqlite3.Connection) -> None:
    """Add columns/tables introduced in schema v2."""
    if "nationality" not in _table_columns(con, "athletes"):
        con.execute("ALTER TABLE athletes ADD COLUMN nationality TEXT NOT NULL DEFAULT 'NOR'")
    if "source_type" not in _table_columns(con, "results"):
        con.execute("ALTER TABLE results ADD COLUMN source_type TEXT")


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    # Migrate existing tables that CREATE TABLE IF NOT EXISTS won't touch.
    _migrate_to_v2(con)
    # Drop known junk sections (non-event tables) if they were ever ingested.
    con.executescript(
        """
        DELETE FROM results WHERE event_id IN (SELECT id FROM events WHERE TRIM(name_no) = '');
        DELETE FROM events WHERE TRIM(name_no) = '';
        """
    )
    # Purge known placeholder athletes (seen in some legacy pages, e.g. "–––").
    con.executescript(
        """
        DELETE FROM results
        WHERE athlete_id IN (SELECT id FROM athletes WHERE TRIM(name) = '' OR name = '–––');
        DELETE FROM athletes
        WHERE TRIM(name) = '' OR name = '–––';
        """
    )
    # Ensure stable upserts even when some columns are NULL (SQLite UNIQUE treats NULLs as distinct).
    con.executescript(NATURAL_DEDUP_SQL)
    con.executescript(NATURAL_UNIQUE_INDEX_SQL)
    # Record schema version if not already recorded.
    row = con.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
    if row["v"] is None or row["v"] < SCHEMA_VERSION:
        con.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (SCHEMA_VERSION, "Add nationality, source_type, sources, change_log, athlete_aliases"),
        )
    con.commit()


def upsert_athlete(
    *,
    con: sqlite3.Connection,
    athlete_id: int,
    gender: str,
    name: str,
    birth_date: str | None,
    nationality: str = "NOR",
) -> None:
    norm_name = " ".join((name or "").split())
    con.execute(
        """
        INSERT INTO athletes (id, gender, name, birth_date, nationality)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            gender=excluded.gender,
            name=CASE
                WHEN TRIM(athletes.name) = '' THEN excluded.name
                WHEN TRIM(excluded.name) = '' THEN athletes.name
                WHEN LENGTH(TRIM(excluded.name)) > LENGTH(TRIM(athletes.name)) THEN excluded.name
                ELSE athletes.name
            END,
            birth_date=COALESCE(excluded.birth_date, athletes.birth_date),
            nationality=CASE
                WHEN athletes.nationality != 'NOR' THEN athletes.nationality
                ELSE excluded.nationality
            END,
            updated_at=CURRENT_TIMESTAMP
        """,
        (athlete_id, gender, norm_name, birth_date, nationality),
    )


def get_or_create_club(*, con: sqlite3.Connection, club_name: str | None) -> Optional[int]:
    name = (club_name or "").strip()
    if not name:
        return None
    con.execute(
        """
        INSERT INTO clubs (name) VALUES (?)
        ON CONFLICT(name) DO UPDATE SET
            updated_at=CURRENT_TIMESTAMP
        """,
        (name,),
    )
    row = con.execute("SELECT id FROM clubs WHERE name = ?", (name,)).fetchone()
    return int(row["id"]) if row else None


def get_or_create_event(
    *,
    con: sqlite3.Connection,
    gender: str,
    name_no: str,
    wa_event: str | None,
    orientation: str,
) -> int:
    con.execute(
        """
        INSERT INTO events (gender, name_no, wa_event, orientation)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(gender, name_no) DO UPDATE SET
            wa_event=COALESCE(excluded.wa_event, events.wa_event),
            orientation=excluded.orientation,
            updated_at=CURRENT_TIMESTAMP
        """,
        (gender, name_no, wa_event, orientation),
    )
    row = con.execute(
        "SELECT id FROM events WHERE gender = ? AND name_no = ?",
        (gender, name_no),
    ).fetchone()
    if not row:
        raise RuntimeError("Failed to create event")
    return int(row["id"])


def upsert_competition(
    *,
    con: sqlite3.Connection,
    competition_id: int | None,
    name: str | None,
    city: str | None,
    stadium: str | None,
) -> Optional[int]:
    if competition_id is None:
        return None
    con.execute(
        """
        INSERT INTO competitions (id, name, city, stadium)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=COALESCE(excluded.name, competitions.name),
            city=COALESCE(excluded.city, competitions.city),
            stadium=COALESCE(excluded.stadium, competitions.stadium),
            updated_at=CURRENT_TIMESTAMP
        """,
        (competition_id, name, city, stadium),
    )
    return int(competition_id)


def upsert_result(
    *,
    con: sqlite3.Connection,
    season: int,
    gender: str,
    event_id: int,
    athlete_id: int,
    club_id: int | None,
    rank_in_list: int | None,
    performance_raw: str,
    performance_clean: str | None,
    value: float | None,
    wind: float | None,
    placement_raw: str | None,
    competition_id: int | None,
    competition_name: str | None,
    venue_city: str | None,
    stadium: str | None,
    result_date: str | None,
    wa_points: int | None,
    wa_exact: int | None,
    wa_event: str | None,
    wa_error: str | None,
    source_url: str,
    source_type: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO results (
            season, gender, event_id, athlete_id, club_id, rank_in_list,
            performance_raw, performance_clean, value, wind, placement_raw,
            competition_id, competition_name, venue_city, stadium, result_date,
            wa_points, wa_exact, wa_event, wa_error, source_url, source_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO UPDATE SET
            club_id=excluded.club_id,
            rank_in_list=excluded.rank_in_list,
            performance_clean=excluded.performance_clean,
            value=excluded.value,
            wind=excluded.wind,
            competition_name=COALESCE(excluded.competition_name, results.competition_name),
            venue_city=COALESCE(excluded.venue_city, results.venue_city),
            stadium=COALESCE(excluded.stadium, results.stadium),
            wa_points=excluded.wa_points,
            wa_exact=excluded.wa_exact,
            wa_event=excluded.wa_event,
            wa_error=excluded.wa_error,
            source_type=COALESCE(excluded.source_type, results.source_type),
            scraped_at=CURRENT_TIMESTAMP
        """,
        (
            season,
            gender,
            event_id,
            athlete_id,
            club_id,
            rank_in_list,
            performance_raw,
            performance_clean,
            value,
            wind,
            placement_raw,
            competition_id,
            competition_name,
            venue_city,
            stadium,
            result_date,
            wa_points,
            wa_exact,
            wa_event,
            wa_error,
            source_url,
            source_type,
        ),
    )


def upsert_source(
    *,
    con: sqlite3.Connection,
    source_type: str,
    url: str | None,
    internal_ref: str | None = None,
    description: str | None = None,
    season: int | None = None,
    gender: str | None = None,
    row_count: int | None = None,
) -> None:
    """Register or update a data source in the sources catalog."""
    con.execute(
        """
        INSERT INTO sources (source_type, url, internal_ref, description, season, gender, last_synced_at, row_count)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(source_type, url) DO UPDATE SET
            internal_ref=COALESCE(excluded.internal_ref, sources.internal_ref),
            description=COALESCE(excluded.description, sources.description),
            season=COALESCE(excluded.season, sources.season),
            gender=COALESCE(excluded.gender, sources.gender),
            last_synced_at=CURRENT_TIMESTAMP,
            row_count=excluded.row_count
        """,
        (source_type, url, internal_ref, description, season, gender, row_count),
    )


def log_change(
    *,
    con: sqlite3.Connection,
    change_type: str,
    table_name: str,
    record_id: int | None = None,
    field_name: str | None = None,
    old_value: str | None = None,
    new_value: str | None = None,
    reason: str | None = None,
) -> None:
    """Record a manual change to the change_log for audit trail."""
    con.execute(
        """
        INSERT INTO change_log (change_type, table_name, record_id, field_name, old_value, new_value, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (change_type, table_name, record_id, field_name, old_value, new_value, reason),
    )


def fill_club_gaps(con: sqlite3.Connection, season: int | None = None) -> int:
    """Fill missing club_id in results using other results for the same athlete/season.

    Returns the number of rows updated.
    """
    where = "WHERE r.club_id IS NULL"
    params: tuple = ()
    if season is not None:
        where += " AND r.season = ?"
        params = (season,)
    cur = con.execute(
        f"""
        UPDATE results SET club_id = (
            SELECT r2.club_id FROM results r2
            WHERE r2.athlete_id = results.athlete_id
              AND r2.season = results.season
              AND r2.club_id IS NOT NULL
            ORDER BY r2.scraped_at DESC
            LIMIT 1
        )
        WHERE results.id IN (
            SELECT r.id FROM results r
            {where}
            AND EXISTS (
                SELECT 1 FROM results r2
                WHERE r2.athlete_id = r.athlete_id
                  AND r2.season = r.season
                  AND r2.club_id IS NOT NULL
            )
        )
        """,
        params,
    )
    return cur.rowcount
