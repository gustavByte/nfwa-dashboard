from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS athletes (
    id INTEGER PRIMARY KEY,
    gender TEXT NOT NULL CHECK(gender IN ('Men','Women')),
    name TEXT NOT NULL,
    birth_date TEXT,
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


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    # Drop known junk sections (non-event tables) if they were ever ingested.
    con.executescript(
        """
        DELETE FROM results WHERE event_id IN (SELECT id FROM events WHERE TRIM(name_no) = '');
        DELETE FROM events WHERE TRIM(name_no) = '';
        """
    )
    # Ensure stable upserts even when some columns are NULL (SQLite UNIQUE treats NULLs as distinct).
    con.executescript(NATURAL_DEDUP_SQL)
    con.executescript(NATURAL_UNIQUE_INDEX_SQL)
    con.commit()


def upsert_athlete(*, con: sqlite3.Connection, athlete_id: int, gender: str, name: str, birth_date: str | None) -> None:
    con.execute(
        """
        INSERT INTO athletes (id, gender, name, birth_date)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            gender=excluded.gender,
            name=excluded.name,
            birth_date=COALESCE(excluded.birth_date, athletes.birth_date),
            updated_at=CURRENT_TIMESTAMP
        """,
        (athlete_id, gender, name, birth_date),
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
) -> None:
    con.execute(
        """
        INSERT INTO results (
            season, gender, event_id, athlete_id, club_id, rank_in_list,
            performance_raw, performance_clean, value, wind, placement_raw,
            competition_id, competition_name, venue_city, stadium, result_date,
            wa_points, wa_exact, wa_event, wa_error, source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        ),
    )
