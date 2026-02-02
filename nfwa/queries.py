from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from .util import format_value_no


_EVENT_ORDER_PATTERNS: tuple[tuple[int, re.Pattern[str]], ...] = (
    (0, re.compile(r"^100\s+meter$", re.IGNORECASE)),
    (1, re.compile(r"^200\s+meter$", re.IGNORECASE)),
    (2, re.compile(r"^400\s+meter$", re.IGNORECASE)),
    (3, re.compile(r"^800\s+meter$", re.IGNORECASE)),
    (4, re.compile(r"^1500\s+meter$", re.IGNORECASE)),
    (5, re.compile(r"^3000\s+meter$", re.IGNORECASE)),
    (6, re.compile(r"^5000\s+meter$", re.IGNORECASE)),
    (7, re.compile(r"^10000\s+meter$", re.IGNORECASE)),
    (8, re.compile(r"^(?:maraton|marathon)$", re.IGNORECASE)),
    (9, re.compile(r"^(?:halvmaraton|halvmarton)$", re.IGNORECASE)),
    (10, re.compile(r"^10\s*km\s+gatel\u00f8p$", re.IGNORECASE)),
    (11, re.compile(r"^5\s*km\s+gatel\u00f8p$", re.IGNORECASE)),
    (12, re.compile(r"^3000\s+meter\s+hinder\b", re.IGNORECASE)),
    (13, re.compile(r"^(?:110|100)\s+meter\s+hekk\b", re.IGNORECASE)),
    (14, re.compile(r"^400\s+meter\s+hekk\b", re.IGNORECASE)),
    (15, re.compile(r"^h\u00f8yde$", re.IGNORECASE)),
    (16, re.compile(r"^stav$", re.IGNORECASE)),
    (17, re.compile(r"^lengde$", re.IGNORECASE)),
    (18, re.compile(r"^tresteg$", re.IGNORECASE)),
    (19, re.compile(r"^kule\b", re.IGNORECASE)),
    (20, re.compile(r"^diskos\b", re.IGNORECASE)),
    (21, re.compile(r"^slegge\b", re.IGNORECASE)),
    (22, re.compile(r"^spyd\b", re.IGNORECASE)),
    (23, re.compile(r"^(?:10|7)\s+kamp\b", re.IGNORECASE)),
    (24, re.compile(r"^kappgang\s+20\s*km\b", re.IGNORECASE)),
    (25, re.compile(r"^kappgang\s+(?:35|42|50)\s*km\b", re.IGNORECASE)),
    (26, re.compile(r"^4\s*x\s*(?:100|400)\s*meter\b.*\bmix", re.IGNORECASE)),
    (27, re.compile(r"^4\s*x\s*100\s*meter\b.*\bstafett\b", re.IGNORECASE)),
    (28, re.compile(r"^4\s*x\s*400\s*meter\b.*\bstafett\b", re.IGNORECASE)),
)


def event_sort_key(event_no: str) -> tuple[int, str]:
    idx = _event_order_index(event_no)
    name = (event_no or "").strip()
    return (idx if idx is not None else 10_000, name.lower())


def _event_order_index(event_no: str) -> Optional[int]:
    name = (event_no or "").strip()
    if not name:
        return None
    for idx, pat in _EVENT_ORDER_PATTERNS:
        if pat.search(name):
            return int(idx)
    return None


@dataclass(frozen=True)
class EventSummaryRow:
    season: int
    gender: str
    event_no: str
    wa_event: Optional[str]
    orientation: str
    top_n: int
    athletes_total: int
    results_total: int
    points_available: int
    avg_points_top_n: Optional[float]
    avg_value_top_n_perf: Optional[float]
    avg_perf_top_n: Optional[str]


def event_summary(
    *,
    con: sqlite3.Connection,
    season: int,
    gender: str,
    top_ns: Iterable[int] = (5, 10, 20),
) -> list[EventSummaryRow]:
    events = con.execute(
        """
        SELECT e.id, e.name_no, e.wa_event, e.orientation
        FROM events e
        WHERE e.gender = ?
        ORDER BY e.name_no
        """,
        (gender,),
    ).fetchall()

    out: list[EventSummaryRow] = []
    for ev in events:
        event_id = int(ev["id"])
        event_no = str(ev["name_no"])
        wa_event = str(ev["wa_event"]) if ev["wa_event"] is not None else None
        orientation = str(ev["orientation"])

        totals = con.execute(
            """
            SELECT
                COUNT(*) AS results_total,
                COUNT(DISTINCT athlete_id) AS athletes_total,
                SUM(CASE WHEN wa_points IS NOT NULL THEN 1 ELSE 0 END) AS points_available
            FROM results
            WHERE season = ? AND gender = ? AND event_id = ?
            """,
            (season, gender, event_id),
        ).fetchone()
        if not totals or int(totals["results_total"] or 0) == 0:
            continue

        results_total = int(totals["results_total"] or 0)
        athletes_total = int(totals["athletes_total"] or 0)
        points_available = int(totals["points_available"] or 0)

        sort_expr = "CASE WHEN e.orientation = 'lower' THEN r.value ELSE -r.value END"
        best_cte = f"""
            WITH best AS (
                SELECT
                    r.*,
                    e.orientation,
                    {sort_expr} AS sort_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.season, r.gender, r.event_id, r.athlete_id
                        ORDER BY {sort_expr} ASC, r.result_date DESC
                    ) AS rn
                FROM results r
                JOIN events e ON e.id = r.event_id
                WHERE r.season = ? AND r.gender = ? AND r.event_id = ?
            )
        """

        for n in top_ns:
            points_rows = con.execute(
                best_cte
                + """
                SELECT wa_points
                FROM best
                WHERE rn = 1 AND wa_points IS NOT NULL
                ORDER BY wa_points DESC
                LIMIT ?
                """,
                (season, gender, event_id, int(n)),
            ).fetchall()
            avg_points = None
            if points_rows:
                vals = [int(r["wa_points"]) for r in points_rows if r["wa_points"] is not None]
                avg_points = (sum(vals) / len(vals)) if vals else None

            perf_rows = con.execute(
                best_cte
                + """
                SELECT value
                FROM best
                WHERE rn = 1 AND value IS NOT NULL
                ORDER BY sort_value ASC
                LIMIT ?
                """,
                (season, gender, event_id, int(n)),
            ).fetchall()
            avg_value = None
            if perf_rows:
                vals = [float(r["value"]) for r in perf_rows if r["value"] is not None]
                avg_value = (sum(vals) / len(vals)) if vals else None
            avg_perf = format_value_no(avg_value, orientation=orientation) if avg_value is not None else None

            out.append(
                EventSummaryRow(
                    season=season,
                    gender=gender,
                    event_no=event_no,
                    wa_event=wa_event,
                    orientation=orientation,
                    top_n=int(n),
                    athletes_total=athletes_total,
                    results_total=results_total,
                    points_available=points_available,
                    avg_points_top_n=avg_points,
                    avg_value_top_n_perf=avg_value,
                    avg_perf_top_n=avg_perf,
                )
            )

    return out


def write_event_summary_csv(rows: list[EventSummaryRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "season",
                "gender",
                "event_no",
                "wa_event",
                "orientation",
                "top_n",
                "athletes_total",
                "results_total",
                "points_available",
                "avg_points_top_n",
                "avg_value_top_n_perf",
                "avg_perf_top_n",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "season": row.season,
                    "gender": row.gender,
                    "event_no": row.event_no,
                    "wa_event": row.wa_event,
                    "orientation": row.orientation,
                    "top_n": row.top_n,
                    "athletes_total": row.athletes_total,
                    "results_total": row.results_total,
                    "points_available": row.points_available,
                    "avg_points_top_n": row.avg_points_top_n,
                    "avg_value_top_n_perf": row.avg_value_top_n_perf,
                    "avg_perf_top_n": row.avg_perf_top_n,
                }
            )


def available_seasons(*, con: sqlite3.Connection) -> list[int]:
    rows = con.execute("SELECT DISTINCT season FROM results ORDER BY season").fetchall()
    return [int(r[0]) for r in rows]


def events_for_gender(*, con: sqlite3.Connection, gender: str) -> list[sqlite3.Row]:
    rows = con.execute(
        """
        SELECT name_no, wa_event, orientation
        FROM events
        WHERE gender = ?
        ORDER BY name_no
        """,
        (gender,),
    ).fetchall()
    rows.sort(key=lambda r: event_sort_key(str(r["name_no"])))
    return rows


def event_trend(
    *,
    con: sqlite3.Connection,
    gender: str,
    event_no: str,
    top_n: int = 10,
    seasons: Optional[Iterable[int]] = None,
) -> list[EventSummaryRow]:
    ev = con.execute(
        "SELECT id, wa_event, orientation FROM events WHERE gender = ? AND name_no = ?",
        (gender, event_no),
    ).fetchone()
    if not ev:
        return []

    event_id = int(ev["id"])
    wa_event = str(ev["wa_event"]) if ev["wa_event"] is not None else None
    orientation = str(ev["orientation"])

    if seasons is None:
        seasons = [
            int(r[0])
            for r in con.execute(
                "SELECT DISTINCT season FROM results WHERE gender = ? AND event_id = ? ORDER BY season",
                (gender, event_id),
            ).fetchall()
        ]

    sort_expr = "CASE WHEN e.orientation = 'lower' THEN r.value ELSE -r.value END"
    best_cte = f"""
        WITH best AS (
            SELECT
                r.*,
                e.orientation,
                {sort_expr} AS sort_value,
                ROW_NUMBER() OVER (
                    PARTITION BY r.season, r.gender, r.event_id, r.athlete_id
                    ORDER BY {sort_expr} ASC, r.result_date DESC
                ) AS rn
            FROM results r
            JOIN events e ON e.id = r.event_id
            WHERE r.season = ? AND r.gender = ? AND r.event_id = ?
        )
    """

    out: list[EventSummaryRow] = []
    for season in seasons:
        totals = con.execute(
            """
            SELECT
                COUNT(*) AS results_total,
                COUNT(DISTINCT athlete_id) AS athletes_total,
                SUM(CASE WHEN wa_points IS NOT NULL THEN 1 ELSE 0 END) AS points_available
            FROM results
            WHERE season = ? AND gender = ? AND event_id = ?
            """,
            (int(season), gender, event_id),
        ).fetchone()
        if not totals or int(totals["results_total"] or 0) == 0:
            continue

        results_total = int(totals["results_total"] or 0)
        athletes_total = int(totals["athletes_total"] or 0)
        points_available = int(totals["points_available"] or 0)

        points_rows = con.execute(
            best_cte
            + """
            SELECT wa_points
            FROM best
            WHERE rn = 1 AND wa_points IS NOT NULL
            ORDER BY wa_points DESC
            LIMIT ?
            """,
            (int(season), gender, event_id, int(top_n)),
        ).fetchall()
        avg_points = None
        if points_rows:
            vals = [int(r["wa_points"]) for r in points_rows if r["wa_points"] is not None]
            avg_points = (sum(vals) / len(vals)) if vals else None

        perf_rows = con.execute(
            best_cte
            + """
            SELECT value
            FROM best
            WHERE rn = 1 AND value IS NOT NULL
            ORDER BY sort_value ASC
            LIMIT ?
            """,
            (int(season), gender, event_id, int(top_n)),
        ).fetchall()
        avg_value = None
        if perf_rows:
            vals = [float(r["value"]) for r in perf_rows if r["value"] is not None]
            avg_value = (sum(vals) / len(vals)) if vals else None
        avg_perf = format_value_no(avg_value, orientation=orientation) if avg_value is not None else None

        out.append(
            EventSummaryRow(
                season=int(season),
                gender=gender,
                event_no=event_no,
                wa_event=wa_event,
                orientation=orientation,
                top_n=int(top_n),
                athletes_total=athletes_total,
                results_total=results_total,
                points_available=points_available,
                avg_points_top_n=avg_points,
                avg_value_top_n_perf=avg_value,
                avg_perf_top_n=avg_perf,
            )
        )

    out.sort(key=lambda r: r.season)
    return out


def athlete_results(
    *,
    con: sqlite3.Connection,
    athlete_id: int,
    since_season: int | None = None,
) -> list[sqlite3.Row]:
    sql = """
    SELECT
        r.season,
        r.gender,
        e.name_no AS event_no,
        r.performance_raw,
        r.wa_points,
        r.result_date,
        r.competition_name,
        r.venue_city,
        r.stadium,
        c.name AS club_name
    FROM results r
    JOIN events e ON e.id = r.event_id
    LEFT JOIN clubs c ON c.id = r.club_id
    WHERE r.athlete_id = ?
    """
    params: list[object] = [athlete_id]
    if since_season is not None:
        sql += " AND r.season >= ?"
        params.append(int(since_season))
    sql += " ORDER BY r.season DESC, r.wa_points DESC NULLS LAST, r.result_date DESC"
    return con.execute(sql, params).fetchall()


def event_results(
    *,
    con: sqlite3.Connection,
    season: int,
    gender: str,
    event_no: str,
    mode: str = "best",  # "best" | "all"
    limit: int = 200,
    offset: int = 0,
) -> tuple[int, Optional[str], str, list[sqlite3.Row]]:
    ev = con.execute(
        "SELECT id, wa_event, orientation FROM events WHERE gender = ? AND name_no = ?",
        (gender, event_no),
    ).fetchone()
    if not ev:
        return (0, None, "higher", [])

    event_id = int(ev["id"])
    wa_event = str(ev["wa_event"]) if ev["wa_event"] is not None else None
    orientation = str(ev["orientation"])

    limit = max(1, min(int(limit), 2000))
    offset = max(0, int(offset))

    sort_expr = "CASE WHEN e.orientation = 'lower' THEN r.value ELSE -r.value END"

    if mode == "best":
        best_cte = f"""
            WITH best AS (
                SELECT
                    r.id,
                    r.season,
                    r.gender,
                    r.athlete_id,
                    a.name AS athlete_name,
                    a.birth_date AS birth_date,
                    c.name AS club_name,
                    r.performance_raw,
                    r.performance_clean,
                    r.value,
                    r.wa_points,
                    r.result_date,
                    r.competition_name,
                    r.venue_city,
                    r.stadium,
                    r.source_url,
                    {sort_expr} AS sort_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.season, r.gender, r.event_id, r.athlete_id
                        ORDER BY {sort_expr} ASC, r.result_date DESC
                    ) AS rn
                FROM results r
                JOIN events e ON e.id = r.event_id
                JOIN athletes a ON a.id = r.athlete_id
                LEFT JOIN clubs c ON c.id = r.club_id
                WHERE r.season = ? AND r.gender = ? AND r.event_id = ?
            )
        """

        total_row = con.execute(best_cte + "SELECT COUNT(*) AS n FROM best WHERE rn = 1", (season, gender, event_id)).fetchone()
        total = int(total_row["n"] if total_row else 0)
        rows = con.execute(
            best_cte
            + """
            SELECT *
            FROM best
            WHERE rn = 1
            ORDER BY sort_value ASC, wa_points DESC NULLS LAST, result_date DESC
            LIMIT ? OFFSET ?
            """,
            (season, gender, event_id, limit, offset),
        ).fetchall()
        return (total, wa_event, orientation, rows)

    # mode == "all"
    total_row = con.execute(
        "SELECT COUNT(*) AS n FROM results WHERE season = ? AND gender = ? AND event_id = ?",
        (season, gender, event_id),
    ).fetchone()
    total = int(total_row["n"] if total_row else 0)
    rows = con.execute(
        f"""
        SELECT
            r.id,
            r.season,
            r.gender,
            r.athlete_id,
            a.name AS athlete_name,
            a.birth_date AS birth_date,
            c.name AS club_name,
            r.performance_raw,
            r.performance_clean,
            r.value,
            r.wa_points,
            r.result_date,
            r.competition_name,
            r.venue_city,
            r.stadium,
            r.source_url,
            {sort_expr} AS sort_value
        FROM results r
        JOIN events e ON e.id = r.event_id
        JOIN athletes a ON a.id = r.athlete_id
        LEFT JOIN clubs c ON c.id = r.club_id
        WHERE r.season = ? AND r.gender = ? AND r.event_id = ?
        ORDER BY sort_value ASC, wa_points DESC NULLS LAST, result_date DESC
        LIMIT ? OFFSET ?
        """,
        (season, gender, event_id, limit, offset),
    ).fetchall()
    return (total, wa_event, orientation, rows)
