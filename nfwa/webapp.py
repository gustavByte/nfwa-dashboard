from __future__ import annotations

import json
import sqlite3
import webbrowser
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from .queries import (
    DEFAULT_TOP_NS,
    athlete_results,
    available_seasons,
    event_results,
    event_sort_key,
    event_summary,
    event_trend,
    events_for_gender,
)


def run_web(*, db_path: Path, host: str = "127.0.0.1", port: int = 8000, open_browser: bool = True) -> None:
    static_dir = Path(__file__).resolve().parent / "web_static"
    if not static_dir.exists():
        raise FileNotFoundError(f"Mangler web_static: {static_dir}")

    server = _make_server(db_path=db_path, static_dir=static_dir, host=host, port=port)
    url = f"http://{host}:{port}/"
    print(f"Starter dashboard: {url}")
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    server.serve_forever()


def _make_server(*, db_path: Path, static_dir: Path, host: str, port: int) -> ThreadingHTTPServer:
    class Handler(_Handler):
        _db_path = db_path
        _static_dir = static_dir

    return ThreadingHTTPServer((host, int(port)), Handler)


class _Handler(BaseHTTPRequestHandler):
    _db_path: Path
    _static_dir: Path

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep console output readable.
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in {"/", "/index.html"}:
            return self._serve_static("index.html", content_type="text/html; charset=utf-8")

        if path == "/inspect":
            return self._serve_static("inspect.html", content_type="text/html; charset=utf-8")

        if path.startswith("/static/"):
            rel = path.removeprefix("/static/").lstrip("/")
            return self._serve_static(rel)

        if path.startswith("/api/"):
            try:
                payload = self._handle_api(path, qs)
            except _ApiError as exc:
                return self._json({"error": exc.message}, status=exc.status)
            except Exception as exc:  # noqa: BLE001
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, status=500)
            return self._json(payload)

        return self._json({"error": "Not found"}, status=404)

    def _handle_api(self, path: str, qs: dict[str, list[str]]) -> dict[str, Any] | list[dict[str, Any]]:
        if path == "/api/meta":
            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                seasons = available_seasons(con=con)
            return {"seasons": seasons, "genders": ["Women", "Men"], "top_ns": list(DEFAULT_TOP_NS)}

        if path == "/api/events":
            gender = _get_one(qs, "gender")
            if gender not in {"Women", "Men"}:
                raise _ApiError(400, "gender må være Women eller Men")
            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                rows = events_for_gender(con=con, gender=gender)
            return [
                {"event_no": r["name_no"], "wa_event": r["wa_event"], "orientation": r["orientation"]}
                for r in rows
            ]

        if path == "/api/event_trend":
            gender = _get_one(qs, "gender")
            event_no = _get_one(qs, "event")
            top_n = int(_get_one(qs, "top", default="10"))
            if gender not in {"Women", "Men"}:
                raise _ApiError(400, "gender må være Women eller Men")
            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                rows = event_trend(con=con, gender=gender, event_no=event_no, top_n=top_n)
            return [_summary_row_to_dict(r) for r in rows]

        if path == "/api/season_summary":
            gender = _get_one(qs, "gender")
            season = int(_get_one(qs, "season"))
            top_n = int(_get_one(qs, "top", default="10"))
            sort = _get_one(qs, "sort", default="points")
            if gender not in {"Women", "Men"}:
                raise _ApiError(400, "gender må være Women eller Men")
            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                rows = event_summary(con=con, season=season, gender=gender, top_ns=[top_n])

            if sort == "points":
                rows.sort(key=lambda r: (r.avg_points_top_n is None, -(r.avg_points_top_n or 0)))
            elif sort == "performance":
                # For times (lower is better) sort ascending, for distances sort descending.
                def perf_key(r: Any) -> tuple[bool, float]:
                    if r.avg_value_top_n_perf is None:
                        return (True, 0.0)
                    return (False, r.avg_value_top_n_perf if r.orientation == "lower" else -r.avg_value_top_n_perf)

                rows.sort(key=perf_key)
            elif sort == "event":
                rows.sort(key=lambda r: event_sort_key(str(r.event_no)))
            else:
                raise _ApiError(400, "sort må være event, points eller performance")

            return [_summary_row_to_dict(r) for r in rows]

        if path == "/api/athlete":
            athlete_id = int(_get_one(qs, "id"))
            since = qs.get("since", [None])[0]
            since_season = int(since) if since else None
            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                rows = athlete_results(con=con, athlete_id=athlete_id, since_season=since_season)
            birth_date = rows[0]["birth_date"] if rows else None
            return {
                "athlete_id": athlete_id,
                "birth_date": birth_date,
                "rows": [dict(r) for r in rows],
            }

        if path == "/api/event_results":
            gender = _get_one(qs, "gender")
            season = int(_get_one(qs, "season"))
            event_no = _get_one(qs, "event")
            mode = _get_one(qs, "mode", default="best")
            limit = int(_get_one(qs, "limit", default="200"))
            offset = int(_get_one(qs, "offset", default="0"))
            if gender not in {"Women", "Men"}:
                raise _ApiError(400, "gender må være Women eller Men")
            if mode not in {"best", "all"}:
                raise _ApiError(400, "mode må være best eller all")

            with sqlite3.connect(self._db_path) as con:
                con.row_factory = sqlite3.Row
                total, wa_event, orientation, rows = event_results(
                    con=con,
                    season=int(season),
                    gender=gender,
                    event_no=event_no,
                    mode=mode,
                    limit=int(limit),
                    offset=int(offset),
                )

            out_rows = []
            rank = 0
            prev_perf: str | None = None
            for i, r in enumerate(rows):
                d = dict(r)
                perf = d.get("performance_clean") or ""
                if perf != prev_perf:
                    rank = int(offset) + i + 1
                    prev_perf = perf
                d["rank"] = rank
                out_rows.append(d)

            return {
                "season": int(season),
                "gender": gender,
                "event_no": event_no,
                "wa_event": wa_event,
                "orientation": orientation,
                "mode": mode,
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total),
                "rows": out_rows,
            }

        if path == "/api/inspect/overview":
            return self._inspect_overview()

        if path == "/api/inspect/samples":
            source_type = qs.get("source_type", [None])[0]
            season = qs.get("season", [None])[0]
            gender = qs.get("gender", [None])[0]
            limit = int(_get_one(qs, "limit", default="20"))
            return self._inspect_samples(source_type=source_type, season=int(season) if season else None, gender=gender, limit=limit)

        if path == "/api/inspect/foreign":
            limit = int(_get_one(qs, "limit", default="50"))
            return self._inspect_foreign(limit=limit)

        if path == "/api/inspect/sources":
            return self._inspect_sources()

        raise _ApiError(404, "Ukjent API-endepunkt")

    def _inspect_overview(self) -> dict[str, Any]:
        with sqlite3.connect(self._db_path) as con:
            con.row_factory = sqlite3.Row
            total_results = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
            total_athletes = con.execute("SELECT COUNT(*) FROM athletes").fetchone()[0]
            total_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            total_clubs = con.execute("SELECT COUNT(*) FROM clubs").fetchone()[0]

            source_types = [
                dict(r) for r in con.execute(
                    """SELECT COALESCE(source_type, '(null)') AS source_type,
                              COUNT(*) AS results, COUNT(DISTINCT athlete_id) AS athletes,
                              MIN(season) AS min_season, MAX(season) AS max_season
                       FROM results GROUP BY source_type ORDER BY COUNT(*) DESC"""
                ).fetchall()
            ]
            nationalities = [
                dict(r) for r in con.execute(
                    "SELECT nationality, COUNT(*) AS count FROM athletes GROUP BY nationality ORDER BY COUNT(*) DESC LIMIT 20"
                ).fetchall()
            ]
            birth_formats = [
                dict(r) for r in con.execute(
                    """SELECT
                        COALESCE(r.source_type, '(null)') AS source_type,
                        CASE
                            WHEN a.birth_date IS NULL THEN 'NULL'
                            WHEN LENGTH(a.birth_date) = 10 THEN 'YYYY-MM-DD'
                            WHEN LENGTH(a.birth_date) = 4 THEN 'YYYY'
                            ELSE 'other'
                        END AS format,
                        COUNT(DISTINCT a.id) AS athletes
                    FROM athletes a JOIN results r ON r.athlete_id = a.id
                    GROUP BY r.source_type, format ORDER BY r.source_type, format"""
                ).fetchall()
            ]
            club_with = con.execute("SELECT COUNT(*) FROM results WHERE club_id IS NOT NULL").fetchone()[0]
            club_without = total_results - club_with
            wind_count = con.execute("SELECT COUNT(*) FROM results WHERE wind IS NOT NULL").fetchone()[0]

        return {
            "total_results": total_results,
            "total_athletes": total_athletes,
            "total_events": total_events,
            "total_clubs": total_clubs,
            "source_types": source_types,
            "nationalities": nationalities,
            "birth_formats": birth_formats,
            "club_with": club_with,
            "club_without": club_without,
            "wind_count": wind_count,
        }

    def _inspect_samples(
        self, *, source_type: str | None, season: int | None, gender: str | None, limit: int,
    ) -> list[dict[str, Any]]:
        where_parts: list[str] = []
        params: list[object] = []
        if source_type:
            where_parts.append("r.source_type = ?")
            params.append(source_type)
        if season:
            where_parts.append("r.season = ?")
            params.append(season)
        if gender:
            where_parts.append("r.gender = ?")
            params.append(gender)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(max(1, min(limit, 200)))

        with sqlite3.connect(self._db_path) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                f"""SELECT r.season, r.gender, e.name_no AS event, a.name AS athlete,
                           a.nationality, a.birth_date, r.performance_raw, r.wind,
                           r.wa_points, r.result_date, c.name AS club,
                           r.source_type, r.source_url
                    FROM results r
                    JOIN events e ON e.id = r.event_id
                    JOIN athletes a ON a.id = r.athlete_id
                    LEFT JOIN clubs c ON c.id = r.club_id
                    {where}
                    ORDER BY RANDOM()
                    LIMIT ?""",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def _inspect_foreign(self, *, limit: int) -> dict[str, Any]:
        with sqlite3.connect(self._db_path) as con:
            con.row_factory = sqlite3.Row
            total = con.execute("SELECT COUNT(*) FROM athletes WHERE nationality != 'NOR'").fetchone()[0]
            rows = con.execute(
                """SELECT a.id, a.name, a.gender, a.nationality, a.birth_date,
                          COUNT(r.id) AS results_count
                   FROM athletes a LEFT JOIN results r ON r.athlete_id = a.id
                   WHERE a.nationality != 'NOR'
                   GROUP BY a.id ORDER BY a.nationality, a.name LIMIT ?""",
                (max(1, min(limit, 200)),),
            ).fetchall()
        return {"total": total, "rows": [dict(r) for r in rows]}

    def _inspect_sources(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as con:
            con.row_factory = sqlite3.Row
            try:
                rows = con.execute(
                    "SELECT * FROM sources ORDER BY source_type, season, gender"
                ).fetchall()
            except Exception:
                return []
        return [dict(r) for r in rows]

    def _serve_static(self, rel_path: str, *, content_type: Optional[str] = None) -> None:
        safe = (rel_path or "").replace("\\", "/").lstrip("/")
        if ".." in safe:
            raise _ApiError(400, "Ugyldig sti")
        path = (self._static_dir / safe).resolve()
        if self._static_dir.resolve() not in path.parents and path != self._static_dir.resolve():
            raise _ApiError(400, "Ugyldig sti")
        if not path.exists() or not path.is_file():
            raise _ApiError(404, "Filen finnes ikke")

        ctype = content_type or _guess_content_type(path.name)
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        # This is a local dev dashboard; avoid confusing stale assets when code changes.
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, data: Any, *, status: int = 200) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class _ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = int(status)
        self.message = message


def _get_one(qs: dict[str, list[str]], key: str, *, default: Optional[str] = None) -> str:
    if key not in qs or not qs[key]:
        if default is not None:
            return default
        raise _ApiError(400, f"Mangler parameter: {key}")
    return qs[key][0]


def _guess_content_type(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".html"):
        return "text/html; charset=utf-8"
    if lower.endswith(".css"):
        return "text/css; charset=utf-8"
    if lower.endswith(".js"):
        return "application/javascript; charset=utf-8"
    if lower.endswith(".png"):
        return "image/png"
    if lower.endswith(".svg"):
        return "image/svg+xml"
    return "application/octet-stream"


def _summary_row_to_dict(row: Any) -> dict[str, Any]:
    d = asdict(row)
    d["event_order"] = int(event_sort_key(str(d.get("event_no") or ""))[0])
    # Keep numbers nicely rounded for UI
    if d.get("avg_points_top_n") is not None:
        d["avg_points_top_n"] = round(float(d["avg_points_top_n"]), 3)
    if d.get("avg_value_top_n_perf") is not None:
        d["avg_value_top_n_perf"] = round(float(d["avg_value_top_n_perf"]), 6)
    return d
