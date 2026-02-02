from __future__ import annotations

import json
import sqlite3
import webbrowser
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from .queries import athlete_results, available_seasons, event_results, event_sort_key, event_summary, event_trend, events_for_gender


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
            return {"seasons": seasons, "genders": ["Women", "Men"]}

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
            return {
                "athlete_id": athlete_id,
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
            for i, r in enumerate(rows):
                d = dict(r)
                d["rank"] = int(offset) + i + 1
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

        raise _ApiError(404, "Ukjent API-endepunkt")

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
