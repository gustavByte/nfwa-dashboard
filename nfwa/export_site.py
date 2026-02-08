from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import unicodedata
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .queries import DEFAULT_TOP_NS, available_seasons, event_results, event_sort_key, event_summary, event_trend, events_for_gender


_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


def export_site(
    *,
    db_path: Path,
    out_dir: Path,
    top_ns: Iterable[int] = DEFAULT_TOP_NS,
    include_athlete_index: bool = True,
) -> None:
    """
    Eksporterer en statisk versjon av dashboardet slik at det kan publiseres på f.eks. GitHub Pages.

    Strukturen som lages i out_dir:
      - index.html
      - static/app.js, static/styles.css
      - api/*.json (pre-genererte endepunkter)
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Fant ikke database: {db_path}")

    web_src = Path(__file__).resolve().parent / "web_static"
    if not (web_src / "index.html").exists():
        raise FileNotFoundError(f"Mangler web_static: {web_src}")

    top_ns = tuple(int(x) for x in top_ns)

    out_dir.mkdir(parents=True, exist_ok=True)
    _clean_dir(out_dir / "static")
    _clean_dir(out_dir / "api")
    _unlink_if_exists(out_dir / "index.html")
    _unlink_if_exists(out_dir / ".nojekyll")

    # Copy frontend assets to a "static/" folder (matches paths used in index.html)
    (out_dir / "static").mkdir(parents=True, exist_ok=True)
    shutil.copyfile(web_src / "index.html", out_dir / "index.html")
    shutil.copyfile(web_src / "app.js", out_dir / "static" / "app.js")
    shutil.copyfile(web_src / "styles.css", out_dir / "static" / "styles.css")
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        seasons = available_seasons(con=con)
        genders = ["Women", "Men"]
        generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        _write_json(
            out_dir / "api" / "meta.json",
            {"seasons": seasons, "genders": genders, "top_ns": list(top_ns), "generated_at": generated_at},
        )

        # Events per gender (also provides stable event_key used in static URLs)
        events_by_gender: dict[str, list[dict[str, Any]]] = {}
        event_key_by_gender_and_no: dict[tuple[str, str], str] = {}
        for gender in genders:
            ev_rows = events_for_gender(con=con, gender=gender)
            ev_out: list[dict[str, Any]] = []
            for r in ev_rows:
                event_no = str(r["name_no"])
                key = _event_key(event_no)
                event_key_by_gender_and_no[(gender, event_no)] = key
                ev_out.append(
                    {
                        "event_no": event_no,
                        "wa_event": r["wa_event"],
                        "orientation": r["orientation"],
                        "event_key": key,
                    }
                )
            events_by_gender[gender] = ev_out
            _write_json(out_dir / "api" / "events" / f"{gender}.json", ev_out)

        # Precompute season summaries and trends for the dashboard's Top-N values.
        for season in seasons:
            for gender in genders:
                for top_n in top_ns:
                    rows = event_summary(con=con, season=int(season), gender=gender, top_ns=[int(top_n)])
                    payload = [_summary_row_to_dict(r) for r in rows]
                    _write_json(out_dir / "api" / "season_summary" / str(season) / gender / f"top{int(top_n)}.json", payload)

        for gender in genders:
            for ev in events_by_gender.get(gender, []):
                event_no = str(ev["event_no"])
                key = str(ev["event_key"])
                for top_n in top_ns:
                    rows = event_trend(con=con, gender=gender, event_no=event_no, top_n=int(top_n), seasons=seasons)
                    payload = [_summary_row_to_dict(r) for r in rows]
                    _write_json(out_dir / "api" / "event_trend" / gender / key / f"top{int(top_n)}.json", payload)

        # Event results (full list per event/season/gender/mode; pagination is handled client-side)
        for season in seasons:
            for gender in genders:
                for ev in events_by_gender.get(gender, []):
                    event_no = str(ev["event_no"])
                    key = str(ev["event_key"])
                    for mode in ("best", "all"):
                        total, wa_event, orientation, all_rows = _event_results_all(
                            con=con,
                            season=int(season),
                            gender=gender,
                            event_no=event_no,
                            mode=mode,
                        )
                        out_rows: list[dict[str, Any]] = []
                        rank = 0
                        prev_perf: str | None = None
                        for i, r in enumerate(all_rows):
                            d = dict(r)
                            d.pop("sort_value", None)
                            perf = d.get("performance_clean") or ""
                            if perf != prev_perf:
                                rank = i + 1
                                prev_perf = perf
                            d["rank"] = rank
                            out_rows.append(d)

                        _write_json(
                            out_dir / "api" / "event_results" / str(season) / gender / key / f"{mode}.json",
                            {
                                "season": int(season),
                                "gender": gender,
                                "event_no": event_no,
                                "wa_event": wa_event,
                                "orientation": orientation,
                                "mode": mode,
                                "total": int(total),
                                "rows": out_rows,
                            },
                        )

        if include_athlete_index:
            _write_json(out_dir / "api" / "athlete" / "index.json", _build_athlete_index(con=con))
    finally:
        con.close()


def _event_key(event_no: str) -> str:
    raw = (event_no or "").strip()
    if not raw:
        raw = "event"

    # Make a readable ASCII-ish prefix, but keep a stable hash for uniqueness.
    norm = unicodedata.normalize("NFKD", raw)
    asciiish = norm.encode("ascii", "ignore").decode("ascii").lower()
    asciiish = _SLUG_NON_ALNUM.sub("-", asciiish).strip("-")
    if not asciiish:
        asciiish = "event"
    asciiish = asciiish[:50].rstrip("-")

    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{asciiish}--{h}"


def _summary_row_to_dict(row: Any) -> dict[str, Any]:
    d = asdict(row)
    d["event_order"] = int(event_sort_key(str(d.get("event_no") or ""))[0])
    # Keep numbers nicely rounded for UI
    if d.get("avg_points_top_n") is not None:
        d["avg_points_top_n"] = round(float(d["avg_points_top_n"]), 3)
    if d.get("avg_value_top_n_perf") is not None:
        d["avg_value_top_n_perf"] = round(float(d["avg_value_top_n_perf"]), 6)
    return d


def _event_results_all(
    *,
    con: sqlite3.Connection,
    season: int,
    gender: str,
    event_no: str,
    mode: str,
) -> tuple[int, str | None, str, list[sqlite3.Row]]:
    # The API clamps limit to 2000; page through to get all rows.
    all_rows: list[sqlite3.Row] = []
    total = 0
    wa_event = None
    orientation = "higher"

    offset = 0
    page_size = 2000
    while True:
        total_i, wa_event_i, orientation_i, rows = event_results(
            con=con,
            season=int(season),
            gender=gender,
            event_no=event_no,
            mode=mode,
            limit=page_size,
            offset=offset,
        )
        total = int(total_i)
        wa_event = wa_event_i
        orientation = str(orientation_i)

        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        if offset >= total:
            break
    return (total, wa_event, orientation, all_rows)


def _build_athlete_index(*, con: sqlite3.Connection) -> dict[str, Any]:
    rows = con.execute(
        """
        SELECT
            r.athlete_id,
            a.birth_date,
            r.season,
            r.gender,
            e.name_no AS event_no,
            e.wa_event,
            r.performance_raw,
            r.wind,
            r.wa_points,
            r.result_date,
            r.competition_name,
            r.venue_city,
            r.stadium,
            c.name AS club_name
        FROM results r
        JOIN events e ON e.id = r.event_id
        JOIN athletes a ON a.id = r.athlete_id
        LEFT JOIN clubs c ON c.id = r.club_id
        ORDER BY
            r.athlete_id ASC,
            r.season DESC,
            (r.wa_points IS NULL) ASC,
            r.wa_points DESC,
            r.result_date DESC
        """
    ).fetchall()

    by_id: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        aid = str(r["athlete_id"])
        by_id.setdefault(aid, []).append(
            {
                "season": int(r["season"]),
                "gender": r["gender"],
                "birth_date": r["birth_date"],
                "event_no": r["event_no"],
                "wa_event": r["wa_event"],
                "performance_raw": r["performance_raw"],
                "wind": r["wind"],
                "wa_points": r["wa_points"],
                "result_date": r["result_date"],
                "competition_name": r["competition_name"],
                "venue_city": r["venue_city"],
                "stadium": r["stadium"],
                "club_name": r["club_name"],
            }
        )

    return {"by_id": by_id}


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    path.write_text(raw, encoding="utf-8")


def _clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, onerror=_on_rmtree_error)


def _on_rmtree_error(func: Any, path: str, exc_info: Any) -> None:
    # Windows: exported docs/ can be marked read-only (e.g. by Git),
    # which makes shutil.rmtree fail. Clear the attribute and retry.
    try:
        os.chmod(path, stat.S_IWRITE)
    except Exception:  # noqa: BLE001
        pass
    func(path)


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
