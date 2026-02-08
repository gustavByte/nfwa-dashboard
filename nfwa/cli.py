from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from .config import default_cache_dir, default_kondis_cache_dir, default_old_data_dir, default_results_db_path, default_wa_scoring_db_path
from .export_site import export_site
from .ingest import sync_kondis, sync_landsoversikt, sync_old_data
from .queries import DEFAULT_TOP_NS, athlete_results, event_summary, write_event_summary_csv
from .site_build import build_site
from .webapp import run_web


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nfwa", description="Norsk friidrett -> SQLite + WA-poeng (lokalt)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sync = sub.add_parser("sync", help="Last ned og oppdater databasen fra landsoversikten")
    sync.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025], help="Sesonger, f.eks. 2023 2024 2025")
    sync.add_argument("--gender", choices=["Women", "Men", "Both"], default="Both", help="Kjønn")
    sync.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    sync.add_argument("--wa-db", type=Path, default=default_wa_scoring_db_path(), help="WA scoring-db (fra WA Poeng)")
    sync.add_argument("--wa-root", type=Path, default=Path("WA Poeng"), help="Mappe som inneholder wa_poeng/")
    sync.add_argument("--cache-dir", type=Path, default=default_cache_dir(), help="Cache for nedlastede HTML-sider")
    sync.add_argument("--refresh", action="store_true", help="Last ned på nytt selv om cache finnes")
    sync.add_argument("--polite-delay", type=float, default=0.5, help="Pause mellom sider (sekunder)")

    sync_k = sub.add_parser("sync-kondis", help="Last ned og oppdater databasen fra Kondis (gateløp-statistikk)")
    sync_k.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025], help="Sesonger, f.eks. 2023 2024 2025")
    sync_k.add_argument("--gender", choices=["Women", "Men", "Both"], default="Both", help="Kjønn")
    sync_k.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    sync_k.add_argument("--wa-db", type=Path, default=default_wa_scoring_db_path(), help="WA scoring-db (fra WA Poeng)")
    sync_k.add_argument("--wa-root", type=Path, default=Path("WA Poeng"), help="Mappe som inneholder wa_poeng/")
    sync_k.add_argument("--cache-dir", type=Path, default=default_kondis_cache_dir(), help="Cache for nedlastede HTML-sider")
    sync_k.add_argument("--refresh", action="store_true", help="Last ned på nytt selv om cache finnes")
    sync_k.add_argument("--polite-delay", type=float, default=0.5, help="Pause mellom sider (sekunder)")

    sync_old = sub.add_parser("sync-old", help="Importer gamle data (pre-2000) fra tekstfiler")
    sync_old.add_argument("--years", nargs="+", type=int, default=[1999], help="Sesonger, f.eks. 1999")
    sync_old.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    sync_old.add_argument("--wa-db", type=Path, default=default_wa_scoring_db_path(), help="WA scoring-db (fra WA Poeng)")
    sync_old.add_argument("--wa-root", type=Path, default=Path("WA Poeng"), help="Mappe som inneholder wa_poeng/")
    sync_old.add_argument("--data-dir", type=Path, default=default_old_data_dir(), help="Mappe med gamle data-filer")

    summary = sub.add_parser("event-summary", help="Lager top-N-snitt per øvelse (CSV)")
    summary.add_argument("--season", type=int, required=True, help="Sesong, f.eks. 2025")
    summary.add_argument("--gender", choices=["Women", "Men", "Both"], default="Both", help="Kjønn")
    summary.add_argument(
        "--top",
        nargs="+",
        type=int,
        default=list(DEFAULT_TOP_NS),
        help="Top-N, f.eks. 3 5 10 20 50 100 150 200",
    )
    summary.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    summary.add_argument("--csv", type=Path, default=None, help="Utfil (CSV)")

    athlete = sub.add_parser("athlete", help="Søk opp utøver (alle resultater)")
    athlete.add_argument("--athlete-id", type=int, required=True, help="showathl-id fra minfriidrettsstatistikk")
    athlete.add_argument("--since", type=int, default=None, help="Fra og med sesong, f.eks. 2024")
    athlete.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")

    web = sub.add_parser("web", help="Start lokal web-dashboard")
    web.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    web.add_argument("--host", type=str, default="127.0.0.1", help="Host, f.eks. 127.0.0.1")
    web.add_argument("--port", type=int, default=8000, help="Port, f.eks. 8000")
    web.add_argument("--no-open", action="store_true", help="Ikke åpne nettleser automatisk")

    export = sub.add_parser("export-site", help="Eksporter statisk webside (for publisering)")
    export.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    export.add_argument("--out", type=Path, default=Path("docs"), help="Utmappe (f.eks. docs/ for GitHub Pages)")
    export.add_argument(
        "--top",
        nargs="+",
        type=int,
        default=list(DEFAULT_TOP_NS),
        help="Top-N som pre-genereres (f.eks. 3 5 10 20 50 100 150 200)",
    )
    export.add_argument("--no-athlete-index", action="store_true", help="Ikke ta med athlete-oppslag (mindre eksport)")

    build = sub.add_parser("build-site", help="Oppdater database og eksporter statisk webside (for publisering)")
    build.add_argument("--min-year", type=int, default=2002, help="Første sesong som skal være med")
    build.add_argument("--max-year", type=int, default=None, help="Siste sesong (default: inneværende år)")
    build.add_argument("--refresh-years", type=int, default=2, help="Hvor mange siste år som re-lastest ned (default: 2)")
    build.add_argument("--gender", choices=["Women", "Men", "Both"], default="Both", help="Kjønn")
    build.add_argument("--no-kondis", action="store_true", help="Ikke synk Kondis (gateløp)")
    build.add_argument("--no-old-data", action="store_true", help="Ikke synk gamle data (pre-2000)")
    build.add_argument("--old-data-dir", type=Path, default=default_old_data_dir(), help="Mappe med gamle data-filer")
    build.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    build.add_argument("--wa-db", type=Path, default=default_wa_scoring_db_path(), help="WA scoring-db (fra WA Poeng)")
    build.add_argument("--wa-root", type=Path, default=Path("WA Poeng"), help="Mappe som inneholder wa_poeng/")
    build.add_argument("--cache-dir", type=Path, default=default_cache_dir(), help="Cache for nedlastede HTML-sider (minfriidrett)")
    build.add_argument(
        "--kondis-cache-dir",
        type=Path,
        default=default_kondis_cache_dir(),
        help="Cache for nedlastede HTML-sider (Kondis)",
    )
    build.add_argument("--out", type=Path, default=Path("docs"), help="Utmappe (f.eks. docs/ for GitHub Pages)")
    build.add_argument(
        "--top",
        nargs="+",
        type=int,
        default=list(DEFAULT_TOP_NS),
        help="Top-N som pre-genereres (f.eks. 3 5 10 20 50 100 150 200)",
    )
    build.add_argument("--no-athlete-index", action="store_true", help="Ikke ta med athlete-oppslag (mindre eksport)")
    build.add_argument("--polite-delay", type=float, default=0.5, help="Pause mellom sider (sekunder)")

    inspect = sub.add_parser("inspect-db", help="Kvalitetskontroll: vis oversikt over databaseinnhold per kilde")
    inspect.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")

    fill = sub.add_parser("fill-clubs", help="Fyll manglende klubber fra andre kilder for samme utøver/sesong")
    fill.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")

    browse = sub.add_parser("browse-db", help="Bla gjennom databaseinnhold (stikkprøver per kilde)")
    browse.add_argument("--db", type=Path, default=default_results_db_path(), help="SQLite-fil for resultater")
    browse.add_argument(
        "--view",
        choices=[
            "samples", "athletes", "foreign", "wind", "missing-club",
            "missing-birth", "birth-format", "sources", "events", "check",
        ],
        default="check",
        help="Hva som skal vises (default: check = kjør alle kvalitetssjekker)",
    )
    browse.add_argument("--source-type", type=str, default=None, help="Filtrer på kildetype (f.eks. kondis, minfriidrett)")
    browse.add_argument("--season", type=int, default=None, help="Filtrer på sesong")
    browse.add_argument("--gender", choices=["Women", "Men"], default=None, help="Filtrer på kjønn")
    browse.add_argument("--limit", type=int, default=20, help="Maks antall rader (default: 20)")

    args = parser.parse_args(argv)

    if args.cmd == "sync":
        years = [int(y) for y in args.years]
        from .config import SOURCES

        sources = SOURCES if args.gender == "Both" else [s for s in SOURCES if s.gender == args.gender]

        res = sync_landsoversikt(
            db_path=args.db,
            wa_db_path=args.wa_db,
            wa_poeng_root=args.wa_root,
            cache_dir=args.cache_dir,
            years=years,
            sources=sources,
            refresh=bool(args.refresh),
            polite_delay_s=float(args.polite_delay),
        )
        print(
            "Sync ferdig:",
            f"pages={res.pages}",
            f"rows={res.rows_seen}",
            f"wa_ok={res.wa_points_ok}",
            f"wa_failed={res.wa_points_failed}",
            f"wa_missing={res.wa_points_missing}",
            sep=" ",
        )
        return 0

    if args.cmd == "sync-kondis":
        years = [int(y) for y in args.years]
        res = sync_kondis(
            db_path=args.db,
            wa_db_path=args.wa_db,
            wa_poeng_root=args.wa_root,
            cache_dir=args.cache_dir,
            years=years,
            gender=args.gender,
            refresh=bool(args.refresh),
            polite_delay_s=float(args.polite_delay),
        )
        print(
            "Sync ferdig (Kondis):",
            f"pages={res.pages}",
            f"rows={res.rows_seen}",
            f"wa_ok={res.wa_points_ok}",
            f"wa_failed={res.wa_points_failed}",
            f"wa_missing={res.wa_points_missing}",
            sep=" ",
        )
        return 0

    if args.cmd == "sync-old":
        years = [int(y) for y in args.years]
        res = sync_old_data(
            db_path=args.db,
            wa_db_path=args.wa_db,
            wa_poeng_root=args.wa_root,
            data_dir=args.data_dir,
            years=years,
        )
        print(
            "Sync ferdig (gamle data):",
            f"seasons={res.pages}",
            f"rows={res.rows_seen}",
            f"wa_ok={res.wa_points_ok}",
            f"wa_failed={res.wa_points_failed}",
            f"wa_missing={res.wa_points_missing}",
            sep=" ",
        )
        return 0

    if args.cmd == "event-summary":
        genders = ["Women", "Men"] if args.gender == "Both" else [args.gender]
        out_csv = args.csv
        if out_csv is None:
            gtag = "both" if args.gender == "Both" else args.gender.lower()
            out_csv = Path("data") / f"event_summary_{args.season}_{gtag}.csv"

        all_rows = []
        con = sqlite3.connect(args.db)
        con.row_factory = sqlite3.Row
        try:
            for g in genders:
                all_rows.extend(event_summary(con=con, season=int(args.season), gender=g, top_ns=args.top))
        finally:
            con.close()

        write_event_summary_csv(all_rows, out_csv)
        print(f"Skrev {len(all_rows)} rader til {out_csv}")
        return 0

    if args.cmd == "athlete":
        con = sqlite3.connect(args.db)
        con.row_factory = sqlite3.Row
        try:
            rows = athlete_results(con=con, athlete_id=int(args.athlete_id), since_season=args.since)
        finally:
            con.close()

        if not rows:
            print("Ingen treff.")
            return 0

        # Print a compact table-like view
        for r in rows[:200]:
            pts = r["wa_points"]
            pts_s = "-" if pts is None else str(pts)
            print(
                f"{r['season']} {r['gender']} | {r['event_no']} | {r['performance_raw']} | {pts_s} | {r['result_date'] or '-'} | {r['competition_name'] or '-'}"
            )
        if len(rows) > 200:
            print(f"... ({len(rows) - 200} flere)")
        return 0

    if args.cmd == "web":
        run_web(db_path=args.db, host=args.host, port=int(args.port), open_browser=not bool(args.no_open))
        return 0

    if args.cmd == "export-site":
        export_site(
            db_path=args.db,
            out_dir=args.out,
            top_ns=[int(x) for x in args.top],
            include_athlete_index=not bool(args.no_athlete_index),
        )
        print(f"Eksporterte statisk web til: {args.out}")
        return 0

    if args.cmd == "build-site":
        res = build_site(
            db_path=args.db,
            wa_db_path=args.wa_db,
            wa_poeng_root=args.wa_root,
            cache_dir=args.cache_dir,
            kondis_cache_dir=args.kondis_cache_dir,
            min_year=int(args.min_year),
            max_year=args.max_year,
            gender=args.gender,
            refresh_years=int(args.refresh_years),
            include_kondis=not bool(args.no_kondis),
            include_old_data=not bool(args.no_old_data),
            old_data_dir=args.old_data_dir,
            out_dir=args.out,
            top_ns=[int(x) for x in args.top],
            include_athlete_index=not bool(args.no_athlete_index),
            polite_delay_s=float(args.polite_delay),
        )
        print(
            "Build-site ferdig:",
            f"years_fill={res.years_filled}",
            f"years_refresh={res.years_refreshed}",
            f"lands_pages={res.landsoversikt.pages}",
            f"lands_rows={res.landsoversikt.rows_seen}",
            f"kondis_pages={(res.kondis.pages if res.kondis else 0)}",
            f"out={res.out_dir}",
            sep=" ",
        )
        return 0

    if args.cmd == "inspect-db":
        _cmd_inspect_db(args.db)
        return 0

    if args.cmd == "fill-clubs":
        from . import db as results_db

        con = results_db.connect(args.db)
        try:
            updated = results_db.fill_club_gaps(con)
            con.commit()
        finally:
            con.close()
        print(f"Oppdaterte {updated} rader med manglende klubb.")
        return 0

    if args.cmd == "browse-db":
        _cmd_browse_db(
            db_path=args.db,
            view=args.view,
            source_type=args.source_type,
            season=args.season,
            gender=args.gender,
            limit=args.limit,
        )
        return 0

    parser.error("Ukjent kommando")
    return 2


def _cmd_inspect_db(db_path: Path) -> None:
    """Print a quality report of the database contents."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        # Overall counts
        total_results = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        total_athletes = con.execute("SELECT COUNT(*) FROM athletes").fetchone()[0]
        total_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        total_clubs = con.execute("SELECT COUNT(*) FROM clubs").fetchone()[0]

        print(f"Database: {db_path}")
        print(f"  Resultater: {total_results}")
        print(f"  Utøvere: {total_athletes}")
        print(f"  Øvelser: {total_events}")
        print(f"  Klubber: {total_clubs}")
        print()

        # Per source_type
        print("Per kildetype (source_type):")
        source_rows = con.execute(
            """
            SELECT
                COALESCE(source_type, '(null)') AS st,
                COUNT(*) AS cnt,
                COUNT(DISTINCT athlete_id) AS athletes,
                MIN(season) AS min_season,
                MAX(season) AS max_season
            FROM results
            GROUP BY source_type
            ORDER BY cnt DESC
            """
        ).fetchall()
        for r in source_rows:
            print(f"  {r['st']}: {r['cnt']} resultater, {r['athletes']} utøvere, sesong {r['min_season']}-{r['max_season']}")
        print()

        # Nationality distribution
        print("Nasjonalitet (athletes):")
        nat_rows = con.execute(
            "SELECT nationality, COUNT(*) AS cnt FROM athletes GROUP BY nationality ORDER BY cnt DESC LIMIT 15"
        ).fetchall()
        for r in nat_rows:
            print(f"  {r['nationality']}: {r['cnt']}")
        print()

        # Birth date coverage
        bd_total = con.execute("SELECT COUNT(*) FROM athletes").fetchone()[0]
        bd_full = con.execute("SELECT COUNT(*) FROM athletes WHERE LENGTH(birth_date) = 10").fetchone()[0]
        bd_year = con.execute("SELECT COUNT(*) FROM athletes WHERE LENGTH(birth_date) = 4").fetchone()[0]
        bd_null = con.execute("SELECT COUNT(*) FROM athletes WHERE birth_date IS NULL").fetchone()[0]
        print("Fødselsdato-dekning (athletes):")
        print(f"  Full dato (YYYY-MM-DD): {bd_full}")
        print(f"  Kun år (YYYY): {bd_year}")
        print(f"  Mangler: {bd_null}")
        print(f"  Totalt: {bd_total}")
        print()

        # Club coverage in results
        club_total = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        club_set = con.execute("SELECT COUNT(*) FROM results WHERE club_id IS NOT NULL").fetchone()[0]
        club_null = club_total - club_set
        print("Klubb-dekning (results):")
        print(f"  Med klubb: {club_set}")
        print(f"  Mangler klubb: {club_null}")
        print()

        # Sources catalog
        try:
            src_rows = con.execute(
                "SELECT source_type, url, season, gender, row_count, last_synced_at FROM sources ORDER BY last_synced_at DESC LIMIT 20"
            ).fetchall()
            if src_rows:
                print("Siste synkroniserte kilder (sources, max 20):")
                for r in src_rows:
                    url_short = (r["url"] or "")[:60]
                    print(f"  [{r['source_type']}] {r['season']} {r['gender']} | {r['row_count']} rader | {url_short}")
                print()
        except sqlite3.OperationalError:
            pass  # sources table might not exist yet

        # Wind data coverage
        wind_total = con.execute("SELECT COUNT(*) FROM results WHERE wind IS NOT NULL").fetchone()[0]
        print(f"Resultater med vinddata: {wind_total}")

    finally:
        con.close()


def _cmd_browse_db(
    *,
    db_path: Path,
    view: str,
    source_type: str | None,
    season: int | None,
    gender: str | None,
    limit: int,
) -> None:
    """Browse database contents with various views."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        if view == "check":
            _browse_check_all(con, limit=limit)
        elif view == "samples":
            _browse_samples(con, source_type=source_type, season=season, gender=gender, limit=limit)
        elif view == "athletes":
            _browse_athletes(con, source_type=source_type, limit=limit)
        elif view == "foreign":
            _browse_foreign(con, limit=limit)
        elif view == "wind":
            _browse_wind(con, limit=limit)
        elif view == "missing-club":
            _browse_missing_club(con, source_type=source_type, limit=limit)
        elif view == "missing-birth":
            _browse_missing_birth(con, source_type=source_type, limit=limit)
        elif view == "birth-format":
            _browse_birth_format(con, source_type=source_type, limit=limit)
        elif view == "sources":
            _browse_sources(con)
        elif view == "events":
            _browse_events(con, gender=gender)
    finally:
        con.close()


def _print_table(headers: list[str], rows: list[list[str]], *, max_col_width: int = 40) -> None:
    """Print a formatted table to stdout."""
    if not rows:
        print("  (ingen rader)")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], min(len(str(cell)), max_col_width))

    # Print header
    header_line = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    print(f"  {header_line}")
    print(f"  {'-+-'.join('-' * w for w in widths)}")

    # Print rows
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            s = str(cell)
            w = widths[i] if i < len(widths) else max_col_width
            if len(s) > w:
                s = s[: w - 1] + "\u2026"
            cells.append(s.ljust(w))
        print(f"  {' | '.join(cells)}")


def _browse_samples(
    con: sqlite3.Connection,
    *,
    source_type: str | None,
    season: int | None,
    gender: str | None,
    limit: int,
) -> None:
    """Show sample results, optionally filtered."""
    where_parts = []
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
    params.append(max(1, min(limit, 500)))

    rows = con.execute(
        f"""
        SELECT
            r.season, r.gender, e.name_no AS event,
            a.name AS athlete, a.nationality, a.birth_date,
            r.performance_raw, r.wind, r.wa_points,
            r.result_date, c.name AS club,
            r.source_type, r.source_url
        FROM results r
        JOIN events e ON e.id = r.event_id
        JOIN athletes a ON a.id = r.athlete_id
        LEFT JOIN clubs c ON c.id = r.club_id
        {where}
        ORDER BY r.season DESC, r.id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()

    filter_desc = []
    if source_type:
        filter_desc.append(f"source_type={source_type}")
    if season:
        filter_desc.append(f"season={season}")
    if gender:
        filter_desc.append(f"gender={gender}")
    title = "Stikkprøve resultater"
    if filter_desc:
        title += f" ({', '.join(filter_desc)})"
    print(f"\n{title} — {len(rows)} rader:")

    _print_table(
        ["Sesong", "Kjønn", "Øvelse", "Utøver", "Nat", "Født", "Resultat", "Vind", "WA", "Dato", "Klubb", "Kilde", "URL"],
        [
            [
                r["season"], r["gender"], r["event"], r["athlete"],
                r["nationality"] or "-", r["birth_date"] or "-",
                r["performance_raw"], r["wind"] if r["wind"] is not None else "",
                r["wa_points"] if r["wa_points"] is not None else "-",
                r["result_date"] or "-", r["club"] or "-",
                r["source_type"] or "-", r["source_url"] or "-",
            ]
            for r in rows
        ],
    )


def _browse_athletes(con: sqlite3.Connection, *, source_type: str | None, limit: int) -> None:
    """Show athletes with their metadata."""
    if source_type:
        rows = con.execute(
            """
            SELECT DISTINCT a.id, a.name, a.gender, a.nationality, a.birth_date
            FROM athletes a
            JOIN results r ON r.athlete_id = a.id
            WHERE r.source_type = ?
            ORDER BY a.name
            LIMIT ?
            """,
            (source_type, max(1, min(limit, 500))),
        ).fetchall()
        print(f"\nUtøvere fra kilde '{source_type}' — {len(rows)} rader:")
    else:
        rows = con.execute(
            "SELECT id, name, gender, nationality, birth_date FROM athletes ORDER BY name LIMIT ?",
            (max(1, min(limit, 500)),),
        ).fetchall()
        print(f"\nUtøvere — {len(rows)} rader:")

    _print_table(
        ["ID", "Navn", "Kjønn", "Nasjonalitet", "Fødselsdato"],
        [[r["id"], r["name"], r["gender"], r["nationality"], r["birth_date"] or "-"] for r in rows],
    )


def _browse_foreign(con: sqlite3.Connection, *, limit: int) -> None:
    """Show athletes with non-NOR nationality."""
    rows = con.execute(
        """
        SELECT a.id, a.name, a.gender, a.nationality, a.birth_date,
               COUNT(r.id) AS results_count
        FROM athletes a
        LEFT JOIN results r ON r.athlete_id = a.id
        WHERE a.nationality != 'NOR'
        GROUP BY a.id
        ORDER BY a.nationality, a.name
        LIMIT ?
        """,
        (max(1, min(limit, 500)),),
    ).fetchall()

    total = con.execute("SELECT COUNT(*) FROM athletes WHERE nationality != 'NOR'").fetchone()[0]
    print(f"\nUtenlandske utøvere (ikke-NOR) — viser {len(rows)} av {total}:")

    _print_table(
        ["ID", "Navn", "Kjønn", "Nasjonalitet", "Fødselsdato", "Resultater"],
        [
            [r["id"], r["name"], r["gender"], r["nationality"], r["birth_date"] or "-", r["results_count"]]
            for r in rows
        ],
    )


def _browse_wind(con: sqlite3.Connection, *, limit: int) -> None:
    """Show results that have wind data."""
    rows = con.execute(
        """
        SELECT r.season, r.gender, e.name_no AS event, a.name AS athlete,
               r.performance_raw, r.wind, r.wa_points, r.source_type
        FROM results r
        JOIN events e ON e.id = r.event_id
        JOIN athletes a ON a.id = r.athlete_id
        WHERE r.wind IS NOT NULL
        ORDER BY r.season DESC, ABS(r.wind) DESC
        LIMIT ?
        """,
        (max(1, min(limit, 500)),),
    ).fetchall()

    total = con.execute("SELECT COUNT(*) FROM results WHERE wind IS NOT NULL").fetchone()[0]
    print(f"\nResultater med vinddata — viser {len(rows)} av {total}:")

    _print_table(
        ["Sesong", "Kjønn", "Øvelse", "Utøver", "Resultat", "Vind", "WA", "Kilde"],
        [
            [r["season"], r["gender"], r["event"], r["athlete"],
             r["performance_raw"], f"{r['wind']:+.1f}", r["wa_points"] or "-", r["source_type"] or "-"]
            for r in rows
        ],
    )


def _browse_missing_club(con: sqlite3.Connection, *, source_type: str | None, limit: int) -> None:
    """Show results missing club data."""
    where = "WHERE r.club_id IS NULL"
    params: list[object] = []
    if source_type:
        where += " AND r.source_type = ?"
        params.append(source_type)
    params.append(max(1, min(limit, 500)))

    rows = con.execute(
        f"""
        SELECT r.season, r.gender, e.name_no AS event, a.name AS athlete,
               r.performance_raw, r.source_type
        FROM results r
        JOIN events e ON e.id = r.event_id
        JOIN athletes a ON a.id = r.athlete_id
        {where}
        ORDER BY r.season DESC
        LIMIT ?
        """,
        params,
    ).fetchall()

    total = con.execute(f"SELECT COUNT(*) FROM results r {where}", params[:-1]).fetchone()[0]
    desc = f" (kilde: {source_type})" if source_type else ""
    print(f"\nResultater uten klubb{desc} — viser {len(rows)} av {total}:")

    _print_table(
        ["Sesong", "Kjønn", "Øvelse", "Utøver", "Resultat", "Kilde"],
        [[r["season"], r["gender"], r["event"], r["athlete"], r["performance_raw"], r["source_type"] or "-"] for r in rows],
    )


def _browse_missing_birth(con: sqlite3.Connection, *, source_type: str | None, limit: int) -> None:
    """Show athletes missing birth date."""
    if source_type:
        rows = con.execute(
            """
            SELECT DISTINCT a.id, a.name, a.gender, a.nationality
            FROM athletes a
            JOIN results r ON r.athlete_id = a.id
            WHERE a.birth_date IS NULL AND r.source_type = ?
            ORDER BY a.name
            LIMIT ?
            """,
            (source_type, max(1, min(limit, 500))),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT id, name, gender, nationality FROM athletes WHERE birth_date IS NULL ORDER BY name LIMIT ?",
            (max(1, min(limit, 500)),),
        ).fetchall()

    total = con.execute("SELECT COUNT(*) FROM athletes WHERE birth_date IS NULL").fetchone()[0]
    desc = f" (kilde: {source_type})" if source_type else ""
    print(f"\nUtøvere uten fødselsdato{desc} — viser {len(rows)} av {total}:")

    _print_table(
        ["ID", "Navn", "Kjønn", "Nasjonalitet"],
        [[r["id"], r["name"], r["gender"], r["nationality"]] for r in rows],
    )


def _browse_birth_format(con: sqlite3.Connection, *, source_type: str | None, limit: int) -> None:
    """Check birth_date format distribution per source type."""
    print("\nFødselsdato-format per kilde:")
    rows = con.execute(
        """
        SELECT
            COALESCE(r.source_type, '(null)') AS st,
            CASE
                WHEN a.birth_date IS NULL THEN 'NULL'
                WHEN LENGTH(a.birth_date) = 10 THEN 'YYYY-MM-DD'
                WHEN LENGTH(a.birth_date) = 4 THEN 'YYYY'
                ELSE 'annet(' || LENGTH(a.birth_date) || ')'
            END AS format,
            COUNT(DISTINCT a.id) AS athletes
        FROM athletes a
        JOIN results r ON r.athlete_id = a.id
        GROUP BY r.source_type, format
        ORDER BY r.source_type, format
        """
    ).fetchall()

    _print_table(
        ["Kildetype", "Format", "Utøvere"],
        [[r["st"], r["format"], r["athletes"]] for r in rows],
    )

    # Also show any suspicious birth dates
    bad_rows = con.execute(
        """
        SELECT DISTINCT a.id, a.name, a.birth_date, r.source_type
        FROM athletes a
        JOIN results r ON r.athlete_id = a.id
        WHERE a.birth_date IS NOT NULL
          AND a.birth_date NOT GLOB '[0-9][0-9][0-9][0-9]'
          AND a.birth_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
        LIMIT ?
        """,
        (max(1, min(limit, 500)),),
    ).fetchall()
    if bad_rows:
        print(f"\nMistenkelige fødselsdatoer ({len(bad_rows)} stk):")
        _print_table(
            ["ID", "Navn", "Fødselsdato", "Kilde"],
            [[r["id"], r["name"], r["birth_date"], r["source_type"] or "-"] for r in bad_rows],
        )
    else:
        print("\nIngen mistenkelige fødselsdatoer funnet.")


def _browse_sources(con: sqlite3.Connection) -> None:
    """Show the sources catalog."""
    try:
        rows = con.execute(
            "SELECT source_type, url, season, gender, row_count, last_synced_at FROM sources ORDER BY source_type, season, gender"
        ).fetchall()
    except sqlite3.OperationalError:
        print("Sources-tabellen finnes ikke ennå.")
        return

    print(f"\nKildekatalog (sources) — {len(rows)} oppføringer:")
    _print_table(
        ["Type", "URL", "Sesong", "Kjønn", "Rader", "Sist synk"],
        [[r["source_type"], r["url"] or "-", r["season"] or "-", r["gender"] or "-", r["row_count"] or "-", r["last_synced_at"] or "-"] for r in rows],
    )


def _browse_events(con: sqlite3.Connection, *, gender: str | None) -> None:
    """Show all events with result counts."""
    where = "WHERE e.gender = ?" if gender else ""
    params: list[object] = [gender] if gender else []

    rows = con.execute(
        f"""
        SELECT e.gender, e.name_no, e.wa_event, e.orientation,
               COUNT(r.id) AS results_count,
               COUNT(DISTINCT r.athlete_id) AS athletes_count,
               MIN(r.season) AS min_season,
               MAX(r.season) AS max_season
        FROM events e
        LEFT JOIN results r ON r.event_id = e.id
        {where}
        GROUP BY e.id
        ORDER BY e.gender, e.name_no
        """,
        params,
    ).fetchall()

    desc = f" ({gender})" if gender else ""
    print(f"\nØvelser{desc} — {len(rows)} stk:")
    _print_table(
        ["Kjønn", "Øvelse", "WA-kode", "Retning", "Resultater", "Utøvere", "Fra", "Til"],
        [
            [r["gender"], r["name_no"], r["wa_event"] or "-", r["orientation"],
             r["results_count"], r["athletes_count"], r["min_season"] or "-", r["max_season"] or "-"]
            for r in rows
        ],
    )


def _browse_check_all(con: sqlite3.Connection, *, limit: int) -> None:
    """Run all quality checks and show a combined report."""
    print("=" * 70)
    print("  KVALITETSKONTROLL — Stikkprøver fra alle kilder")
    print("=" * 70)

    # 1. Sample per source_type
    source_types = con.execute(
        "SELECT DISTINCT COALESCE(source_type, '(null)') AS st FROM results ORDER BY st"
    ).fetchall()

    for st_row in source_types:
        st = st_row["st"]
        st_filter = None if st == "(null)" else st
        where = "WHERE r.source_type = ?" if st_filter else "WHERE r.source_type IS NULL"
        params: list[object] = [st_filter] if st_filter else []
        params.append(min(limit, 5))

        rows = con.execute(
            f"""
            SELECT r.season, r.gender, e.name_no AS event, a.name AS athlete,
                   a.nationality, a.birth_date, r.performance_raw, r.wind,
                   r.wa_points, r.result_date, c.name AS club, r.source_url
            FROM results r
            JOIN events e ON e.id = r.event_id
            JOIN athletes a ON a.id = r.athlete_id
            LEFT JOIN clubs c ON c.id = r.club_id
            {where}
            ORDER BY RANDOM()
            LIMIT ?
            """,
            params,
        ).fetchall()

        count = con.execute(
            f"SELECT COUNT(*) FROM results r {where}", params[:-1]
        ).fetchone()[0]

        print(f"\n--- Kilde: {st} ({count} totalt, viser {len(rows)} tilfeldig) ---")
        _print_table(
            ["Sesong", "Kjønn", "Øvelse", "Utøver", "Nat", "Født", "Resultat", "Vind", "WA", "Dato", "Klubb", "source_url"],
            [
                [
                    r["season"], r["gender"], r["event"], r["athlete"],
                    r["nationality"] or "-", r["birth_date"] or "-",
                    r["performance_raw"],
                    f"{r['wind']:+.1f}" if r["wind"] is not None else "",
                    r["wa_points"] if r["wa_points"] is not None else "-",
                    r["result_date"] or "-", r["club"] or "-",
                    r["source_url"] or "-",
                ]
                for r in rows
            ],
        )

    # 2. Foreign athletes
    foreign = con.execute(
        "SELECT a.name, a.nationality, a.gender FROM athletes a WHERE a.nationality != 'NOR' ORDER BY a.nationality, a.name LIMIT 10"
    ).fetchall()
    foreign_total = con.execute("SELECT COUNT(*) FROM athletes WHERE nationality != 'NOR'").fetchone()[0]
    if foreign:
        print(f"\n--- Utenlandske utøvere ({foreign_total} totalt, viser {len(foreign)}) ---")
        _print_table(
            ["Navn", "Nasjonalitet", "Kjønn"],
            [[r["name"], r["nationality"], r["gender"]] for r in foreign],
        )

    # 3. Birth date sanity
    print("\n--- Fødselsdato-format per kilde ---")
    _browse_birth_format(con, source_type=None, limit=limit)

    # 4. Quick sanity checks
    print("\n--- Sanity-sjekker ---")
    checks = [
        (
            "Resultater uten source_type",
            "SELECT COUNT(*) FROM results WHERE source_type IS NULL",
        ),
        (
            "Utøvere uten fødselsdato",
            "SELECT COUNT(*) FROM athletes WHERE birth_date IS NULL",
        ),
        (
            "Resultater uten klubb",
            "SELECT COUNT(*) FROM results WHERE club_id IS NULL",
        ),
        (
            "Resultater med vinddata",
            "SELECT COUNT(*) FROM results WHERE wind IS NOT NULL",
        ),
        (
            "Kondis fødselsdatoer med -01-01 (skal være 0 etter fix)",
            """SELECT COUNT(DISTINCT a.id) FROM athletes a
               JOIN results r ON r.athlete_id = a.id
               WHERE r.source_type = 'kondis'
                 AND a.birth_date LIKE '%-01-01'""",
        ),
        (
            "Old_data source_url med 'old_data:' prefix (intern ref)",
            "SELECT COUNT(*) FROM results WHERE source_type = 'old_data' AND source_url LIKE 'old_data:%'",
        ),
    ]
    for label, sql in checks:
        try:
            val = con.execute(sql).fetchone()[0]
            status = "OK" if val == 0 and "skal være 0" in label else ""
            print(f"  {label}: {val} {status}")
        except sqlite3.OperationalError:
            print(f"  {label}: (feil i spørring)")

    print()
    print("Bruk --view for mer detaljert visning:")
    print("  --view samples           Stikkprøve resultater (med --source-type, --season, --gender)")
    print("  --view athletes          Utøverliste (med --source-type)")
    print("  --view foreign           Utenlandske utøvere")
    print("  --view wind              Resultater med vinddata")
    print("  --view missing-club      Resultater uten klubb")
    print("  --view missing-birth     Utøvere uten fødselsdato")
    print("  --view birth-format      Fødselsdato-format per kilde")
    print("  --view sources           Kildekatalog")
    print("  --view events            Alle øvelser med telleverk")
