from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from .config import default_cache_dir, default_kondis_cache_dir, default_results_db_path, default_wa_scoring_db_path
from .export_site import export_site
from .ingest import sync_kondis, sync_landsoversikt
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
    build.add_argument("--min-year", type=int, default=2010, help="Første sesong som skal være med")
    build.add_argument("--max-year", type=int, default=None, help="Siste sesong (default: inneværende år)")
    build.add_argument("--refresh-years", type=int, default=2, help="Hvor mange siste år som re-lastest ned (default: 2)")
    build.add_argument("--gender", choices=["Women", "Men", "Both"], default="Both", help="Kjønn")
    build.add_argument("--no-kondis", action="store_true", help="Ikke synk Kondis (gateløp)")
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

    parser.error("Ukjent kommando")
    return 2
