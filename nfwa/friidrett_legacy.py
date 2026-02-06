from __future__ import annotations

from collections import defaultdict
import hashlib
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

import requests
from lxml import html

from .minfriidrett import ScrapedResult
from .util import clean_performance, parse_ddmmyy


@dataclass(frozen=True)
class FriidrettPage:
    season: int
    gender: str  # "Women" | "Men"
    url: str


FRIIDRETT_PAGES_2004: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2004
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/b7689de2190c471eaa83dd6040439d26.aspx"),  # Sprint
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/2c88b6a41ec14efa81fe8a40a4c7bcbe.aspx"),  # Distanse
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/9740c1dba7044ddeb3193efd8f077cda.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/14ef797fd6974e52a8d4e4cc645ef1f5.aspx"),  # Hoppøvelser
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/3b1be65688ad4059bbb992e5c7c7afb9.aspx"),  # Kastøvelser
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/6b91c177fd804828ac38ad9e25d48546.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2004
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/f590577446724c0ebae08990edc969e9.aspx"),  # Sprint
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/c4361d6660d54ff1a4a17e32659d1a36.aspx"),  # Distanse
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/2b23f67fb8094dc5ac1144a5dd57670c.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/b38c73b65a1a4b0d96c2907bb975cdae.aspx"),  # Hoppøvelser
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/7d0f036512504dc4a5ab9b4e8d5749a7.aspx"),  # Kastøvelser
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/a6bd689db0584d21a7aa92e594c24af4.aspx"),  # Mangekamp
    # Kappgang (samme side inneholder både menn og kvinner)
    FriidrettPage(season=2004, gender="Men", url="https://www.friidrett.no/link/7e74fa1b87e84a32ad7c648297d1bc3f.aspx"),
    FriidrettPage(season=2004, gender="Women", url="https://www.friidrett.no/link/7e74fa1b87e84a32ad7c648297d1bc3f.aspx"),
)


FRIIDRETT_PAGES_2005: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2005
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/1f12a57b44d848b8aadcdfb32f938c1a.aspx"),  # Sprint
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/988739c982df4e7bb07c390dd0d5623c.aspx"),  # Distanse
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/82aa64ca42914887a47db224da7958f8.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/49c5b34448ca47e9919c3a7deae47ea2.aspx"),  # Hoppøvelser
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/b3d28584083d4abeb962ee732c5c81b6.aspx"),  # Kastøvelser
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/81ee84fb2f0f4424ac4c42a4cc0ebf06.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2005
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/d2143a1bd9ed4e4fab3cd14b84e8602a.aspx"),  # Sprint
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/6aad55062a8a4414b502c9010c2e7c60.aspx"),  # Distanse
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/3331de9bb6c64411864c3014c67b37c9.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/46369a0d6e05445b97011120b8ed8540.aspx"),  # Hoppøvelser
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/e169051afb604ff8b83df994c2c0c76b.aspx"),  # Kastøvelser
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/20c7881ac43b4a83b875d6c098987e55.aspx"),  # Mangekamp
    # Kappgang (samme side inneholder både menn og kvinner)
    FriidrettPage(season=2005, gender="Men", url="https://www.friidrett.no/link/3c21893a626f47ff9419f23234ed798b.aspx"),
    FriidrettPage(season=2005, gender="Women", url="https://www.friidrett.no/link/3c21893a626f47ff9419f23234ed798b.aspx"),
)


FRIIDRETT_PAGES_2006: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2006
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/e088594c8bd3428590a022a286aba683.aspx"),  # Sprint
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/d4356c7b35dd408894470440c1395470.aspx"),  # Distanse
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/c465e69cd5884ad1b250d380197430b4.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/dc85258a115b4ca6aceebee9edec59c7.aspx"),  # Hoppøvelser
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/d9df769c9bd94c8da9904ce4d0e93783.aspx"),  # Kastøvelser
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/b57b48dc899d429c8ab00d8007a6e224.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2006
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/82185085a0ad4776a9f94d72c2bb9378.aspx"),  # Sprint
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/496f22dc108246899e3fee35afb409b0.aspx"),  # Distanse
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/52bbdf518e20470aacb05e2b8019ca24.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/a37551d47c5946ed9b62afb5f76b7107.aspx"),  # Hoppøvelser
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/b830649644774b54b6ec7a1d475c4f8c.aspx"),  # Kastøvelser
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/3a4e08f7139f4336aa99b2a7a8be3c03.aspx"),  # Mangekamp
    # Kappgang (samme PDF inneholder både menn og kvinner)
    FriidrettPage(season=2006, gender="Men", url="https://www.friidrett.no/link/1b84630cea7745e4a7ecfc4528486953.aspx"),
    FriidrettPage(season=2006, gender="Women", url="https://www.friidrett.no/link/1b84630cea7745e4a7ecfc4528486953.aspx"),
)


FRIIDRETT_PAGES_2007: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2007
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/c2938be4ac7b46dbbc231397759271a6.aspx"),  # Sprint
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/ce7cd713fc28461b819ccc961f65fd29.aspx"),  # Distanse
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/dd76d4679f8447279305ade7a0d245e8.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/20c2385c7ff44a078b8b2ec6bc2fb41a.aspx"),  # Hoppøvelser
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/6944d92a0211477ca048f8eb825fc75f.aspx"),  # Kastøvelser
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/291e34c189ea4665bae2d7511e1d75ac.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2007
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/7fff65814ba24701aac9e3eace693646.aspx"),  # Sprint
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/0e57cc073eda4b64b510d95385313d07.aspx"),  # Distanse
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/82950f16a4334e13b03a0396336b3304.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/e5f1b973b464496aa65a7ed8e5f46e72.aspx"),  # Hoppøvelser
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/76d569ec039746d091c626401eaad822.aspx"),  # Kastøvelser
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/0aaa3cc0b5d24302826074d08def0a01.aspx"),  # Mangekamp
    # Kappgang (samme PDF inneholder både menn og kvinner)
    FriidrettPage(season=2007, gender="Men", url="https://www.friidrett.no/link/15aabcac69214321ad5d974d3b3c11b2.aspx"),
    FriidrettPage(season=2007, gender="Women", url="https://www.friidrett.no/link/15aabcac69214321ad5d974d3b3c11b2.aspx"),
)


FRIIDRETT_PAGES_2008: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2008
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/ededda76178747499ab11bea8ebaa930.aspx"),  # Sprint
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/47d7e60b56c24727b14b0df456ebb049.aspx"),  # Distanse
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/0329d071badb421ebdbae98d140c7ccf.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/d00ff6eaace545ffaa3e97f7f2a658be.aspx"),  # Hoppøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/14ef5ded64ec4edc84de594fc0929cab.aspx"),  # Kastøvelser
    FriidrettPage(season=2008, gender="Men", url="https://www.friidrett.no/link/2b28b7d7700a496794d78ebd385aaacd.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2008
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/3ff25f4a57bb445c9643a678d7dc259e.aspx"),  # Sprint
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/7d498b1130774467a50e2918667213df.aspx"),  # Distanse
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/62dec3aef5414932af0395bf434f4f21.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/1d94cc63d00f48cebe4be05fec33aa9a.aspx"),  # Hoppøvelser
    FriidrettPage(season=2008, gender="Women", url="https://www.friidrett.no/link/8a94b13eb9d34f1b8e1864b7b6bb67b9.aspx"),  # Kastøvelser
    FriidrettPage(
        season=2008,
        gender="Women",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-ksmk08.htm",
    ),  # Mangekamp (dead on current site, kept as fallback URL)
    # Senior Kappgang 2008 (URL currently appears dead, but kept so ingest can recover if source returns later)
    FriidrettPage(
        season=2008,
        gender="Men",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-kappgangs2008.pdf",
    ),
    FriidrettPage(
        season=2008,
        gender="Women",
        url="https://www.friidrett.no/globalassets/aktivitet/statistikk/arsstatistikker/2008/www.friidrett.no-kappgangs2008.pdf",
    ),
)

FRIIDRETT_PAGES_2009: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2009
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/6b0cef9d0a7d45a783dc1eb43420041e.aspx"),  # Sprint
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/b2d5b54f138c4d7d99e4efd00c3c99ac.aspx"),  # Distanse
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/659719298dde47569553085ad1ba9a9a.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/89f4d82f6d9341a5b7060c65f1f22c95.aspx"),  # Hoppøvelser
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/e6600f3cf16c48a09ad03ba97576b2f1.aspx"),  # Kastøvelser
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/9703872fc0d84c5db7e1c6161c78b1d0.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2009
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/aacc9ac900cb463081dad0182b69953d.aspx"),  # Sprint
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/6f6b2b0f0e8e4ffaa25a8e5d62eb36f7.aspx"),  # Distanse
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/d89814ed4b54461dbf56b5df7e404692.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/303475b9a53644c4a3d01b8a1374f6c0.aspx"),  # Hoppøvelser
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/be492eefba5e47aabb01dffe53cdbb9b.aspx"),  # Kastøvelser
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/2d4f12910e8a4f16ad3f7e5a882a1fc6.aspx"),  # Mangekamp
    # Kappgang (samme side inneholder både menn og kvinner)
    FriidrettPage(season=2009, gender="Men", url="https://www.friidrett.no/link/8047a01e6fb7452186bfb1c4583c2a4a.aspx"),
    FriidrettPage(season=2009, gender="Women", url="https://www.friidrett.no/link/8047a01e6fb7452186bfb1c4583c2a4a.aspx"),
)

FRIIDRETT_PAGES_2010: tuple[FriidrettPage, ...] = (
    # MENN / MEN 2010
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/9f75977878cc4932809862cd399e435c.aspx"),  # Sprint
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/ef7554091d4f4e3eb3d27159365e2f82.aspx"),  # Distanse
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/01774b1d5d9842ddb8622316090d03b7.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/580473c8526f4e0d879df48950427fe0.aspx"),  # Hoppøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/97eefbd05e3b4b7aad6f13569801a065.aspx"),  # Kastøvelser
    FriidrettPage(season=2010, gender="Men", url="https://www.friidrett.no/link/2d3b2204f863462c8b3f79a57010357d.aspx"),  # Mangekamp
    # KVINNER / WOMEN 2010
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/e21697d0f7db47fcb77d6825cda87118.aspx"),  # Sprint
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/38589b538d324a7eacfd96e33ac85316.aspx"),  # Distanse
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/3a3ffae3dd724e7f89ebfe9555ef561a.aspx"),  # Hekkeøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/24faa01d343a4e25807beddb39f4b73b.aspx"),  # Hoppøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/5be74b7a9c3a4d9089371d20f19fb7d5.aspx"),  # Kastøvelser
    FriidrettPage(season=2010, gender="Women", url="https://www.friidrett.no/link/2f5b992e90744492b8a25ad530088cd2.aspx"),  # Mangekamp
)

FRIIDRETT_PAGES: tuple[FriidrettPage, ...] = (
    FRIIDRETT_PAGES_2004
    + FRIIDRETT_PAGES_2005
    + FRIIDRETT_PAGES_2006
    + FRIIDRETT_PAGES_2007
    + FRIIDRETT_PAGES_2008
    + FRIIDRETT_PAGES_2009
    + FRIIDRETT_PAGES_2010
)


def pages_for_years(*, years: Iterable[int], gender: str = "Both") -> list[FriidrettPage]:
    ys = {int(y) for y in years}
    pages = [p for p in FRIIDRETT_PAGES if int(p.season) in ys]
    if gender == "Both":
        return pages
    return [p for p in pages if p.gender == gender]


def fetch_page(
    *,
    url: str,
    cache_dir: Path,
    refresh: bool = False,
    session: Optional[requests.Session] = None,
) -> bytes:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / _safe_cache_filename(url)
    if cache_path.exists() and not refresh:
        return cache_path.read_bytes()

    sess = session or requests.Session()
    headers = {"User-Agent": "nfwa-local/0.1 (contact: local)"}
    resp = sess.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    content = resp.content
    cache_path.write_bytes(content)
    return content


def parse_page(*, html_bytes: bytes, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    """Parse friidrett.no Word-HTML pages (legacy) and return best-per-athlete rows per event."""
    if html_bytes.lstrip().startswith(b"%PDF"):
        # Kappgang PDFs (e.g. 2007) contain both genders in one document.
        return _parse_kappgang_pdf(pdf_bytes=html_bytes, season=season, gender=gender, source_url=source_url)

    doc = html.fromstring(html_bytes)
    if _looks_like_not_found_page(doc):
        return []

    out: list[ScrapedResult] = []

    for h2 in doc.xpath("//h2"):
        heading_raw = (h2.text_content() or "").strip()
        event_no = _canonical_event_no(heading_raw, gender=gender)
        if not event_no:
            continue

        tables = _tables_until_next_h2(h2)
        if not tables:
            continue

        # 2010 usually has records+results, while 2008 often has a single table.
        # Parse all candidates and keep the one with most valid result rows.
        best: list[ScrapedResult] = []
        for table in tables:
            parsed = _parse_results_table(
                table=table,
                season=season,
                gender=gender,
                event_no=event_no,
                source_url=source_url,
            )
            if len(parsed) > len(best):
                best = parsed
        out.extend(best)

    if out:
        return out

    # Fallback for pages with no <h2>-per-event structure (e.g. 2008 women throws).
    return _parse_sectioned_table_page(doc=doc, season=season, gender=gender, source_url=source_url)


_WIND_CELL_RE = re.compile(r"^[+\-–−]?\s*\d+(?:[.,]\d+)?$")
_PERF_WITH_TRAIL_WIND_RE = re.compile(r"^(?P<perf>.+?)\s+(?P<wind>[+\-–−]\d+(?:[.,]\d+)?)[#*]?$")
_PLACEMENT_CELL_RE = re.compile(r"^\(?\d+[A-Za-z0-9/.-]*\)?[A-Za-z0-9/.-]*$")

_HURDLE_HEIGHT_CM: dict[tuple[str, int], str] = {
    ("Women", 60): "84,0",
    ("Women", 100): "84,0",
    ("Women", 200): "76,2",
    ("Women", 300): "76,2",
    ("Women", 400): "76,2",
    ("Men", 110): "106,7",
    ("Men", 200): "76,2",
    ("Men", 300): "91,4",
    ("Men", 400): "91,4",
}

_STEEPLE_HEIGHT_CM: dict[tuple[str, int], str] = {
    ("Women", 2000): "76,2",
    ("Women", 3000): "76,2",
    ("Men", 2000): "91,4",
    ("Men", 3000): "91,4",
}


def _parse_results_table(*, table: html.HtmlElement, season: int, gender: str, event_no: str, source_url: str) -> list[ScrapedResult]:
    seen: set[int] = set()
    out: list[ScrapedResult] = []

    last_full: Optional[tuple[str, Optional[str], Optional[str]]] = None  # (name, club, birth_iso)
    known_by_surname: dict[str, tuple[str, Optional[str], Optional[str]]] = {}
    known_ids_by_name: dict[str, dict[Optional[str], int]] = defaultdict(dict)
    rank = 0

    for tr in table.xpath(".//tr"):
        cells = _compact_cells([_norm_cell(c.text_content()) for c in tr.xpath("./td|./th")])
        if not cells:
            continue

        parsed = _parse_result_cells(
            cells=cells,
            season=season,
            last_full=last_full,
            known_by_surname=known_by_surname,
        )
        if not parsed:
            continue

        (
            cleaned,
            wind,
            athlete_name,
            club_name,
            birth_iso,
            placement,
            competition_code,
            venue_city,
            result_date,
            next_last_full,
        ) = parsed
        last_full = next_last_full
        surname = _surname_token(athlete_name)
        if surname:
            known_by_surname[surname] = (athlete_name, club_name, birth_iso)

        athlete_id = _resolve_event_athlete_id(
            gender=gender,
            athlete_name=athlete_name,
            birth_iso=birth_iso,
            known_ids_by_name=known_ids_by_name,
        )
        if athlete_id is None:
            continue
        if athlete_id in seen:
            continue
        seen.add(athlete_id)
        _remember_event_athlete_id(
            athlete_name=athlete_name,
            birth_iso=birth_iso,
            athlete_id=athlete_id,
            known_ids_by_name=known_ids_by_name,
        )
        rank += 1

        out.append(
            ScrapedResult(
                season=int(season),
                gender=gender,
                event_no=event_no,
                rank_in_list=rank,
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=wind,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=club_name,
                birth_date=birth_iso,
                placement_raw=placement,
                venue_city=venue_city,
                stadium=None,
                competition_id=None,
                competition_name=competition_code,
                result_date=result_date,
                source_url=source_url,
            )
        )

    return out


def _parse_sectioned_table_page(*, doc: html.HtmlElement, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    best: list[ScrapedResult] = []
    for table in doc.xpath("//table"):
        parsed = _parse_sectioned_table(table=table, season=season, gender=gender, source_url=source_url)
        if len(parsed) > len(best):
            best = parsed
    return best


def _parse_sectioned_table(*, table: html.HtmlElement, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    out: list[ScrapedResult] = []
    seen_by_event: dict[str, set[int]] = defaultdict(set)
    rank_by_event: dict[str, int] = defaultdict(int)
    last_full_by_event: dict[str, tuple[str, Optional[str], Optional[str]]] = {}
    known_by_event_surname: dict[str, dict[str, tuple[str, Optional[str], Optional[str]]]] = defaultdict(dict)
    known_ids_by_event_name: dict[str, dict[str, dict[Optional[str], int]]] = defaultdict(dict)

    current_event: Optional[str] = None
    for tr in table.xpath(".//tr"):
        cells = _compact_cells([_norm_cell(c.text_content()) for c in tr.xpath("./td|./th")])
        if not cells:
            continue

        heading = _section_heading_candidate(cells)
        if heading is not None:
            current_event = _canonical_event_no(heading, gender=gender)
            continue

        if not current_event:
            continue

        parsed = _parse_result_cells(
            cells=cells,
            season=season,
            last_full=last_full_by_event.get(current_event),
            known_by_surname=known_by_event_surname[current_event],
        )
        if not parsed:
            continue

        (
            cleaned,
            wind,
            athlete_name,
            club_name,
            birth_iso,
            placement,
            competition_code,
            venue_city,
            result_date,
            next_last_full,
        ) = parsed
        last_full_by_event[current_event] = next_last_full
        surname = _surname_token(athlete_name)
        if surname:
            known_by_event_surname[current_event][surname] = (athlete_name, club_name, birth_iso)

        athlete_id = _resolve_event_athlete_id(
            gender=gender,
            athlete_name=athlete_name,
            birth_iso=birth_iso,
            known_ids_by_name=known_ids_by_event_name[current_event],
        )
        if athlete_id is None:
            continue
        if athlete_id in seen_by_event[current_event]:
            continue
        seen_by_event[current_event].add(athlete_id)
        _remember_event_athlete_id(
            athlete_name=athlete_name,
            birth_iso=birth_iso,
            athlete_id=athlete_id,
            known_ids_by_name=known_ids_by_event_name[current_event],
        )

        rank_by_event[current_event] += 1
        out.append(
            ScrapedResult(
                season=int(season),
                gender=gender,
                event_no=current_event,
                rank_in_list=int(rank_by_event[current_event]),
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=wind,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=club_name,
                birth_date=birth_iso,
                placement_raw=placement,
                venue_city=venue_city,
                stadium=None,
                competition_id=None,
                competition_name=competition_code,
                result_date=result_date,
                source_url=source_url,
            )
        )

    return out


def _parse_result_cells(
    *,
    cells: list[str],
    season: int,
    last_full: Optional[tuple[str, Optional[str], Optional[str]]],
    known_by_surname: Optional[dict[str, tuple[str, Optional[str], Optional[str]]]] = None,
) -> Optional[tuple]:
    if not cells:
        return None

    perf_text, wind_from_perf = _split_perf_and_wind(cells[0])
    cleaned = clean_performance(perf_text)
    if not cleaned or not cleaned.clean or not any(ch.isdigit() for ch in cleaned.clean):
        return None

    has_wind = len(cells) >= 2 and _looks_like_wind(cells[1])
    wind = _parse_wind(cells[1]) if has_wind else wind_from_perf

    idx_ath = _guess_athlete_index(cells=cells, has_wind=has_wind, last_full=last_full)
    if idx_ath is None or idx_ath >= len(cells):
        return None

    athlete_cell = (cells[idx_ath] or "").strip()
    birth_raw = (cells[idx_ath + 1] or "").strip() if len(cells) > idx_ath + 1 else ""
    is_abbrev = not birth_raw and _looks_like_abbrev_name(athlete_cell)

    if is_abbrev:
        resolved = _resolve_abbreviated_athlete(
            athlete_cell=athlete_cell,
            last_full=last_full,
            known_by_surname=known_by_surname or {},
        )
        if not resolved:
            return None
        athlete_name, club_name, prev_birth = resolved
        birth_iso = _parse_birth_date(birth_raw) or prev_birth
        next_last_full = (athlete_name, club_name, birth_iso)
    else:
        athlete_name, club_name = _split_name_and_club(athlete_cell)
        if not athlete_name:
            return None
        birth_iso = _parse_birth_date(birth_raw)
        next_last_full = (athlete_name, club_name, birth_iso)

    placement = _extract_placement(cells=cells, idx_ath=idx_ath)
    result_date, date_idx = _extract_result_date(cells=cells, idx_ath=idx_ath, season=season)
    competition_code, venue_city = _extract_comp_and_venue(cells=cells, idx_ath=idx_ath, date_idx=date_idx)

    return (
        cleaned,
        wind,
        athlete_name,
        club_name,
        birth_iso,
        placement,
        competition_code,
        venue_city,
        result_date,
        next_last_full,
    )


def _split_perf_and_wind(text: str) -> tuple[str, Optional[float]]:
    s = _norm_cell(text)
    if not s:
        return ("", None)
    m = _PERF_WITH_TRAIL_WIND_RE.match(s)
    if not m:
        return (s, None)
    return (m.group("perf").strip(), _parse_wind(m.group("wind")))


def _compact_cells(cells: list[str]) -> list[str]:
    # Some Word-exported tables (notably 2007 women) are heavily padded with empty columns.
    xs = ["" if c == "Ā" else c for c in cells]
    while xs and not xs[0]:
        xs.pop(0)
    while xs and not xs[-1]:
        xs.pop()
    return xs


def _guess_athlete_index(*, cells: list[str], has_wind: bool, last_full: Optional[tuple[str, Optional[str], Optional[str]]]) -> Optional[int]:
    start = 2 if has_wind else 1
    for cand in (start, start + 1):
        if cand < len(cells) and _is_likely_athlete_cell(cells[cand], last_full=last_full):
            return cand

    # Fallback for odd column layouts: scan only the early columns (before venue/date).
    for cand in range(1, min(len(cells), 6)):
        if _is_likely_athlete_cell(cells[cand], last_full=last_full):
            return cand
    return None


def _is_likely_athlete_cell(text: str, *, last_full: Optional[tuple[str, Optional[str], Optional[str]]]) -> bool:
    s = _norm_cell(text)
    if not s:
        return False
    if _looks_like_non_athlete_marker(s):
        return False
    if not any(ch.isalpha() for ch in s):
        return False
    if _looks_like_wind(s) or _looks_like_placement(s):
        return False
    if "," in s:
        return True
    if len(s.split()) >= 2:
        return True
    return bool(last_full and _looks_like_abbrev_name(s))


def _looks_like_non_athlete_marker(text: str) -> bool:
    s = _norm_cell(text)
    if not s:
        return False
    compact = s.replace(" ", "")
    low = compact.lower()

    # Seen on legacy pages as placement/qualification markers, not athlete names.
    if re.fullmatch(r"\([a-z0-9]{1,5}\)[a-z0-9]{0,4}", low):
        return True
    if low in {"ok", "dns", "dnf", "nm", "nc", "dq"}:
        return True
    if s.islower() and re.fullmatch(r"[a-z]{1,3}", low):
        return True
    return False


def _looks_like_abbrev_name(text: str) -> bool:
    s = _norm_cell(text)
    if not s or any(ch.isdigit() for ch in s) or _looks_like_non_athlete_marker(s):
        return False
    parts = s.split()
    if len(parts) != 1:
        return False
    token = parts[0]
    if any(ch in ",()/[]{}" for ch in token):
        return False
    if not token[0].isalpha():
        return False
    if not all(ch.isalpha() or ch in "-'" for ch in token):
        return False
    return len(token) >= 2 and any(ch.islower() for ch in token[1:])


def _resolve_abbreviated_athlete(
    *,
    athlete_cell: str,
    last_full: Optional[tuple[str, Optional[str], Optional[str]]],
    known_by_surname: dict[str, tuple[str, Optional[str], Optional[str]]],
) -> Optional[tuple[str, Optional[str], Optional[str]]]:
    key = _norm_cell(athlete_cell).lower()
    if not key:
        return None

    found = known_by_surname.get(key)
    if found:
        return found

    if last_full:
        last_surname = _surname_token(last_full[0])
        if last_surname and last_surname.lower() == key:
            return last_full

    return None


def _surname_token(name: str) -> Optional[str]:
    tokens = [t.strip(".,") for t in _norm_cell(name).split() if t.strip(".,")]
    if not tokens:
        return None
    tail = tokens[-1]
    if not tail or not any(ch.isalpha() for ch in tail):
        return None
    return tail.lower()


def _extract_placement(*, cells: list[str], idx_ath: int) -> Optional[str]:
    if idx_ath > 1 and _looks_like_placement(cells[idx_ath - 1]):
        return _none_if_empty(cells[idx_ath - 1])
    if len(cells) > idx_ath + 2 and _looks_like_placement(cells[idx_ath + 2]):
        return _none_if_empty(cells[idx_ath + 2])
    return None


def _extract_result_date(*, cells: list[str], idx_ath: int, season: int) -> tuple[Optional[str], Optional[int]]:
    for i in range(idx_ath + 2, len(cells)):
        parsed = _parse_result_date(cells[i], season=season)
        if parsed:
            return parsed, i
    return None, None


def _extract_comp_and_venue(*, cells: list[str], idx_ath: int, date_idx: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    if date_idx is None:
        return None, None

    mids = [i for i in range(idx_ath + 2, date_idx) if _none_if_empty(cells[i])]
    if not mids:
        return None, None

    non_place = [i for i in mids if not _looks_like_placement(cells[i])]
    if not non_place:
        return None, None

    venue_idx = non_place[-1]
    venue_city = _clean_venue(cells[venue_idx])

    comp_candidates = [i for i in non_place if i < venue_idx]
    competition_code = _none_if_empty(cells[comp_candidates[-1]]) if comp_candidates else None
    return competition_code, venue_city


def _canonical_event_no(heading: str, *, gender: str) -> Optional[str]:
    text = _norm_cell(heading)
    if not text:
        return None

    # Strip standards/notes, keep the Norwegian label (left of "/" if present)
    base = text.split("(")[0]
    base = re.split(r"\s+[–—-]\s+", base, maxsplit=1)[0]
    base = base.split("/")[0]
    base = _norm_cell(base).upper()

    # Combined events
    base_norm = base.replace("-", " ").strip()
    if base_norm.startswith("10 KAMP"):
        return "10 kamp"
    if base_norm.startswith("7 KAMP"):
        return "7 kamp"
    if base_norm.startswith("5 KAMP"):
        return "5 kamp"
    if base_norm.startswith("KAST 5 KAMP") or base_norm.startswith("KAST 5-KAMP"):
        return "Kast 5 Kamp (Slegge-Kule-Diskos-Spyd-Vektkast)"

    # Field events
    if base.startswith("HØYDE") or base.startswith("HOYDE"):
        return "Høyde"
    if base.startswith("STAV"):
        return "Stav"
    if base.startswith("LENGDE"):
        return "Lengde"
    if base.startswith("TRESTEG"):
        return "Tresteg"

    # Throws (canonicalize to the same event names used by minfriidrett)
    if base.startswith("KULE"):
        return "Kule 7,26kg" if gender == "Men" else "Kule 4,0kg"
    if base.startswith("DISKOS"):
        return "Diskos 2,0kg" if gender == "Men" else "Diskos 1,0kg"
    if base.startswith("SLEGGE"):
        return "Slegge 7,26kg/121,5cm" if gender == "Men" else "Slegge 4,0kg/119,5cm"
    if base.startswith("SPYD"):
        return "Spyd 800gram" if gender == "Men" else "Spyd 600gram"
    if base.startswith("VEKTKAST"):
        return "VektKast 15,88Kg" if gender == "Men" else "VektKast 9,08Kg"
    if base.startswith("SUPERVEKTKAST"):
        return "SuperVektKast 25,4Kg" if gender == "Men" else "SuperVektKast 15,88Kg"

    # Track events: distance, hurdles, steeplechase
    m = re.match(r"^(?P<num>[\d ]+)\s*METER\b", base)
    if m:
        num = int(m.group("num").replace(" ", ""))
        if "HEKK" in base:
            height = _HURDLE_HEIGHT_CM.get((gender, num))
            if height:
                return f"{num} meter hekk ({height}cm)"
            return f"{num} meter hekk"
        if "HINDER" in base:
            height = _STEEPLE_HEIGHT_CM.get((gender, num))
            if height:
                return f"{num} meter hinder ({height}cm)"
            return f"{num} meter hinder"
        return f"{num} meter"

    m = re.match(r"^(?P<num>\d+)\s*MILES?\b", base)
    if m:
        miles = int(m.group("num"))
        return "1 mile" if miles == 1 else f"{miles} miles"

    return None


def _friidrett_athlete_id(*, gender: str, name: str, birth_date: Optional[str]) -> int:
    key = f"friidrett|{gender}|{(name or '').strip().lower()}|{birth_date or ''}"
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    return -1 - int(n)


def _athlete_name_key(name: str) -> str:
    return _norm_cell(name).lower()


def _resolve_event_athlete_id(
    *,
    gender: str,
    athlete_name: str,
    birth_iso: Optional[str],
    known_ids_by_name: dict[str, dict[Optional[str], int]],
) -> Optional[int]:
    """Resolve athlete-id within one event table, reusing IDs when rows omit birth dates."""
    name_key = _athlete_name_key(athlete_name)
    by_birth = known_ids_by_name.get(name_key, {})
    if by_birth:
        if birth_iso in by_birth:
            return int(by_birth[birth_iso])

        known_births = [b for b in by_birth.keys() if b is not None]
        if birth_iso is None:
            if None in by_birth:
                return int(by_birth[None])
            if len(known_births) == 1:
                return int(by_birth[known_births[0]])
            if len(known_births) > 1:
                # Ambiguous: same full name already seen with multiple birth dates in this event.
                return None
        else:
            if None in by_birth and not known_births:
                return int(by_birth[None])

    return _friidrett_athlete_id(gender=gender, name=athlete_name, birth_date=birth_iso)


def _remember_event_athlete_id(
    *,
    athlete_name: str,
    birth_iso: Optional[str],
    athlete_id: int,
    known_ids_by_name: dict[str, dict[Optional[str], int]],
) -> None:
    name_key = _athlete_name_key(athlete_name)
    by_birth = known_ids_by_name.setdefault(name_key, {})
    by_birth.setdefault(birth_iso, int(athlete_id))


def _safe_cache_filename(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    path = re.sub(r"^https?://", "", url)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", path).strip("_").lower()
    slug = slug[:80] if slug else "friidrett"
    return f"{slug}_{digest}.html"


def _norm_cell(text: str) -> str:
    s = (text or "").replace("\u00a0", " ").replace("Ā", " ").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", s)


def _split_name_and_club(text: str) -> tuple[str, Optional[str]]:
    s = _norm_cell(text)
    if not s:
        return ("", None)
    if _looks_like_non_athlete_marker(s):
        return ("", None)
    # Some friidrett.no legacy pages contain placeholder rows where the athlete cell is e.g. "–––"
    # (no actual name). Treat any cell without letters as missing.
    if not any(ch.isalpha() for ch in s):
        return ("", None)
    if "," not in s:
        return (s, None)
    name, rest = s.split(",", 1)
    return (name.strip(), (rest.strip() or None))


def _looks_like_wind(text: str) -> bool:
    s = _norm_cell(text).replace("−", "-").replace("–", "-").replace("—", "-")
    if not s or s == "-":
        return False
    return bool(_WIND_CELL_RE.match(s))


def _parse_wind(text: str) -> Optional[float]:
    s = _norm_cell(text).replace("−", "-").replace("–", "-").replace("—", "-")
    if not s or s == "-":
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _parse_result_date(text: str, *, season: int) -> Optional[str]:
    s = _norm_cell(text).strip().rstrip(".")
    if not s:
        return None

    # Full date dd.mm.yy / dd.mm.yyyy
    full = parse_ddmmyy(s)
    if full:
        return full.isoformat()

    # Date range: 28/29.07 or 25-26.08 (use first day in range)
    m = re.fullmatch(r"(?P<d1>\d{1,2})(?:[/-]\d{1,2})\.(?P<m>\d{1,2})", s)
    if m:
        try:
            return date(int(season), int(m.group("m")), int(m.group("d1"))).isoformat()
        except ValueError:
            return None

    # dd.mm (with or without trailing dot)
    m = re.fullmatch(r"(?P<d>\d{1,2})\.(?P<m>\d{1,2})", s)
    if m:
        try:
            return date(int(season), int(m.group("m")), int(m.group("d"))).isoformat()
        except ValueError:
            return None

    return None


def _none_if_empty(text: str) -> Optional[str]:
    s = _norm_cell(text)
    return s if s else None


def _section_heading_candidate(cells: list[str]) -> Optional[str]:
    non_empty = [c for c in cells if c]
    if not non_empty:
        return None
    # Word-exported pages can have one or two non-empty cells on heading rows.
    if len(non_empty) <= 2 and any(ch.isalpha() for ch in non_empty[0]):
        return non_empty[0]
    return None


def _looks_like_placement(text: str) -> bool:
    s = _norm_cell(text)
    if not s:
        return False
    if _looks_like_wind(s):
        return False
    return bool(_PLACEMENT_CELL_RE.fullmatch(s))


def _parse_birth_date(text: str) -> Optional[str]:
    s = _norm_cell(text)
    if not s:
        return None

    # Legacy pages sometimes use "dd.mm yy" for births.
    s = re.sub(r"^(\d{1,2}\.\d{1,2})\s+(\d{2})$", r"\1.\2", s)
    dt = parse_ddmmyy(s)
    return dt.isoformat() if dt else None


def _clean_venue(text: str) -> Optional[str]:
    s = _none_if_empty(text)
    if not s:
        return None
    s = s.rstrip(",").strip()
    return s or None


def _tables_until_next_h2(h2: html.HtmlElement) -> list[html.HtmlElement]:
    tables: list[html.HtmlElement] = []
    seen: set[int] = set()
    sib = h2.getnext()
    while sib is not None:
        if isinstance(sib.tag, str) and sib.tag.lower() == "h2":
            break
        candidates = [sib] if isinstance(sib.tag, str) and sib.tag.lower() == "table" else sib.xpath(".//table")
        for table in candidates:
            key = id(table)
            if key in seen:
                continue
            seen.add(key)
            tables.append(table)
        sib = sib.getnext()
    return tables


_KAPPGANG_SECTION_RE = re.compile(r"^(?P<label>Menn|Kvinner)\s+(?P<event>.+?)\s*\([^)]*\)\s*$", re.IGNORECASE)
_KAPPGANG_RESULT_RE = re.compile(
    r"^(?P<name>.+?)\s+(?:\(\d+\)\s+)?(?P<birth>\d{6})\s+(?P<club>.+?)\s+"
    r"(?P<perf>\d[\d\.,:]+)\s+(?P<placement>\([^)]*\))\s+(?P<city>.+?)\s+(?P<date>\d{1,2}\.\d{1,2})$"
)


def _parse_kappgang_pdf(*, pdf_bytes: bytes, season: int, gender: str, source_url: str) -> list[ScrapedResult]:
    text = _pdf_to_text(pdf_bytes)
    if not text:
        return []

    out: list[ScrapedResult] = []
    current_event: Optional[str] = None
    rank_by_event: dict[str, int] = defaultdict(int)
    seen_by_event: dict[str, set[int]] = defaultdict(set)

    for raw_line in text.splitlines():
        line = _norm_cell(raw_line)
        if not line:
            continue
        if line.startswith("MiKTeX requires Windows"):
            continue

        sec = _KAPPGANG_SECTION_RE.match(line)
        if sec:
            sec_gender = "Men" if sec.group("label").lower().startswith("menn") else "Women"
            if sec_gender != gender:
                current_event = None
                continue
            current_event = _kappgang_event_no(sec.group("event"))
            continue

        if not current_event:
            continue

        m = _KAPPGANG_RESULT_RE.match(line)
        if not m:
            continue

        cleaned = clean_performance(m.group("perf"))
        if not cleaned or not cleaned.clean or not any(ch.isdigit() for ch in cleaned.clean):
            continue

        birth_dt = _parse_ddmmyy_compact(m.group("birth"))
        birth_iso = birth_dt.isoformat() if birth_dt else None

        athlete_name = _norm_cell(m.group("name"))
        if not athlete_name or not any(ch.isalpha() for ch in athlete_name):
            continue

        athlete_id = _friidrett_athlete_id(gender=gender, name=athlete_name, birth_date=birth_iso)
        if athlete_id in seen_by_event[current_event]:
            continue
        seen_by_event[current_event].add(athlete_id)

        rank_by_event[current_event] += 1
        out.append(
            ScrapedResult(
                season=int(season),
                gender=gender,
                event_no=current_event,
                rank_in_list=int(rank_by_event[current_event]),
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=None,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=_none_if_empty(m.group("club")),
                birth_date=birth_iso,
                placement_raw=_none_if_empty(m.group("placement")),
                venue_city=_none_if_empty(m.group("city")),
                stadium=None,
                competition_id=None,
                competition_name=None,
                result_date=_parse_result_date(m.group("date"), season=season),
                source_url=source_url,
            )
        )

    return out


def _kappgang_event_no(raw_event: str) -> Optional[str]:
    e = _norm_cell(raw_event).lower()
    if not e or "innend" in e:
        return None

    m = re.search(r"(?P<km>\d+)\s*km\b", e)
    if m:
        return f"Kappgang {int(m.group('km'))} km"

    m = re.search(r"(?P<m>\d+)\s*m(?:eter)?\b", e)
    if m:
        return f"Kappgang {int(m.group('m'))} meter"

    return None


def _pdf_to_text(pdf_bytes: bytes) -> Optional[str]:
    pdftotext = shutil.which("pdftotext")
    if not pdftotext:
        return None

    with tempfile.TemporaryDirectory(prefix="nfwa_pdf_") as tmp:
        tmp_dir = Path(tmp)
        in_pdf = tmp_dir / "in.pdf"
        out_txt = tmp_dir / "out.txt"
        in_pdf.write_bytes(pdf_bytes)

        try:
            subprocess.run(
                [pdftotext, "-layout", "-enc", "UTF-8", str(in_pdf), str(out_txt)],
                check=True,
                capture_output=True,
                text=False,
            )
        except Exception:
            return None

        if not out_txt.exists():
            return None
        return out_txt.read_text(encoding="utf-8", errors="replace")


def _parse_ddmmyy_compact(token: str) -> Optional[date]:
    t = _norm_cell(token)
    if not re.fullmatch(r"\d{6}", t):
        return None
    return parse_ddmmyy(f"{t[0:2]}.{t[2:4]}.{t[4:6]}")


def _looks_like_not_found_page(doc: html.HtmlElement) -> bool:
    title = _norm_cell(" ".join(doc.xpath("//title/text()"))).lower()
    if "vi fant ikke siden" in title:
        return True
    body = _norm_cell(doc.text_content()).lower()
    if "microsoftonline.com" in body and "oauth2/authorize" in body:
        return True
    return False
