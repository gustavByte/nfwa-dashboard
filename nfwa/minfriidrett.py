from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import requests
from lxml import html

from .config import MINFRIIDRETT_BASE_URL
from .util import clean_performance, parse_ddmmyy


_ATHLETE_ID_RE = re.compile(r"[?&]showathl=(?P<id>\d+)\b")
_COMP_ID_RE = re.compile(r"posttoresultlist\((?P<id>\d+)\)")


@dataclass(frozen=True)
class ScrapedResult:
    season: int
    gender: str  # "Women" | "Men"
    event_no: str
    rank_in_list: int
    performance_raw: str
    performance_clean: Optional[str]
    wind: Optional[float]
    athlete_id: int
    athlete_name: str
    club_name: Optional[str]
    birth_date: Optional[str]  # ISO YYYY-MM-DD
    nationality: Optional[str]  # ISO 3166-1 alpha-3, e.g. "NOR", "ETH"; None = default NOR
    placement_raw: Optional[str]
    venue_city: Optional[str]
    stadium: Optional[str]
    competition_id: Optional[int]
    competition_name: Optional[str]
    result_date: Optional[str]  # ISO YYYY-MM-DD
    source_url: str


def build_landsstatistikk_url(*, showclass: int, season: int, outdoor: bool = True, showevent: int = 0, showclub: int = 0) -> str:
    outdoor_flag = "Y" if outdoor else "N"
    return (
        f"{MINFRIIDRETT_BASE_URL}"
        f"?showclass={showclass}&showevent={showevent}&outdoor={outdoor_flag}&showseason={season}&showclub={showclub}"
    )


def fetch_landsstatistikk(
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


def parse_landsstatistikk(*, html_bytes: bytes, season: int, gender: str, source_url: str) -> Iterable[ScrapedResult]:
    doc = html.fromstring(html_bytes)
    # The site uses a Norwegian id="øvelse". Use explicit escape to avoid any encoding surprises.
    event_divs = doc.xpath("//div[@id='\u00f8velse']")
    for div in event_divs:
        event_name = div.xpath("string(.//h4)").strip()
        if not event_name:
            # The site includes multiple non-event sections inside the same page (e.g. foreign citizens),
            # but those are duplicates/side-lists and not per-event ranking tables.
            continue
        table = div.xpath(".//table[1]")
        if not table:
            continue
        rows = table[0].xpath(".//tr")[1:]  # skip header
        rank = 0
        result_count = 0
        prev_clean: Optional[str] = None
        for tr in rows:
            cells = tr.xpath("./td")
            if len(cells) < 6:
                continue
            perf_raw = cells[0].text_content().strip()
            cleaned = clean_performance(perf_raw)
            if not cleaned:
                continue
            if cleaned.clean == "-----":
                continue

            athlete_td = cells[1]
            athlete_link = athlete_td.xpath(".//a")
            if not athlete_link:
                continue
            athlete_link = athlete_link[0]
            athlete_name = athlete_link.text_content().strip()
            athlete_href = athlete_link.get("href") or ""
            athlete_id = _parse_int(_ATHLETE_ID_RE.search(athlete_href))
            if athlete_id is None:
                continue

            club_name = "".join(athlete_td.xpath("text()")).strip()
            if club_name.startswith(","):
                club_name = club_name[1:].strip()
            club_name = club_name or None

            birth = parse_ddmmyy(cells[2].text_content().strip())
            birth_iso = birth.isoformat() if birth else None

            placement = cells[3].text_content().strip() or None

            venue_td = cells[4]
            stadium = (venue_td.get("title") or "").strip() or None
            city_text = "".join(venue_td.xpath("text()")).strip()
            if city_text.endswith(","):
                city_text = city_text[:-1].strip()
            venue_city = city_text or None

            comp_id: Optional[int] = None
            comp_name: Optional[str] = None
            comp_link = venue_td.xpath(".//a")
            if comp_link:
                comp_link = comp_link[0]
                comp_name = comp_link.text_content().strip() or None
                comp_href = comp_link.get("href") or ""
                comp_id = _parse_int(_COMP_ID_RE.search(comp_href))

            result_date = parse_ddmmyy(cells[5].text_content().strip())
            result_iso = result_date.isoformat() if result_date else None

            # Competition-style ranking: tied performances share the same rank
            result_count += 1
            if cleaned.clean != prev_clean:
                rank = result_count
                prev_clean = cleaned.clean

            yield ScrapedResult(
                season=season,
                gender=gender,
                event_no=event_name,
                rank_in_list=rank,
                performance_raw=cleaned.raw,
                performance_clean=cleaned.clean,
                wind=cleaned.wind,
                athlete_id=athlete_id,
                athlete_name=athlete_name,
                club_name=club_name,
                birth_date=birth_iso,
                nationality=None,  # minfriidrett does not provide nationality; defaults to NOR
                placement_raw=placement,
                venue_city=venue_city,
                stadium=stadium,
                competition_id=comp_id,
                competition_name=comp_name,
                result_date=result_iso,
                source_url=source_url,
            )


def _parse_int(match: Optional[re.Match[str]]) -> Optional[int]:
    if not match:
        return None
    try:
        return int(match.group("id"))
    except (ValueError, IndexError):
        return None


def _safe_cache_filename(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    showclass = re.search(r"[?&]showclass=(\d+)\b", url)
    season = re.search(r"[?&]showseason=(\d+)\b", url)
    parts = ["lands"]
    if showclass:
        parts.append(f"c{showclass.group(1)}")
    if season:
        parts.append(f"s{season.group(1)}")
    parts.append(digest)
    return "_".join(parts) + ".html"
