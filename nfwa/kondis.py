from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

import requests
from lxml import html

from .util import clean_performance


_MONTHS = {
    # Norwegian
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "okt": 10,
    "nov": 11,
    "des": 12,
    # English (seen on some pages)
    "may": 5,
    "oct": 10,
    "dec": 12,
}


@dataclass(frozen=True)
class KondisPage:
    season: int
    gender: str  # "Women" | "Men"
    event_no: str
    url: str
    enabled: bool = True


@dataclass(frozen=True)
class KondisResult:
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
    birth_date: Optional[str]  # ISO YYYY-MM-DD (year-only if inferred)
    placement_raw: Optional[str]
    venue_city: Optional[str]
    stadium: Optional[str]
    competition_id: Optional[int]
    competition_name: Optional[str]
    result_date: Optional[str]  # ISO YYYY-MM-DD
    source_url: str


def _pages_from_season_url_pairs(
    *,
    gender: str,
    event_no: str,
    season_url_pairs: Iterable[tuple[int, str]],
) -> tuple[KondisPage, ...]:
    return tuple(
        KondisPage(season=int(season), gender=gender, event_no=event_no, url=url)
        for season, url in season_url_pairs
    )


_FIVE_KM_WOMEN_LEGACY_URLS: tuple[tuple[int, str], ...] = (
    (2010, "https://www.kondis.no/a/4769524"),
    (2009, "https://www.kondis.no/a/4628497"),
    (2008, "https://www.kondis.no/a/4628499"),
    (2007, "https://www.kondis.no/a/4628502"),
    (2006, "https://www.kondis.no/a/4628503"),
    (2005, "https://www.kondis.no/a/4628504"),
)


_MARATON_WOMEN_LEGACY_URLS: tuple[tuple[int, str], ...] = (
    (2010, "https://www.kondis.no/a/4750660"),
    (2009, "https://www.kondis.no/a/4627922"),
    (2008, "https://www.kondis.no/a/4627941"),
    (2007, "https://www.kondis.no/a/4627942"),
    (2006, "https://www.kondis.no/a/4627943"),
    (2005, "https://www.kondis.no/a/4627944"),
    (2004, "https://www.kondis.no/a/4627945"),
    (2003, "https://www.kondis.no/a/4627946"),
    (2002, "https://www.kondis.no/a/4627947"),
    (2001, "https://www.kondis.no/a/4627948"),
    (2000, "https://www.kondis.no/a/4627949"),
    (1999, "https://www.kondis.no/a/4627950"),
    (1998, "https://www.kondis.no/a/4627951"),
    (1997, "https://www.kondis.no/a/4627952"),
)


_HALVMARATON_WOMEN_LEGACY_URLS: tuple[tuple[int, str], ...] = (
    (2010, "https://www.kondis.no/a/4760450"),
    (2009, "https://www.kondis.no/a/4627953"),
    (2008, "https://www.kondis.no/a/4627954"),
    (2007, "https://www.kondis.no/a/4627955"),
    (2006, "https://www.kondis.no/a/4627956"),
    (2005, "https://www.kondis.no/a/4627957"),
    (2004, "https://www.kondis.no/a/4627958"),
    (2003, "https://www.kondis.no/a/4627959"),
    (2002, "https://www.kondis.no/a/4627960"),
    (2001, "https://www.kondis.no/a/4627962"),
    (2000, "https://www.kondis.no/a/4627964"),
    (1999, "https://www.kondis.no/a/4627965"),
    (1998, "https://www.kondis.no/a/4627967"),
    (1997, "https://www.kondis.no/a/4627968"),
)


_MARATON_MEN_LEGACY_URLS: tuple[tuple[int, str], ...] = (
    (2010, "https://www.kondis.no/a/4750579"),
    (2009, "https://www.kondis.no/a/4627318"),
    (2008, "https://www.kondis.no/a/4627736"),
    (2007, "https://www.kondis.no/a/4627737"),
    (2006, "https://www.kondis.no/a/4627738"),
    (2005, "https://www.kondis.no/a/4627740"),
    (2004, "https://www.kondis.no/a/4627742"),
    (2003, "https://www.kondis.no/a/4627743"),
    (2002, "https://www.kondis.no/a/4627744"),
    (2001, "https://www.kondis.no/a/4627753"),
    (2000, "https://www.kondis.no/a/4627760"),
    (1999, "https://www.kondis.no/a/4627761"),
    (1998, "https://www.kondis.no/a/4627762"),
    (1997, "https://www.kondis.no/a/4627763"),
)


KONDIS_PAGES: tuple[KondisPage, ...] = (
    # 5 km (Women)
    KondisPage(season=2025, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2025-5-km-kvinner/469715"),
    KondisPage(season=2024, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2024-5-km-kvinner/469761"),
    KondisPage(season=2023, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2023-5-km-kvinner/469406"),
    KondisPage(season=2022, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2022-5-km-kvinner/469196"),
    KondisPage(season=2021, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2021-5-km-kvinner/469308"),
    KondisPage(season=2020, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2020-5-km-kvinner/469549"),
    KondisPage(season=2019, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2019-5-km-kvinner/1530239"),
    KondisPage(season=2018, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2018-5-km-kvinner/1530190"),
    KondisPage(season=2017, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2017-5-km-kvinner/1530458"),
    KondisPage(season=2016, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2016-5-km-kvinner/1530764"),
    # No correct source page found for 2015 women's 5 km.
    KondisPage(season=2014, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-5-km-kvinner/1529705"),
    KondisPage(season=2013, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-5-km-kvinner/1530122"),
    KondisPage(season=2012, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-5-km-kvinner/1530020"),
    KondisPage(season=2011, gender="Women", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-5-km-kvinner/1530223"),
    *_pages_from_season_url_pairs(
        gender="Women",
        event_no="5 km gateløp",
        season_url_pairs=_FIVE_KM_WOMEN_LEGACY_URLS,
    ),
    # 5 km (Men)
    KondisPage(season=2025, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2025-5-km-menn/469161"),
    KondisPage(season=2024, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2024-5-km-menn/469056"),
    KondisPage(season=2023, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2023-5-km-menn/469195"),
    KondisPage(season=2022, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2022-5-km-menn/469435"),
    KondisPage(season=2021, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2021-5-km-menn/469099"),
    KondisPage(season=2020, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2020-5-km-menn/469718"),
    KondisPage(season=2019, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2019-5-km-menn/1530325"),
    KondisPage(season=2018, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2018-5-km-menn/1530960"),
    KondisPage(season=2017, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2017-5-km-menn/1529840"),
    KondisPage(season=2016, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/de-beste-2016-5-km-menn/1529481"),
    KondisPage(season=2015, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2015-5-km-menn/1529534"),
    KondisPage(season=2014, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-5-km-menn/1530301"),
    KondisPage(season=2013, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-5-km-menn/1529659"),
    KondisPage(season=2012, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-5-km-menn/1529910"),
    KondisPage(season=2011, gender="Men", event_no="5 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-5-km-menn/1530603"),
    # 10 km (Women)
    KondisPage(season=2025, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2025-10-km-kvinner/469281"),
    KondisPage(season=2024, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2024-10-km-kvinner/469743"),
    KondisPage(season=2023, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2023-10-km-kvinner/469252"),
    KondisPage(season=2022, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2022-10-km-kvinner/469414"),
    KondisPage(season=2021, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2021-10-km-kvinner/469372"),
    KondisPage(season=2020, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2020-10-km-kvinner/469037"),
    KondisPage(season=2019, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2019-10-km-kvinner/1529842"),
    KondisPage(season=2018, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2018-10-km-kvinner/1530884"),
    KondisPage(season=2017, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2017-10-km-kvinner/1530320"),
    KondisPage(season=2016, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2016-10-km-kvinner/1529621"),
    KondisPage(season=2015, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2015-10-km-kvinner/1530945"),
    KondisPage(season=2014, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-10-km-kvinner/1529956"),
    KondisPage(season=2013, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-10-km-kvinner/1530716"),
    KondisPage(season=2012, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-10-km-kvinner/1530617"),
    KondisPage(season=2011, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-10-km-kvinner/1530169"),
    KondisPage(season=2010, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4800243"),
    KondisPage(season=2009, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628387"),
    KondisPage(season=2008, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628484"),
    KondisPage(season=2007, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628485"),
    KondisPage(season=2006, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628486"),
    KondisPage(season=2005, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628487"),
    KondisPage(season=2004, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628489"),
    KondisPage(season=2003, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628490"),
    KondisPage(season=2002, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628491"),
    KondisPage(season=2001, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628492"),
    KondisPage(season=2000, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628493"),
    KondisPage(season=1999, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628494"),
    KondisPage(season=1998, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628495"),
    KondisPage(season=1997, gender="Women", event_no="10 km gateløp", url="https://www.kondis.no/a/4628496"),
    # 10 km (Men)
    KondisPage(season=2025, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2025-10-km-menn/469622"),
    KondisPage(season=2024, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2024-10-km-menn/469602"),
    KondisPage(season=2023, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2023-10-km-menn/469696"),
    KondisPage(season=2022, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2022-10-km-menn/469546"),
    KondisPage(season=2021, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2021-10-km-menn/469670"),
    KondisPage(season=2020, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/de-beste-2020-10-km-menn/469156"),
    KondisPage(season=2019, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2019-10-km-menn/1530487"),
    KondisPage(season=2018, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2018-10-km-menn/1530891"),
    KondisPage(season=2017, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2017-10-km-menn/1530089"),
    KondisPage(season=2016, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/de-beste-2016-10-km-menn/1529822"),
    KondisPage(season=2015, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2015-10-km-menn/1530203"),
    # No correct source page found for 2014 men's 10 km.
    KondisPage(season=2013, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-10-km-menn/1530855"),
    KondisPage(season=2012, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-10-km-menn/1529446"),
    KondisPage(season=2011, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-10-km-menn/1529468"),
    KondisPage(season=2010, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4800244"),
    KondisPage(season=2009, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627781"),
    KondisPage(season=2008, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627783"),
    KondisPage(season=2007, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627784"),
    KondisPage(season=2006, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627785"),
    KondisPage(season=2005, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627786"),
    KondisPage(season=2004, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627787"),
    KondisPage(season=2003, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627788"),
    KondisPage(season=2002, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627789"),
    KondisPage(season=2001, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627790"),
    KondisPage(season=2000, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627791"),
    KondisPage(season=1999, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627792"),
    KondisPage(season=1998, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627793"),
    KondisPage(season=1997, gender="Men", event_no="10 km gateløp", url="https://www.kondis.no/a/4627794"),
    # Half marathon (Women)
    KondisPage(season=2025, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2025-halvmaraton-kvinner/469010"),
    KondisPage(season=2024, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2024-halvmaraton-kvinner/469698"),
    KondisPage(season=2023, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2023-halvmaraton-kvinner/469012"),
    KondisPage(season=2022, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2022-halvmaraton-kvinner/469443"),
    KondisPage(season=2021, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2021-halvmaraton-kvinner/469109"),
    KondisPage(season=2020, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2020-halvmaraton-kvinner/469203"),
    KondisPage(season=2019, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2019-halvmaraton-kvinner/1530369"),
    KondisPage(season=2018, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2018-halvmaraton-kvinner/1530757"),
    KondisPage(season=2017, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2017-halvmaraton-kvinner/1530538"),
    KondisPage(season=2016, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2016-halvmaraton-kvinner/1530055"),
    KondisPage(season=2015, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2015-halvmaraton-kvinner/1529650"),
    KondisPage(season=2014, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-halvmaraton-kvinner/1530255"),
    KondisPage(season=2013, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-halvmaraton-kvinner/1530037"),
    KondisPage(season=2012, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-halvmaraton-kvinner/1529698"),
    KondisPage(season=2011, gender="Women", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-halvmaraton-kvinner/1529392"),
    *_pages_from_season_url_pairs(
        gender="Women",
        event_no="Halvmaraton",
        season_url_pairs=_HALVMARATON_WOMEN_LEGACY_URLS,
    ),
    # Half marathon (Men)
    KondisPage(season=2025, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2025-halvmaraton-menn/469692"),
    KondisPage(season=2024, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2024-halvmaraton-menn/469467"),
    KondisPage(season=2023, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2023-halvmaraton-menn/469245"),
    KondisPage(season=2022, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2022-halvmaraton-menn/469760"),
    KondisPage(season=2021, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2021-halvmaraton-menn/469188"),
    KondisPage(season=2020, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/de-beste-2020-halvmaraton-menn/469429"),
    KondisPage(season=2019, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2019-halvmaraton-menn/1529640"),
    KondisPage(season=2018, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2018-halvmaraton-menn/1530408"),
    # No correct source page found for 2017 men's half marathon. The historical URL used here was wrong and produced
    # unrelated results (e.g. ~10–20 min times). Keep it disabled to avoid ingesting bad data while still allowing
    # sync to purge any previously ingested rows for that URL.
    KondisPage(season=2017, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/index.php?id=5947377", enabled=False),
    KondisPage(season=2016, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/de-beste-2016-halvmaraton-menn/1529724"),
    # No correct source page found for 2015 men's half marathon.
    KondisPage(season=2014, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-halvmaraton-menn/1530327"),
    KondisPage(season=2013, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-halvmaraton-menn/1530936"),
    KondisPage(season=2012, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-halvmaraton-menn/1530956"),
    KondisPage(season=2011, gender="Men", event_no="Halvmaraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-halvmaraton-menn/1530673"),
    # Marathon (Women)
    KondisPage(season=2025, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2025-maraton-kvinner/469517"),
    KondisPage(season=2024, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2024-maraton-kvinner/469539"),
    KondisPage(season=2023, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2023-maraton-kvinner/469199"),
    KondisPage(season=2022, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2022-maraton-kvinner/469311"),
    KondisPage(season=2021, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2021-maraton-kvinner/469704"),
    KondisPage(season=2020, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2020-maraton-kvinner/469438"),
    KondisPage(season=2019, gender="Women", event_no="Maraton", url="https://www.kondis.no/de-beste-2019-maraton-kvinner/1530231"),
    KondisPage(season=2018, gender="Women", event_no="Maraton", url="https://www.kondis.no/de-beste-2018-maraton-kvinner/1530062"),
    KondisPage(season=2017, gender="Women", event_no="Maraton", url="https://www.kondis.no/de-beste-2017-maraton-kvinner/1530701"),
    KondisPage(season=2016, gender="Women", event_no="Maraton", url="https://www.kondis.no/de-beste-2016-maraton-kvinner/1529960"),
    # No correct source page found for 2015 women's marathon.
    KondisPage(season=2014, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-maraton-kvinner/1530975"),
    KondisPage(season=2013, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-maraton-kvinner/1530211"),
    KondisPage(season=2012, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-maraton-kvinner/1530329"),
    KondisPage(season=2011, gender="Women", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-maraton-kvinner/1529618"),
    *_pages_from_season_url_pairs(
        gender="Women",
        event_no="Maraton",
        season_url_pairs=_MARATON_WOMEN_LEGACY_URLS,
    ),
    # Marathon (Men)
    KondisPage(season=2025, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2025-maraton-menn/469051"),
    KondisPage(season=2024, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2024-maraton-menn/469657"),
    KondisPage(season=2023, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2023-maraton-menn/469687"),
    KondisPage(season=2022, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2022-maraton-menn/469694"),
    KondisPage(season=2021, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2021-maraton-menn/469604"),
    KondisPage(season=2020, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/de-beste-2020-maraton-menn/469589"),
    KondisPage(season=2019, gender="Men", event_no="Maraton", url="https://www.kondis.no/de-beste-2019-maraton-menn/1529965"),
    KondisPage(season=2018, gender="Men", event_no="Maraton", url="https://www.kondis.no/de-beste-2018-maraton-menn/1529589"),
    KondisPage(season=2017, gender="Men", event_no="Maraton", url="https://www.kondis.no/de-beste-2017-maraton-menn/1530110"),
    KondisPage(season=2016, gender="Men", event_no="Maraton", url="https://www.kondis.no/de-beste-2016-maraton-menn/1529759"),
    KondisPage(season=2015, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2015-maraton-menn/1530248"),
    KondisPage(season=2014, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2014-maraton-menn/1530640"),
    KondisPage(season=2013, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2013-maraton-menn/1530278"),
    KondisPage(season=2012, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2012-maraton-menn/1530778"),
    KondisPage(season=2011, gender="Men", event_no="Maraton", url="https://www.kondis.no/statistikk/norgesstatistikk-2011-maraton-menn/1530305"),
    *_pages_from_season_url_pairs(
        gender="Men",
        event_no="Maraton",
        season_url_pairs=_MARATON_MEN_LEGACY_URLS,
    ),
)


def pages_for_years(*, years: Iterable[int], gender: str) -> list[KondisPage]:
    ys = {int(y) for y in years}
    genders = {"Women", "Men"} if gender == "Both" else {gender}
    return [p for p in KONDIS_PAGES if p.season in ys and p.gender in genders]


def fetch_kondis_stats(
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


def parse_kondis_stats(*, html_bytes: bytes, page: KondisPage) -> Iterable[KondisResult]:
    doc = html.fromstring(html_bytes)
    out = _parse_kondis_stats_table(doc=doc, page=page)
    if out:
        return out
    out = _parse_kondis_stats_pre(doc=doc, page=page)
    if out:
        return out
    return _parse_kondis_stats_text(doc=doc, page=page)


def _parse_kondis_stats_table(*, doc: html.HtmlElement, page: KondisPage) -> list[KondisResult]:
    tables = doc.xpath("//table")
    if not tables:
        return []

    table = _pick_best_table(tables)
    if table is None:
        return []

    rows = table.xpath(".//tr")
    auto_rank = 0
    out: list[KondisResult] = []

    for tr in rows:
        cells = [c.text_content().strip() for c in tr.xpath("./th|./td")]
        if not cells:
            continue
        if all(not (c or "").strip() for c in cells):
            continue

        # Skip obvious header-ish rows if they ever appear.
        if any(c.lower() in {"navn", "name", "tid", "time"} for c in cells):
            continue

        auto_rank += 1

        time_first = _looks_like_time(cells[0])
        rank_in_list: int

        athlete_cell = ""
        time_cell = ""
        placement_raw: Optional[str] = None
        competition_name: Optional[str] = None
        venue_city: Optional[str] = None
        date_cell: Optional[str] = None

        if not time_first:
            # Legacy Kondis tables (esp. women 5 km ~2005-2009) use:
            # "1 Name" | "17.52" | "Race name"
            # Handle this shape explicitly so name/time don't get swapped.
            m_rank_athlete = _RANK_PREFIX_RE.match(cells[0]) if cells else None
            if (
                len(cells) >= 3
                and _looks_like_time(cells[1])
                and not _looks_like_time(cells[2])
            ):
                if m_rank_athlete:
                    rank_in_list = _parse_rank_token(m_rank_athlete.group("rank")) or auto_rank
                    athlete_cell = m_rank_athlete.group("rest").strip()
                else:
                    # Some legacy rows omit explicit rank and are shaped:
                    # "Name" | "17.52" | "Race name"
                    rank_in_list = auto_rank
                    athlete_cell = cells[0]
                time_cell = cells[1]
                competition_name = _none_if_empty(cells[2])
                if len(cells) > 3:
                    date_cell = _none_if_empty(cells[3])
            else:
                rank_in_list = _parse_rank_token(cells[0]) or auto_rank

                # Scan for the actual time column — some wider legacy tables
                # (e.g. 2003, 2005 half marathon) have club/birth/venue columns
                # between the name and the result.
                time_idx: Optional[int] = None
                for idx in range(2, len(cells)):
                    if _looks_like_time(cells[idx]):
                        time_idx = idx
                        break

                if time_idx is not None and time_idx > 2:
                    # Wide table: rank | name [| club [| birth [| venue]]] | time …
                    pre_time = cells[1:time_idx]

                    if len(pre_time) >= 2:
                        # Last cell before the time is typically venue/sted.
                        venue_city = _none_if_empty(pre_time[-1])
                        athlete_parts = pre_time[:-1]
                    else:
                        athlete_parts = pre_time

                    # Join athlete parts: "Name, Club BirthYear".
                    # Birth-year cells start with "-" (e.g. "-68").
                    pieces: list[str] = []
                    for p in athlete_parts:
                        p_s = p.strip()
                        if not p_s:
                            continue
                        if re.match(r"^-\d{2,4}", p_s.replace("(*)", "").strip()):
                            if pieces:
                                pieces[-1] += " " + p_s
                            else:
                                pieces.append(p_s)
                        else:
                            pieces.append(p_s)
                    athlete_cell = ", ".join(pieces)

                    # Extract only the first time token (cell may contain PR info).
                    raw_time = cells[time_idx]
                    tm = _TIME_TOKEN_RE.match(raw_time)
                    time_cell = tm.group("time") if tm else raw_time
                else:
                    athlete_cell = cells[1] if len(cells) > 1 else ""
                    time_cell = cells[2] if len(cells) > 2 else ""
                    if len(cells) > 3:
                        competition_name = _none_if_empty(cells[3])
                    if len(cells) > 4:
                        date_cell = _none_if_empty(cells[4])
        else:
            rank_in_list = auto_rank
            time_cell = cells[0]
            athlete_cell = cells[1] if len(cells) > 1 else ""
            if len(cells) == 4:
                venue_city = _none_if_empty(cells[2])
                date_cell = _none_if_empty(cells[3])
            elif len(cells) >= 5:
                if _parse_int(cells[2]) is not None:
                    placement_raw = _none_if_empty(cells[2])
                    venue_city = _none_if_empty(cells[3])
                    date_cell = _none_if_empty(cells[4])
                else:
                    competition_name = _none_if_empty(cells[2])
                    venue_city = _none_if_empty(cells[3])
                    date_cell = _none_if_empty(cells[4])

        row = _build_kondis_result(
            page=page,
            rank_in_list=rank_in_list,
            athlete_cell=athlete_cell,
            time_cell=time_cell,
            placement_raw=placement_raw,
            competition_name=competition_name,
            venue_city=venue_city,
            date_cell=date_cell,
        )
        if row is not None:
            out.append(row)

    return out


_TIME_TOKEN_RE = re.compile(r"(?P<time>\d+(?:(?:[:.,])\d{2}){1,3}(?:[A-Za-z]{1,3})?)")
_DATE_TOKEN_RE = re.compile(
    r"(?P<day>\d{1,2})\s*[.,]\s*(?P<mon>[A-Za-z\u00c6\u00d8\u00c5\u00e6\u00f8\u00e5]{3,4})\b"
)
_BIRTH_MARKER_RE = re.compile(r"-(?P<y>\d{2,4}|\?)\b")
_BIRTH_AT_END_RE = re.compile(r"-(?:\d{2,4}|\?)\s*$")
_RANK_PREFIX_RE = re.compile(r"^(?P<rank>\d{1,4}(?:[.,]\d)?\.?)\s+(?P<rest>[A-Z\u00c6\u00d8\u00c5].+)$")
_PRE_RANK_MARKER_RE = re.compile(r"(?<![\d.,:])(?P<rank>\d{1,4})(?:[.)])?\s+(?=[A-Z\u00c6\u00d8\u00c5])")
_PRE_STOP_MARKERS = (
    "andre under",
    "utarbeidet av",
    "basert på tilgjengelige opplysninger",
    "oppdatert",
)


def _parse_kondis_stats_pre(*, doc: html.HtmlElement, page: KondisPage) -> list[KondisResult]:
    best: list[KondisResult] = []

    for pre in doc.xpath("//pre"):
        text = re.sub(r"\s+", " ", (pre.text_content() or "").replace("\u00a0", " ")).strip()
        if not text:
            continue

        text = _truncate_pre_text(text)
        entries = _split_pre_entries(text)
        if len(entries) < 3:
            continue

        rows: list[KondisResult] = []
        for entry in entries:
            row = _parse_pre_entry(entry=entry, page=page, rank_in_list=len(rows) + 1)
            if row is not None:
                rows.append(row)

        if len(rows) > len(best):
            best = rows

    return best


def _truncate_pre_text(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s

    s_low = s.lower()
    cut_at: Optional[int] = None
    for marker in _PRE_STOP_MARKERS:
        idx = s_low.find(marker)
        if idx < 0:
            continue
        cut_at = idx if cut_at is None else min(cut_at, idx)
    if cut_at is not None:
        s = s[:cut_at]

    return s.strip(" -\u2013")


def _split_pre_entries(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []

    marks = list(_PRE_RANK_MARKER_RE.finditer(s))
    if len(marks) < 2:
        return []

    out: list[str] = []
    for i, mark in enumerate(marks):
        start = mark.start()
        end = marks[i + 1].start() if i + 1 < len(marks) else len(s)
        chunk = s[start:end].strip(" ,;|-")
        if not chunk:
            continue
        chunk = _PRE_RANK_MARKER_RE.sub("", chunk, count=1).strip(" ,;|-")
        if chunk:
            out.append(chunk)

    return out


def _parse_pre_entry(*, entry: str, page: KondisPage, rank_in_list: int) -> KondisResult | None:
    s = (entry or "").strip()
    if not s:
        return None

    tm = _TIME_TOKEN_RE.search(s)
    if not tm:
        return None

    athlete_cell = s[: tm.start()].strip(" ,;|-")
    time_cell = tm.group("time").strip()
    after = s[tm.end() :].strip(" ,;|-")
    if not athlete_cell:
        return None

    return _build_kondis_result(
        page=page,
        rank_in_list=rank_in_list,
        athlete_cell=athlete_cell,
        time_cell=time_cell,
        placement_raw=None,
        competition_name=after or None,
        venue_city=None,
        date_cell=None,
    )


def _parse_kondis_stats_text(*, doc: html.HtmlElement, page: KondisPage) -> list[KondisResult]:
    # Older Kondis pages (esp. ~2017-2019) can have results as plain text lines
    # instead of proper HTML tables.
    text = (doc.text_content() or "").replace("\u00a0", " ")
    raw_lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in raw_lines if ln]

    out: list[KondisResult] = []
    auto_rank = 0
    i = 0
    while i < len(lines):
        line = lines[i]

        # Skip "page breaks" like "10", "20" in some lists.
        if re.fullmatch(r"\d{1,4}", line):
            i += 1
            continue

        # Some rows are split over two lines (e.g. athlete on one line, venue/date on the next).
        if _starts_with_time_token(line) and _BIRTH_AT_END_RE.search(line) and not _DATE_TOKEN_RE.search(line):
            if i + 1 < len(lines):
                nxt = lines[i + 1]
                if nxt and not _starts_with_rank_or_time(nxt) and not re.fullmatch(r"\d{1,4}", nxt):
                    line = f"{line} {nxt}"
                    i += 1

        parsed, auto_rank = _parse_kondis_text_line(line=line, page=page, auto_rank=auto_rank)
        if parsed is not None:
            out.append(parsed)
            auto_rank = max(auto_rank, int(parsed.rank_in_list))
        i += 1

    return out


def _parse_kondis_text_line(*, line: str, page: KondisPage, auto_rank: int) -> tuple[KondisResult | None, int]:
    if "|" in line:
        return _parse_kondis_text_line_pipes(line=line, page=page, auto_rank=auto_rank)
    return _parse_kondis_text_line_spaces(line=line, page=page, auto_rank=auto_rank)


def _parse_kondis_text_line_pipes(*, line: str, page: KondisPage, auto_rank: int) -> tuple[KondisResult | None, int]:
    parts = [p.strip() for p in line.split("|")]
    parts = [p for p in parts if p]
    if len(parts) < 2:
        return (None, auto_rank)

    # Rank-first: "1 | Name -YY | 16.39 | Fornebuløpet"
    rank_i = _parse_int(parts[0])
    if rank_i is not None and len(parts) >= 3 and _TIME_TOKEN_RE.search(parts[2]):
        rank_in_list = int(rank_i)
        athlete_cell = parts[1]
        time_cell = parts[2]
        rest_parts = parts[3:]
        result_date, rest_text = _extract_date_and_rest(" ".join(rest_parts), season=page.season)
        row = _build_kondis_result(
            page=page,
            rank_in_list=rank_in_list,
            athlete_cell=athlete_cell,
            time_cell=time_cell,
            placement_raw=None,
            competition_name=rest_text or None,
            venue_city=None,
            date_cell=result_date,
        )
        return (row, auto_rank)

    # Time-first: "2.05.48r | Name -YY | Fukuoka, JPN | 03.des"
    if not _TIME_TOKEN_RE.match(parts[0]):
        return (None, auto_rank)

    auto_rank += 1
    rank_in_list = auto_rank
    time_cell = parts[0]
    athlete_cell = parts[1] if len(parts) > 1 else ""
    rest = " ".join(parts[2:]).strip()
    result_date, rest_text = _extract_date_and_rest(rest, season=page.season)

    row = _build_kondis_result(
        page=page,
        rank_in_list=rank_in_list,
        athlete_cell=athlete_cell,
        time_cell=time_cell,
        placement_raw=None,
        competition_name=None,
        venue_city=rest_text or None,
        date_cell=result_date,
    )
    return (row, auto_rank)


def _parse_kondis_text_line_spaces(*, line: str, page: KondisPage, auto_rank: int) -> tuple[KondisResult | None, int]:
    s = (line or "").strip()
    if not s:
        return (None, auto_rank)

    # Rank-first: "1 Name, Club -YY 16.46 Race name" (also accepts "1. Name ...")
    m_rank = _RANK_PREFIX_RE.match(s)
    if m_rank and not _starts_with_time_token(s):
        rank_token = m_rank.group("rank")
        rank_in_list = _parse_rank_token(rank_token) or (auto_rank + 1)
        rest = m_rank.group("rest").strip()
        tm = _TIME_TOKEN_RE.search(rest)
        if not tm:
            return (None, auto_rank)
        athlete_cell = rest[: tm.start()].strip()
        time_cell = tm.group("time").strip()
        after = rest[tm.end() :].strip()

        result_date, after_wo_date = _extract_date_and_rest(after, season=page.season)

        row = _build_kondis_result(
            page=page,
            rank_in_list=rank_in_list,
            athlete_cell=athlete_cell,
            time_cell=time_cell,
            placement_raw=None,
            competition_name=after_wo_date or None,
            venue_city=None,
            date_cell=result_date,
        )
        return (row, auto_rank)

    # Time-first: "59.48r Name, Club -YY Valencia, ESP 22.okt"
    tm0 = _TIME_TOKEN_RE.match(s)
    if not tm0:
        return (None, auto_rank)

    auto_rank += 1
    rank_in_list = auto_rank

    time_cell = tm0.group("time").strip()
    rest = s[tm0.end() :].strip()

    result_date, rest_wo_date = _extract_date_and_rest(rest, season=page.season)
    athlete_cell, venue = _split_time_first_athlete_and_venue(rest_wo_date)

    row = _build_kondis_result(
        page=page,
        rank_in_list=rank_in_list,
        athlete_cell=athlete_cell,
        time_cell=time_cell,
        placement_raw=None,
        competition_name=None,
        venue_city=venue or None,
        date_cell=result_date,
    )
    return (row, auto_rank)


def _pick_best_table(tables: list[html.HtmlElement]) -> html.HtmlElement | None:
    best = None
    best_score = 0
    for t in tables:
        score = 0
        for tr in t.xpath(".//tr"):
            cells = [c.text_content().strip() for c in tr.xpath("./th|./td")]
            if any(_looks_like_time(c) for c in cells):
                score += 1
        if score > best_score:
            best_score = score
            best = t
    # Require at least a few time-like rows to avoid layout/navigation tables.
    return best if best_score >= 3 else None


def _build_kondis_result(
    *,
    page: KondisPage,
    rank_in_list: int,
    athlete_cell: str,
    time_cell: str,
    placement_raw: Optional[str],
    competition_name: Optional[str],
    venue_city: Optional[str],
    date_cell: Optional[str],
) -> KondisResult | None:
    perf = clean_performance(time_cell)
    if not perf or not perf.clean:
        return None

    athlete_name, club_name, birth_year = _parse_athlete_cell(athlete_cell)
    if not athlete_name:
        return None

    athlete_id = _kondis_athlete_id(gender=page.gender, name=athlete_name, birth_year=birth_year)
    birth_date = f"{birth_year:04d}-01-01" if birth_year is not None else None

    result_date = _parse_kondis_date(date_cell or "", season=page.season)

    return KondisResult(
        season=page.season,
        gender=page.gender,
        event_no=page.event_no,
        rank_in_list=int(rank_in_list),
        performance_raw=perf.raw,
        performance_clean=perf.clean,
        wind=perf.wind,
        athlete_id=int(athlete_id),
        athlete_name=athlete_name,
        club_name=club_name,
        birth_date=birth_date,
        placement_raw=placement_raw,
        venue_city=venue_city,
        stadium=None,
        competition_id=None,
        competition_name=competition_name,
        result_date=result_date,
        source_url=page.url,
    )


def _extract_date_and_rest(text: str, *, season: int) -> tuple[Optional[str], str]:
    s = (text or "").strip()
    if not s:
        return (None, "")

    matches = list(_DATE_TOKEN_RE.finditer(s))
    if not matches:
        return (None, s)
    last = matches[-1]
    date_token = last.group(0)
    if not _parse_kondis_date(date_token, season=season):
        return (None, s)

    before = s[: last.start()].strip()
    after = s[last.end() :].strip()
    rest = f"{before} {after}".strip()
    return (date_token, rest)


def _split_time_first_athlete_and_venue(text: str) -> tuple[str, str]:
    s = (text or "").strip()
    if not s:
        return ("", "")

    birth = None
    for m in _BIRTH_MARKER_RE.finditer(s):
        birth = m
    if not birth:
        return (s, "")

    athlete_cell = s[: birth.end()].strip()
    venue = s[birth.end() :].strip()
    venue = re.sub(r"^\(\s*[*&]\s*\)", "", venue).strip()
    venue = venue.lstrip("*").strip()
    venue = venue.lstrip(",-;/").strip()
    return (athlete_cell, venue)


def _starts_with_time_token(text: str) -> bool:
    return bool(_TIME_TOKEN_RE.match((text or "").strip()))


def _starts_with_rank_or_time(text: str) -> bool:
    t = (text or "").strip()
    return bool(_TIME_TOKEN_RE.match(t) or _RANK_PREFIX_RE.match(t))


def _none_if_empty(text: str) -> Optional[str]:
    s = (text or "").strip()
    return s if s else None


def _looks_like_time(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return bool(_TIME_TOKEN_RE.match(t))


def _parse_int(text: str) -> Optional[int]:
    try:
        return int((text or "").strip())
    except ValueError:
        return None


def _parse_rank_token(text: str) -> Optional[int]:
    s = (text or "").strip()
    if not s:
        return None

    rank_i = _parse_int(s)
    if rank_i is not None:
        return int(rank_i)

    # Some Kondis tables use sub-ranking like "36.1", "36.2" (ties). Store the base rank.
    m = re.fullmatch(r"(?P<rank>\d{1,4})[.,]\d", s)
    if m:
        return int(m.group("rank"))

    # Also handle "36." style tokens if they appear.
    m = re.fullmatch(r"(?P<rank>\d{1,4})\.", s)
    if m:
        return int(m.group("rank"))

    return None


def _parse_kondis_date(text: str, *, season: int) -> Optional[str]:
    t = (text or "").strip()
    if not t:
        return None

    # Examples seen:
    # - 11.okt
    # - 27,apr
    # - 24.Aug
    m = re.search(r"(?P<day>\d{1,2})\s*[.,]\s*(?P<mon>[A-Za-zÆØÅæøå]{3,4})\b", t)
    if not m:
        return None
    try:
        day = int(m.group("day"))
    except ValueError:
        return None
    mon_key = m.group("mon").lower()
    month = _MONTHS.get(mon_key)
    if not month:
        return None
    try:
        return date(int(season), int(month), int(day)).isoformat()
    except ValueError:
        return None


_BIRTH_YEAR_RE = re.compile(r"\s*-\s*(?P<y>\d{2,4}|\?)\s*$")


def _parse_athlete_cell(text: str) -> tuple[str, Optional[str], Optional[int]]:
    s = (text or "").replace("\u00a0", " ").strip()
    if not s:
        return ("", None, None)

    # Remove footnote markers like "(*)"
    s = re.sub(r"\(\s*\*\s*\)", "", s).strip()
    s = s.rstrip("*").strip()
    s = re.sub(r"\s+", " ", s)

    birth_year: Optional[int] = None
    m = _BIRTH_YEAR_RE.search(s)
    if m:
        yy_s = m.group("y")
        if yy_s != "?":
            try:
                yy = int(yy_s)
                if yy < 100:
                    pivot = date.today().year % 100
                    birth_year = 2000 + yy if yy <= pivot else 1900 + yy
                else:
                    birth_year = yy
            except ValueError:
                birth_year = None
        s = s[: m.start()].strip().rstrip(",").strip()

    name = s
    club: Optional[str] = None
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if parts:
            name = parts[0]
            rest = ", ".join(p for p in parts[1:] if p)
            club = rest.strip() or None

    return (name.strip(), club, birth_year)


def _kondis_athlete_id(*, gender: str, name: str, birth_year: Optional[int]) -> int:
    key = f"kondis|{gender}|{(name or '').strip().lower()}|{birth_year or ''}"
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    # Use negative IDs to avoid collisions with minfriidrett showathl IDs.
    return -1 - int(n)


def _safe_cache_filename(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    path = re.sub(r"^https?://", "", url)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", path).strip("_").lower()
    slug = slug[:80] if slug else "kondis"
    return f"{slug}_{digest}.html"
