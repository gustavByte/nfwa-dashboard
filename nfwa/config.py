from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


MINFRIIDRETT_BASE_URL = "https://www.minfriidrettsstatistikk.info/php/LandsStatistikk.php"

# showclass IDs from minfriidrettsstatistikk.info
SHOWCLASS_WOMEN_SENIOR = 22
SHOWCLASS_MEN_SENIOR = 11


@dataclass(frozen=True)
class Source:
    gender: str  # "Women" | "Men"
    showclass: int


SOURCES = (
    Source(gender="Women", showclass=SHOWCLASS_WOMEN_SENIOR),
    Source(gender="Men", showclass=SHOWCLASS_MEN_SENIOR),
)


def default_data_dir() -> Path:
    return Path("data")


def default_results_db_path() -> Path:
    return default_data_dir() / "nfwa_results.sqlite3"


def default_cache_dir() -> Path:
    return default_data_dir() / "cache" / "minfriidrett"


def default_kondis_cache_dir() -> Path:
    return default_data_dir() / "cache" / "kondis"


def default_old_data_dir() -> Path:
    return Path("friidrett_data_old_1999_and_older")


def default_wa_scoring_db_path() -> Path:
    # Re-uses the existing toolkit in ./WA Poeng/
    return Path("WA Poeng") / "wa_scoring.db"
