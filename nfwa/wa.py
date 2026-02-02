from __future__ import annotations

import importlib.util
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class WaEventMeta:
    gender: str
    event: str
    orientation: str  # "lower" | "higher"
    precision: int


def ensure_wa_poeng_importable(*, wa_poeng_root: Path) -> None:
    if importlib.util.find_spec("wa_poeng") is not None:
        return
    sys.path.insert(0, str(wa_poeng_root))


def wa_event_meta(*, wa_db_path: Path, gender: str, event: str) -> Optional[WaEventMeta]:
    con = sqlite3.connect(wa_db_path)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            "SELECT gender, name, orientation, precision FROM events WHERE gender = ? AND name = ?",
            (gender, event),
        ).fetchone()
        if not row:
            return None
        return WaEventMeta(
            gender=row["gender"],
            event=row["name"],
            orientation=row["orientation"],
            precision=row["precision"],
        )
    finally:
        con.close()


def wa_event_names(*, wa_db_path: Path, gender: str) -> set[str]:
    con = sqlite3.connect(wa_db_path)
    try:
        rows = con.execute("SELECT name FROM events WHERE gender = ?", (gender,)).fetchall()
        return {row[0] for row in rows}
    finally:
        con.close()

