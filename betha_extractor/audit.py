
from __future__ import annotations
import csv
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

@dataclass
class AuditRow:
    ts: str
    group: str           # "A" | "B"
    endpoint: str
    unit: str            # "page" | "job"
    index: int
    fetched: int
    accumulated: int
    total_hint: Optional[int]
    percent: Optional[float]
    file: str

class Auditor:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.path = output_dir / "_audit.csv"
        # Write header if file not exists
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts","group","endpoint","unit","index","fetched","accumulated","total_hint","percent","file"])

    def write(self, row: AuditRow) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                row.ts, row.group, row.endpoint, row.unit, row.index,
                row.fetched, row.accumulated,
                row.total_hint if row.total_hint is not None else "",
                f"{row.percent:.2f}" if row.percent is not None else "",
                row.file
            ])

def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
