from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Iterable, List

def write_json(output_dir: Path, name: str, rows: List[dict]) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
    path = output_dir / f"{safe}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return path
