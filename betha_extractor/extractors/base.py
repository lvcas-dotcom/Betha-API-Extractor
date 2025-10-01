from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any
from pathlib import Path
from ..http_client import HttpClient

@dataclass
class ExtractResult:
    endpoint: str
    records: List[dict]
    path: Path

class BaseExtractor:
    def __init__(self, client: HttpClient):
        self.client = client
