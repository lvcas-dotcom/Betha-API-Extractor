from __future__ import annotations
from typing import Dict, List, Any, Tuple, Callable, Optional
from ..http_client import HttpClient
from ..pagination import pick_rows, next_page_state
from ..writers import write_json
from ..audit import Auditor, AuditRow, now_iso

# Tipo do callback de progresso:
# progress_cb(endpoint: str, page_index: int, fetched: int, accumulated: int, total_hint: Optional[int], percent: Optional[float]) -> None
ProgressCB = Callable[[str, int, int, int, Optional[int], Optional[float]], None]


class GroupAExtractor:
    def __init__(self, client: HttpClient, output_dir, limit: int):
        self.client = client
        self.output_dir = output_dir
        self.limit = limit
        self.auditor = Auditor(output_dir)

    def extract_one(
        self, endpoint: str, progress_cb: Optional[ProgressCB] = None
    ) -> Tuple[str, List[dict]]:
        acc: List[dict] = []
        offset = 0
        page = 0
        file_seq = 1
        last_sig = None
        total_hint: Optional[int] = None

        while True:
            params = {
                "limit": self.limit,
                "offset": offset,
                "page": page,
                "size": self.limit,
            }
            resp = self.client.get(endpoint, params=params)
            resp.raise_for_status()
            body = resp.json()

            # detectar total
            if isinstance(body, dict):
                for k in ("totalElements", "total", "totalItems", "totalCount"):
                    if isinstance(body.get(k), int) and body[k] > 0:
                        total_hint = body[k]
                        break

            rows = pick_rows(body)
            acc.extend(rows)

            percent = (
                (len(acc) / total_hint * 100.0)
                if (total_hint and total_hint > 0)
                else None
            )

            # Audit + Progresso
            self.auditor.write(
                AuditRow(
                    ts=now_iso(),
                    group="A",
                    endpoint=endpoint,
                    unit="page",
                    index=file_seq,
                    fetched=len(rows),
                    accumulated=len(acc),
                    total_hint=total_hint,
                    percent=percent,
                    file=f"{endpoint}.json",
                )
            )
            if progress_cb:
                progress_cb(
                    endpoint, file_seq, len(rows), len(acc), total_hint, percent
                )

            has_more, next_offset, next_page, next_seq, last_sig, _count = (
                next_page_state(body, self.limit, offset, page, file_seq, last_sig)
            )
            if not has_more:
                break
            offset, page, file_seq = next_offset, next_page, next_seq

        return endpoint, acc

    def run(
        self, endpoints: Dict[str, str], progress_cb: Optional[ProgressCB] = None
    ) -> Dict[str, List[dict]]:
        out: Dict[str, List[dict]] = {}
        for key, path in endpoints.items():
            ep, rows = self.extract_one(path, progress_cb=progress_cb)
            write_json(self.output_dir, key, rows)
            out[key] = rows
        return out
