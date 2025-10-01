from __future__ import annotations
from typing import Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..http_client import HttpClient
from ..pagination import pick_rows
from ..writers import write_json
from ..endpoints import build_group_b_jobs
from ..audit import Auditor, AuditRow, now_iso

# progress_cb(done_jobs: int, total_jobs: int, endpoint: str, fetched: int, accumulated_global: int, percent: Optional[float]) -> None
ProgressCB = Callable[[int, int, str, int, int, Optional[float]], None]


class GroupBExtractor:
    def __init__(
        self, client: HttpClient, output_dir, limit: int, concurrency: int = 8
    ):
        self.client = client
        self.output_dir = output_dir
        self.limit = limit
        self.concurrency = max(1, concurrency)
        self.auditor = Auditor(output_dir)

    def _fetch(self, url: str) -> List[dict]:
        resp = self.client.get(url, params={"limit": self.limit, "size": self.limit})
        # 404/204 podem ocorrer quando cadastro não existe
        if resp.status_code in (404, 204):
            return []
        resp.raise_for_status()
        return pick_rows(resp.json())

    def run(
        self,
        group_a_data: Dict[str, List[dict]],
        base_url: str,
        progress_cb: Optional[ProgressCB] = None,
    ) -> Dict[str, List[dict]]:
        jobs = build_group_b_jobs(base_url, group_a_data)
        buckets: Dict[str, List[dict]] = {}

        total_jobs = len(jobs) if jobs else 0
        done_jobs = 0

        with ThreadPoolExecutor(max_workers=self.concurrency) as ex:
            fut_to_job = {ex.submit(self._fetch, j["url"]): j for j in jobs}
            for fut in as_completed(fut_to_job):
                job = fut_to_job[fut]
                endpoint = job["endpoint"]
                try:
                    rows = fut.result()
                except Exception:
                    rows = []

                if endpoint not in buckets:
                    buckets[endpoint] = []
                buckets[endpoint].extend(rows)

                # Audit + progresso (percent global por jobs)
                done_jobs += 1
                percent = (done_jobs / total_jobs * 100.0) if total_jobs else None
                accumulated_global = sum(len(v) for v in buckets.values())

                self.auditor.write(
                    AuditRow(
                        ts=now_iso(),
                        group="B",
                        endpoint=endpoint,
                        unit="job",
                        index=done_jobs,
                        fetched=len(rows),
                        accumulated=accumulated_global,
                        total_hint=total_jobs,
                        percent=percent,
                        file=f"{endpoint}.json",
                    )
                )
                if progress_cb:
                    progress_cb(
                        done_jobs,
                        total_jobs,
                        endpoint,
                        len(rows),
                        accumulated_global,
                        percent,
                    )

        # dedupe simples e gravação
        for k, rows in buckets.items():
            seen = set()
            deduped = []
            for r in rows:
                sig = str(sorted(r.items())) if isinstance(r, dict) else str(r)
                if sig in seen:
                    continue
                seen.add(sig)
                deduped.append(r)
            buckets[k] = deduped
            write_json(self.output_dir, k, deduped)

        return buckets
