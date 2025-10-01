from __future__ import annotations
import time
from typing import Any, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class HttpClient:
    def __init__(self, base_url: str, user_access: str, bearer: str, timeout: int = 10, max_retries: int = 5):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

        # Retry policy
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            "User-Access": user_access,
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/json",
        })

    def get(self, path_or_url: str, params: Dict[str, Any] | None = None) -> requests.Response:
        if path_or_url.startswith("http"):
            url = path_or_url
        else:
            url = f"{self.base_url}/{path_or_url.lstrip('/')}"
        # _ts anti-cache
        params = dict(params or {})
        params.setdefault("_ts", int(time.time() * 1000))
        return self.session.get(url, params=params, timeout=self.timeout)
