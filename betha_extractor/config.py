from __future__ import annotations
import os, json, re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

@dataclass
class Settings:
    base_url: str
    user_access: str
    bearer: str
    page_limit: int = 200
    timeout_seconds: int = 10
    max_retries: int = 5
    concurrency: int = 8
    output_dir: Path = Path("./exports")

def _read_workflow_headers(path: Path) -> tuple[Optional[str], Optional[str]]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None, None

    # procuramos o node do tipo httpRequest e seus headers
    nodes = data.get("nodes", [])
    for n in nodes:
        if n.get("type", "").endswith("httpRequest"):
            headers = (n.get("parameters", {}) or {}).get("headerParameters", {})
            params = (headers.get("parameters") or []) if isinstance(headers, dict) else []
            ua, auth = None, None
            for p in params:
                name = p.get("name")
                val = p.get("value")
                if not isinstance(name, str): 
                    continue
                if name.lower().strip() == "user-access":
                    ua = val
                if name.lower().strip() == "authorization":
                    # aceita "Bearer <token>" ou só token
                    if isinstance(val, str) and val.startswith("Bearer "):
                        auth = val.split(" ", 1)[1].strip()
                    else:
                        auth = val
            if ua or auth:
                return ua, auth
    return None, None

def load_settings() -> Settings:
    base_url = os.getenv("BETHA_BASE_URL", "https://tributos.suite.betha.cloud/dados/v1").rstrip("/")
    user_access = os.getenv("BETHA_USER_ACCESS")
    bearer = os.getenv("BETHA_BEARER")
    # fallback: tentar extrair do workflow json
    if not user_access or not bearer:
        wf = os.getenv("WORKFLOW_JSON")
        if wf and Path(wf).exists():
            ua, auth = _read_workflow_headers(Path(wf))
            user_access = user_access or ua
            bearer = bearer or auth

    if not user_access or not bearer:
        raise RuntimeError("Credenciais ausentes. Defina BETHA_USER_ACCESS e BETHA_BEARER no .env ou informe WORKFLOW_JSON.")

    page_limit = int(os.getenv("PAGE_LIMIT", "200"))
    timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
    max_retries = int(os.getenv("MAX_RETRIES", "5"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))
    output_dir = Path(os.getenv("OUTPUT_DIR", "./exports")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        base_url=base_url,
        user_access=user_access,
        bearer=bearer,
        page_limit=page_limit,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        concurrency=concurrency,
        output_dir=output_dir,
    )
