from __future__ import annotations
from typing import Any, Dict, List, Tuple

def pick_rows(body: Any) -> List[Dict]:
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return []
    for key in ("content", "items", "data", "results", "body"):
        v = body.get(key)
        if isinstance(v, list):
            return v
    return []

def key_of(o: Any) -> str:
    if not isinstance(o, dict):
        return ""
    for k in ("id","codigo","codigo_cadastro","_id","uuid","idImovel","idContribuinte","id_imovel","id_contribuinte"):
        if k in o and o[k] is not None:
            return str(o[k])
    return ""

def next_page_state(body: Any, limit: int, cur_offset: int, cur_page: int, cur_seq: int, last_sig: str | None) -> Tuple[bool, int, int, int, str | None, int]:
    rows = pick_rows(body)
    count = len(rows)
    api_has = None
    if isinstance(body, dict):
        if isinstance(body.get("hasNext"), bool):
            api_has = body["hasNext"]
        elif isinstance(body.get("has_more"), bool):
            api_has = body["has_more"]

    first = rows[0] if rows else None
    last = rows[-1] if rows else None
    page_sig = f"{key_of(first)}|{key_of(last)}|{count}"

    has_more = api_has if api_has is not None else (count >= limit)
    if count == 0:
        has_more = False
    if has_more and last_sig and page_sig == last_sig:
        has_more = False  # anti-loop

    next_offset = (cur_offset + limit) if has_more else cur_offset
    next_page = (cur_page + 1) if has_more else cur_page
    next_seq = (cur_seq + 1) if has_more else cur_seq

    if has_more and next_offset == cur_offset:
        has_more = False

    return has_more, next_offset, next_page, next_seq, (page_sig if has_more else None), count
