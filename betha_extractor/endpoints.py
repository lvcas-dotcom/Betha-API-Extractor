from __future__ import annotations
from typing import Dict, Iterable, List, Any

# Grupo A: endpoints independentes
GROUP_A: Dict[str, str] = {
    "imoveis": "imoveis",
    "logradouros": "logradouros",
    "bairros": "bairros",
    "distritos": "distritos",
    "loteamentos": "loteamentos",
    "secoes": "secoes",
    "condominios": "condominios",
    "contribuintes": "contribuintes",
}

# Grupo B: fabricas de URLs a partir de registros do Grupo A
def id_of_imovel(obj: Any) -> str | None:
    for k in ("id","idImovel","id_imovel","codigo"):
        v = obj.get(k) if isinstance(obj, dict) else None
        if v is not None:
            return str(v)
    return None

def id_of_contribuinte(obj: Any) -> str | None:
    for k in ("id","idContribuinte","id_contribuinte","codigo"):
        v = obj.get(k) if isinstance(obj, dict) else None
        if v is not None:
            return str(v)
    return None

def build_group_b_jobs(base_url: str, a_data: Dict[str, List[dict]]) -> List[dict]:
    jobs: List[dict] = []

    # imoveis → proprietarios, testadas, campos-adicionais
    for rec in a_data.get("imoveis", []):
        _id = id_of_imovel(rec)
        if not _id:
            continue
        jobs.append({
            "endpoint": "imoveis_proprietarios",
            "url": f"{base_url}/imoveis/{_id}/proprietarios",
        })
        jobs.append({
            "endpoint": "imoveis_campos_adicionais",
            "url": f"{base_url}/imoveis/{_id}/campos-adicionais",
        })

    # contribuintes → enderecos
    for rec in a_data.get("contribuintes", []):
        _id = id_of_contribuinte(rec)
        if not _id:
            continue
        jobs.append({
            "endpoint": "contribuintes_enderecos",
            "url": f"{base_url}/contribuintes/{_id}/enderecos",
        })
    return jobs
