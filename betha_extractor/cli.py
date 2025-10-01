from __future__ import annotations
import argparse
from pathlib import Path

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="betha-extractor", description="Extrator Betha (Python)")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Executa a extração")
    run.add_argument("--all", action="store_true", help="Extrai Grupo A e Grupo B (padrão)")
    run.add_argument("--group", choices=["A","B"], help="Extrai apenas um grupo")
    run.add_argument("--only", type=str, help="Lista de endpoints do Grupo A separados por vírgula")
    run.add_argument("--output", type=Path, help="Diretório de saída")
    run.add_argument("--limit", type=int, help="Tamanho da página")
    run.add_argument("--timeout", type=int, help="Timeout da requisição (s)")
    run.add_argument("--concurrency", type=int, help="Concorrência para Grupo B")
    return p
