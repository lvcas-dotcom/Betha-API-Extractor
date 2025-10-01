#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
from pathlib import Path
from typing import Dict, List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
)

# módulos internos
from betha_extractor.config import load_settings
from betha_extractor.http_client import HttpClient
from betha_extractor.endpoints import GROUP_A
from betha_extractor.extractors.group_a import GroupAExtractor
from betha_extractor.extractors.group_b import GroupBExtractor

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()


def _banner():
    console.print(
        Panel.fit(
            "[bold magenta]Betha Extractor[/] [white]— CLI coloridinha[/]",
            subtitle="Refatorado do n8n para Python",
            border_style="magenta",
        )
    )


def _print_config(s):
    t = Table(
        title="Configuração", show_lines=False, expand=True, title_style="bold blue"
    )
    t.add_column("Chave", style="cyan", no_wrap=True)
    t.add_column("Valor", style="white")
    t.add_row("Base URL", s.base_url)
    t.add_row("User-Access", "[green]***[/]")
    t.add_row("Bearer", "[green]***[/]")
    t.add_row("Page Limit", str(s.page_limit))
    t.add_row("Timeout (s)", str(s.timeout_seconds))
    t.add_row("Retries", str(s.max_retries))
    t.add_row("Concorrência (B)", str(s.concurrency))
    t.add_row("Output Dir", str(s.output_dir))
    console.print(t)


def _print_summary(title: str, data: Dict[str, list], output_dir: Path):
    tb = Table(title=f"Resumo — {title}", title_style="bold purple", expand=True)
    tb.add_column("Arquivo", style="cyan")
    tb.add_column("Registros", style="green", justify="right")
    tb.add_column("Path", style="white")
    for k, rows in sorted(data.items()):
        path = output_dir / f"{k}.json"
        tb.add_row(f"{k}.json", str(len(rows)), str(path))
    console.print(tb)

    audit = output_dir / "_audit.csv"
    if audit.exists():
        console.print(
            Panel.fit(f"Logs gravados em: [bold]{audit}[/]", border_style="blue")
        )


def _run_impl(
    all: bool,
    group: str | None,
    only: str | None,
    output: Path | None,
    limit: int | None,
    timeout: int | None,
    concurrency: int | None,
):
    _banner()
    s = load_settings()
    if output:
        s.output_dir = output.resolve()
        s.output_dir.mkdir(parents=True, exist_ok=True)
    if limit:
        s.page_limit = limit
    if timeout:
        s.timeout_seconds = timeout
    if concurrency:
        s.concurrency = concurrency

    _print_config(s)

    client = HttpClient(
        base_url=s.base_url,
        user_access=s.user_access,
        bearer=s.bearer,
        timeout=s.timeout_seconds,
        max_retries=s.max_retries,
    )

    mode = "all" if (all or (group is None)) else group.upper()
    if mode not in ("A", "B", "all"):
        console.print("[red]Parâmetro --group deve ser A ou B[/]")
        raise SystemExit(2)

    a_result: Dict[str, List[dict]] = {}

    # ===== Grupo A (com % correta por endpoint) =====
    if mode in ("A", "all"):
        selected = GROUP_A
        if only:
            only_set = {k.strip() for k in only.split(",") if k.strip()}
            selected = {k: v for k, v in GROUP_A.items() if k in only_set}
            if not selected:
                console.print(
                    "[yellow]Nada a fazer: nenhum endpoint válido em --only[/]"
                )
                return

        console.print(
            Panel("Grupo [bold]A[/] — endpoints independentes", border_style="cyan")
        )
        ga = GroupAExtractor(client, s.output_dir, s.page_limit)

        # A task "Endpoints A" (global) permanece como estava
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>6.2f}%"),
            TimeElapsedColumn(),
            transient=False,
            console=console,
        ) as progress:
            task_endpoints = progress.add_task("Endpoints A", total=len(selected))

            for key, path in selected.items():
                # Subtask do endpoint: controlamos % manualmente (0 → 100)
                sub_id = progress.add_task(f"{key}: 0/? (—%)", total=100, completed=0)

                def on_page(
                    _endpoint: str,
                    page_index: int,
                    fetched: int,
                    accumulated: int,
                    total_hint: Optional[int],
                    percent: Optional[float],  # ignorado
                ):
                    if total_hint and total_hint > 0:
                        raw = (accumulated / total_hint) * 100.0
                        p = max(0.0, min(100.0, round(raw, 2)))
                        desc = f"{key}: {accumulated}/{total_hint} ({p:.2f}%)"
                        progress.update(sub_id, completed=p, description=desc)
                    else:
                        # sem total conhecido, não chutamos %
                        desc = f"{key}: {accumulated}/? (—%)"
                        progress.update(sub_id, description=desc)

                _, rows = ga.extract_one(path, progress_cb=on_page)

                from betha_extractor.writers import write_json

                write_json(s.output_dir, key, rows)

                # Fecha a subtask em 100% com números finais
                total_rows = len(rows)
                progress.update(
                    sub_id,
                    completed=100,
                    description=f"{key}: {total_rows}/{total_rows} (100.00%)",
                )
                progress.advance(task_endpoints)

        _print_summary("Grupo A", a_result, s.output_dir)

    # ===== Grupo B (progresso por jobs concluídos) =====
    if mode in ("B", "all"):
        console.print(
            Panel("Grupo [bold]B[/] — endpoints dependentes", border_style="green")
        )

        if mode == "B":
            a_result = {}
            for key in ("imoveis", "contribuintes"):
                p = s.output_dir / f"{key}.json"
                if p.exists():
                    try:
                        a_result[key] = json.loads(p.read_text(encoding="utf-8"))
                    except Exception:
                        a_result[key] = []

        gb = GroupBExtractor(client, s.output_dir, s.page_limit, s.concurrency)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>6.2f}%"),
            TimeElapsedColumn(),
            transient=False,
            console=console,
        ) as progress:
            task_jobs = progress.add_task("Jobs B", total=1)
            first_call = {"done": False}

            def on_job(
                done_jobs: int,
                total_jobs: int,
                endpoint: str,
                fetched: int,
                accumulated_global: int,
                percent: Optional[float],
            ):
                # Ajusta total no primeiro callback para refletir a fila real
                if not first_call["done"]:
                    progress.update(task_jobs, total=max(1, total_jobs))
                    first_call["done"] = True

                if total_jobs and total_jobs > 0:
                    raw = (done_jobs / total_jobs) * 100.0
                    p = max(0.0, min(100.0, round(raw, 2)))
                    progress.update(
                        task_jobs,
                        completed=done_jobs,
                        description=f"Jobs B — {done_jobs}/{total_jobs} ({p:.2f}%)  last={endpoint}[+{fetched}] total={accumulated_global}",
                    )
                else:
                    progress.update(
                        task_jobs,
                        completed=done_jobs,
                        description=f"Jobs B — {done_jobs}/? (—%)  last={endpoint}[+{fetched}] total={accumulated_global}",
                    )

            buckets = gb.run(a_result, s.base_url, progress_cb=on_job)

        _print_summary("Grupo B", buckets, s.output_dir)


@app.command(help="Extrai Grupo A, Grupo B ou ambos (padrão).")
def run(
    all: bool = typer.Option(False, "--all", help="Extrai Grupo A e Grupo B."),
    group: str = typer.Option(
        None, "--group", help="Extrai apenas um grupo", case_sensitive=False
    ),
    only: str = typer.Option(
        None, "--only", help="Lista de endpoints do Grupo A separados por vírgula"
    ),
    output: Path = typer.Option(None, "--output", help="Diretório de saída"),
    limit: int = typer.Option(None, "--limit", help="Tamanho da página"),
    timeout: int = typer.Option(None, "--timeout", help="Timeout por request (s)"),
    concurrency: int = typer.Option(
        None, "--concurrency", help="Concorrência para Grupo B"
    ),
):
    _run_impl(all, group, only, output, limit, timeout, concurrency)


def _interactive_menu():
    _banner()
    console.print(
        "[bold]Selecione a opção:[/]\n"
        "[cyan]1[/] — Extrair Grupo A\n"
        "[green]2[/] — Extrair Grupo B\n"
        "[magenta]3[/] — Extrair Ambos (A + B)\n"
        "[yellow]4[/] — Extrair apenas alguns endpoints de A\n"
        "[red]5[/] — Sair"
    )
    choice = console.input("\nDigite [1-5]: ").strip()
    if choice == "1":
        _run_impl(False, "A", None, None, None, None, None)
    elif choice == "2":
        _run_impl(False, "B", None, None, None, None, None)
    elif choice == "3":
        _run_impl(True, None, None, None, None, None, None)
    elif choice == "4":
        only = console.input(
            "Informe endpoints de A separados por vírgula (ex.: imoveis,logradouros): "
        ).strip()
        _run_impl(False, "A", only, None, None, None, None)
    else:
        console.print("[red]Saindo...[/]")
        raise SystemExit(0)


if __name__ == "__main__":
    try:
        if len(sys.argv) == 1:
            _interactive_menu()
        else:
            app()
    except KeyboardInterrupt:
        console.print("\n[red]Interrompido pelo usuário.[/]")
        sys.exit(130)
