# Betha Extractor •

> **Sobre**
>
> * Grupo **A** (independentes): `imoveis`, `logradouros`, `bairros`, `distritos`, `loteamentos`, `secoes`, `condominios`, `contribuintes`
> * Grupo **B** (dependentes), gerados a partir do A:
>
>   * `imoveis/{id}/proprietarios`
>   * `imoveis/{id}/testadas`
>   * `imoveis/{id}/campos-adicionais`
>   * `contribuintes/{id}/enderecos`
> * Paginação robusta com **anti-loop** (assinatura `first/last/count`)
> * **Retries** com backoff exponencial, timeouts e cabeçalhos dinâmicos
> * Saída **1 arquivo JSON por método** (A e B), acumulando **todas** as páginas
> * CLI com **Typer** (UX clara) e **Rich** (progresso e tabelas)

---

## Requisitos

* Python 3.10+
* Acesso aos endpoints Betha (tokens válidos)

---

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -e .
```
---

## Configuração

Crie um `.env` na raiz (ou exporte variáveis de ambiente):

```ini
# .env
BETHA_BASE_URL=https://tributos.suite.betha.cloud/dados/v1
BETHA_USER_ACCESS=SEU_USER_ACCESS
BETHA_BEARER=SEU_BEARER_TOKEN

# Limites e comportamento
PAGE_LIMIT=200
REQUEST_TIMEOUT_SECONDS=10
MAX_RETRIES=5
CONCURRENCY=8
OUTPUT_DIR=./exports
```


## Estrutura do projeto

```
betha_extractor/
  __init__.py
  main.py              # CLI Typer
  config.py            # Carrega .env e settings
  http_client.py       # requests.Session + Retry/Timeout
  endpoints.py         # Catálogos Grupo A/B
  pagination.py        # Assinaturas e guardas anti-loop
  writers.py           # Escrita de JSON
  extractors/
    base.py            # Contratos comuns
    group_a.py         # Extrator endpoints independentes
    group_b.py         # Extrator dependente (usa resultados do A)
requirements.txt
README.md
```

---

## Uso (CLI)

Ajuda geral:

```bash
python -m betha_extractor.main --help
```

Executar **A + B** (padrão)

Somente **Grupo A**:

Somente **Grupo B** (lê `imoveis.json` e `contribuintes.json` já extraídos)

Ajustes operacionais:

```bash
# paginar com 500, timeout 15s, gravar em ./out
python -m betha_extractor.main run --limit 500 --timeout 15 --output ./out

# controlar concorrência do Grupo B (I/O-bound)
python -m betha_extractor.main run --group B --concurrency 12
```

---

## Saída (padrão)

* **Grupo A:** `OUTPUT_DIR/<endpoint>.json` contendo **todas** as páginas acumuladas.
* **Grupo B:** `OUTPUT_DIR/<metodo>.json` consolidado por método dependente.

Exemplo (trecho):

```json
[
  {
    "id": 12345,
    "codigo": "ABC-001",
    "situacao": "ATIVO",
    "endereco": { "logradouro": "R X", "numero": "100" }
  }
]
```

---

## Boas práticas de operação

* **Page Limit:** use 200–500 (equilíbrio entre payload e chamadas).
* **Timeout:** 10–20s para redes instáveis.
* **Retries:** 3–5 (com backoff); monitore status 429/5xx.
* **Concorrência (B):** 8–16 em hosts com boa largura de banda; ajuste conforme limites do provedor.
* **Idempotência:** reprocessar só sobrescreve JSON, não duplica.

---

## Contribuindo

* Padrão PEP8 + `black`.
* Commits descritivos (`feat:`, `fix:`, `chore:`).
* Testes para extractors e pagination.

---

## Licença

Proprietário / uso interno Tributech.

---
