# Betha Extractor (Python)

Refatoração do workflow n8n para Python — arquitetura modular, robusta e pronta para produção.

## O que faz
- Extrai **Grupo A** (independentes): `imoveis`, `logradouros`, `bairros`, `distritos`, `loteamentos`, `secoes`, `condominios`, `contribuintes`
- Gera **Grupo B** automaticamente a partir dos resultados:
  - `imoveis/{id}/proprietarios`
  - `imoveis/{id}/testadas`
  - `imoveis/{id}/campos-adicionais`
  - `contribuintes/{id}/enderecos`
- Paginação robusta com anti-loop (assinatura `first/last/count`)
- Retentativas com backoff, timeouts e cabeçalhos dinâmicos
- 1 arquivo JSON **por método** (A e B), com **todas** as páginas (A) e agregação (B)
- Execução CLI com opções

## Instalação
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

## Configuração
Crie um `.env` na raiz (ou exporte variáveis de ambiente):

```ini
# .env
BETHA_BASE_URL=https://tributos.suite.betha.cloud/dados/v1
BETHA_USER_ACCESS=SEU_USER_ACCESS
BETHA_BEARER=SEU_BEARER_TOKEN
# Opcional: caminho de um workflow n8n para auto-extrair cabeçalhos (se variáveis acima não forem definidas):
WORKFLOW_JSON=/mnt/data/betha - gpt.json

# Limites
PAGE_LIMIT=200
REQUEST_TIMEOUT_SECONDS=10
MAX_RETRIES=5
CONCURRENCY=8
OUTPUT_DIR=./exports
```
## Estrutura
```
betha_extractor/
  __init__.py
  main.py            # CLI (argparse)
  cli.py             # Parsing + orquestração
  config.py          # .env / workflow parser
  http_client.py     # requests.Session + Retry
  pagination.py      # paginação com anti-loop
  endpoints.py       # catálogos A e B, helpers de ID
  writers.py         # escrita de JSON
  extractors/
    base.py          # contrato comum
    group_a.py       # extrai endpoints independentes
    group_b.py       # consome A → gera chamadas B
requirements.txt
README.md
```

## Observações
- Respostas de API são normalizadas a partir de chaves conhecidas: `content`, `items`, `data`, `results`, ou array direto.
- O cálculo de `has_more` segue: `body.hasNext || body.has_more || len(rows) >= limit`, com guardas de loop.
- Erros de rede retornam com retentativas e log; arquivos ainda são gravados com o que foi coletado até o momento.

Boa extração!
