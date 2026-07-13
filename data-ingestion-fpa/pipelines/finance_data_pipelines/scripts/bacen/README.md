# scripts/bacen — Ranking de Câmbio (Bacen)

Scripts de ingestão manual de dados do Banco Central do Brasil. Diferente dos jobs em `jobs/`, esses scripts **rodam localmente** — a API do Bacen bloqueia requisições vindas de IPs de datacenter/cloud.

---

## Fonte de dados

**Ranking de Câmbio por Instituição Financeira**
Site: https://www.bcb.gov.br/estatisticas/rankingcambioinstituicoes

O Bacen publica mensalmente um ranking com o volume de operações de câmbio (exportação, importação, transferências e interbancário) por instituição financeira. Os dados são disponibilizados em arquivos `.zip` contendo planilhas `.xlsx`.

---

## Destino

Tabela BigQuery:
```
avenue_finance.avenue_bacen_api_ranking_cambio_instituicao_financeiras
```

---

## Scripts

| Arquivo | O que faz |
|---|---|
| `scrape_ranking_cambio.py` | Lista os ZIPs do Bacen para o período, extrai e parseia os XLSXs, e carrega no BigQuery |

---

## Como funciona a ingestão

```
Bacen API (lista ZIPs por ano)
    └── GET /cronologicodados?ano={ano}
            │
            ▼
    Para cada mês: baixa o ZIP em memória
            │
            ▼
    Extrai o XLSX mensal (ignora o acumulado)
            │
            ▼
    Detecta header e coluna conglomerado dinamicamente
    (o layout da planilha varia entre anos)
            │
            ▼
    Normaliza colunas numéricas (separadores BR → padrão)
            │
            ▼
    DELETE no BigQuery para os anos do período
            │
            ▼
    INSERT dos registros consolidados
```

A estratégia de carga é **delete + insert por ano**: antes de inserir, apaga todos os registros dos anos do período. Isso garante idempotência — rodar o script duas vezes para o mesmo período não duplica dados.

---

## Pré-requisitos

**Variáveis de ambiente** (arquivo `.env` na raiz do repo):
```
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<token>
```

As credenciais GCP são buscadas automaticamente no Databricks Secret Scope `avenue-secrets-dash-finance`.

**Dependências extras** (além do `pyproject.toml`):
```bash
pip install openpyxl
```

---

## Como rodar

```bash
# A partir da raiz do repo

# Ano atual (padrão)
python scripts/bacen/scrape_ranking_cambio.py

# Range específico
python scripts/bacen/scrape_ranking_cambio.py --ano-inicio 2023 --ano-fim 2026
```

---

## Periodicidade

**Manual, sob demanda.** Como não é possível agendar em nuvem, o script deve ser executado localmente sempre que houver necessidade de atualizar os dados (ex: início de cada mês, após o Bacen publicar o mês anterior).

O Bacen publica os dados do mês anterior com alguns dias de atraso — verifique o site antes de rodar.
