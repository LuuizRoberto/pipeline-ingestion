# data-fpa-risk

Repositório de dados do time de **FP&A & Risco** da Avenue. Organizado em torno da arquitetura **medallion** (raw → silver → gold), reúne os códigos de ingestão, transformação, modelagem e as extrações analíticas sobre o data warehouse.

---

## Visão geral

Quatro frentes, seguindo o fluxo do dado:

| Frente | Pasta | O que faz | Tecnologia |
|---|---|---|---|
| **Ingestão** (→ raw) | [`pipelines/`](pipelines/) | Extrai dados de fontes externas (Matera, Jira, Bacen) e grava na camada raw | Python · Spark · Databricks → *Cloud Run* |
| **Transformação + Modelagem** (raw → silver → gold) | [`dataform/`](dataform/) | Limpa/conforma (silver) e modela fatos, dimensões e métricas (gold) | Dataform · BigQuery |
| **Exports** (lê da gold) | [`exports/`](exports/) | Queries ad-hoc e extrações rápidas | BigQuery Standard SQL |
| **Orquestração** | [`orchestration/`](orchestration/) | Sequencia as frentes ponta a ponta | Cloud Workflows |

O fluxo de ponta a ponta:

```
Fontes (API) ──► pipelines/ ──► RAW ──► dataform/ (silver) ──► dataform/ (gold) ──► exports/
  Matera,Jira    (ingestão)              (transformação)        (modelagem)        (ad-hoc)
                          └──────────── orquestração: orchestration/ ────────────┘
```

---

## Estrutura

```
data-fpa-risk/
├── pyproject.toml                ← packaging + deps + config de ruff/pytest (raiz)
├── Makefile                      ← atalhos: install / test / lint / fmt
├── .github/workflows/            ← CI (lint + mypy + testes)
│
├── pipelines/                    ← INGESTÃO (→ raw)
│   └── finance_data_pipelines/   ← framework Python de ETL (Matera, Jira, Bacen)
│       ├── core/  sources/  jobs/  transfers/  scripts/  tests/
│
├── dataform/                     ← TRANSFORMAÇÃO + MODELAGEM (raw → silver → gold)
│   ├── workflow_settings.yaml
│   ├── definitions/
│   │   ├── sources/              ← declarations das tabelas raw
│   │   ├── silver/               ← raw → silver
│   │   └── gold/                 ← silver → gold
│   └── includes/                 ← constantes/macros JS
│
├── exports/                      ← EXPORTS ad-hoc (lê da gold)
│   └── simple_exports/
│
├── orchestration/                ← Cloud Workflows
│   └── workflows/
│
└── docs/                         ← arquitetura, dicionário de dados, runbooks
```

> **Imports (ingestão).** O código Python usa o caminho completo `pipelines.finance_data_pipelines.<pacote>`. Rode `pip install -e ".[dev]"` a partir da raiz — isso expõe o pacote e resolve os imports em dev, testes e scripts. No Databricks (DBR 14.3+), a raiz do repo entra automaticamente no `sys.path`. *(Esse prefixo é transitório: some quando a ingestão for containerizada no Cloud Run.)*

---

## Ambiente de dados

- **GCP / BigQuery** — data warehouse principal. Projeto `data-prod-295413`, datasets `avenue_finance`, `avenue_data_warehouse`, `avenue_datalake*`.
- **Databricks / Delta Lake** — plataforma de execução dos pipelines (em processo de descontinuação; ver [nota de migração](pipelines/finance_data_pipelines/README.md#estratégia-de-migração-para-gcp)).

> **Dupla jurisdição.** Os dados abrangem entidades reguladas no Brasil (Avenue Banco de Investimentos) e nos EUA (Avenue Securities, Avenue Cash). Qualquer novo uso, exportação ou compartilhamento de dados que contenham PII de cliente (CPF, SSN, dados de conta e transação) deve considerar LGPD e as regras norte-americanas (Reg S-P, GLBA) e, em caso de dúvida, ser validado com Compliance e Jurídico.

---

## Por onde começar

- **Ingestão / pipelines** → [`pipelines/finance_data_pipelines/README.md`](pipelines/finance_data_pipelines/README.md)
- **Transformação e modelagem (Dataform)** → [`dataform/README.md`](dataform/README.md)
- **Exports e análises ad-hoc** → [`exports/README.md`](exports/README.md)

---

## Convenções

- **Idioma:** documentação e comentários em português; nomes de objetos de dados em inglês (padrão do warehouse).
- **Credenciais:** nunca commitadas. Arquivos `.env` estão no `.gitignore` — use service accounts e secrets do ambiente.
- **Branches:** trabalho em `dev`, PRs para `main`.
