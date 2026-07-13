# pipeline-ingestion

Monorepo de **pipelines e frameworks de ingestão de dados** da Avenue. Cada pasta na raiz é um projeto independente — um framework de ETL ou um pipeline de ingestão de um domínio/time específico — com seu próprio ciclo de vida, dependências e documentação.

> A ideia é que este repositório concentre, sob um mesmo teto, as diferentes frentes de ingestão da organização, mantendo cada uma isolada em sua pasta mas com convenções comuns (idioma, tratamento de segredos, arquitetura medallion).

---

## Projetos

| Pasta | Descrição | Stack |
|---|---|---|
| [`data-ingestion-fpa/`](data-ingestion-fpa/) | Dados do time de **FP&A & Risco**. Ingestão (Matera, Jira, Bacen), transformação/modelagem (medallion: raw → silver → gold), exports analíticos e orquestração. | Python · Databricks/Spark · BigQuery · Dataform · Cloud Workflows |

*(Novos frameworks/pipelines entram como novas pastas na raiz — veja [Como adicionar um projeto](#como-adicionar-um-projeto).)*

---

## `data-ingestion-fpa/`

Repositório de dados do time de FP&A & Risco, organizado em torno da arquitetura **medallion** e dividido em quatro frentes que seguem o fluxo do dado:

```
Fontes (API) ──► pipelines/ ──► RAW ──► dataform/ (silver → gold) ──► exports/
  Matera,Jira    (ingestão)          (transformação + modelagem)      (ad-hoc)
                        └────────── orquestração: orchestration/ ──────────┘
```

| Subpasta | Papel | O que contém |
|---|---|---|
| [`pipelines/`](data-ingestion-fpa/pipelines/) | **Ingestão** (→ raw) | Framework Python de ETL (`finance_data_pipelines`) que extrai de fontes externas e grava na camada raw. Módulos `core/`, `sources/`, `jobs/`, `transfers/`, `scripts/`, `tests/`. |
| [`dataform/`](data-ingestion-fpa/dataform/) | **Transformação + Modelagem** (raw → silver → gold) | Projeto Dataform/BigQuery. `definitions/sources/` (declarações raw), `definitions/silver/` (limpeza/conformação), `definitions/gold/` (fatos, dimensões, métricas), `includes/` (macros JS). |
| [`exports/`](data-ingestion-fpa/exports/) | **Exports** (lê da gold) | Queries ad-hoc e extrações rápidas em BigQuery Standard SQL (`simple_exports/`). |
| [`orchestration/`](data-ingestion-fpa/orchestration/) | **Orquestração** | Cloud Workflows que sequenciam as frentes ponta a ponta (`workflows/`). |
| [`docs/`](data-ingestion-fpa/docs/) | **Documentação** | Arquitetura, dicionário de dados, runbooks e ADRs (`adr/`). |

Arquivos de raiz do projeto: `pyproject.toml` (packaging + deps + ruff/pytest), `Makefile` (atalhos `install`/`test`/`lint`/`fmt`) e CI em `.github/workflows/`.

**Por onde começar:** [`data-ingestion-fpa/README.md`](data-ingestion-fpa/README.md) → e os READMEs de cada frente ([pipelines](data-ingestion-fpa/pipelines/finance_data_pipelines/README.md), [dataform](data-ingestion-fpa/dataform/README.md), [exports](data-ingestion-fpa/exports/README.md)).

---

## Ambiente de dados

- **GCP / BigQuery** — data warehouse principal (projeto `data-prod-295413`).
- **Databricks / Delta Lake** — plataforma de execução dos pipelines (em descontinuação; migração para Cloud Run em andamento).

> **Dupla jurisdição.** Os dados abrangem entidades reguladas no Brasil (Avenue Banco de Investimentos) e nos EUA (Avenue Securities, Avenue Cash). Qualquer novo uso, exportação ou compartilhamento de dados com PII de cliente (CPF, SSN, dados de conta e transação) deve considerar LGPD e as regras norte-americanas (Reg S-P, GLBA) e, em dúvida, ser validado com Compliance e Jurídico.

---

## Convenções (comuns a todos os projetos)

- **Idioma:** documentação e comentários em português; nomes de objetos de dados em inglês (padrão do warehouse).
- **Credenciais:** nunca commitadas. Use Secret Manager / service accounts / secrets do ambiente. Arquivos `.env` ficam no `.gitignore`.
- **Branches:** trabalho em `dev`, PRs para `main`.
- **Isolamento:** cada pasta-projeto é autocontida (dependências, testes e CI próprios) — não há acoplamento entre projetos na raiz.

## Como adicionar um projeto

1. Crie uma nova pasta na raiz com o nome do framework/pipeline (ex.: `data-ingestion-<time-ou-dominio>/`).
2. Inclua um `README.md` próprio descrevendo escopo, arquitetura e como rodar.
3. Registre o projeto na tabela [Projetos](#projetos) acima.
