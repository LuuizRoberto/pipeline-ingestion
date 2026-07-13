# pipelines

Pipelines ETL de ingestão de dados do time de FP&A & Risco. Cada pipeline extrai dados de uma API externa, normaliza, e faz **dual-write** em Delta Lake (Databricks) e BigQuery.

---

## Conteúdo

| Projeto | Descrição |
|---|---|
| [`finance_data_pipelines/`](finance_data_pipelines/) | Projeto Python de ETL para as fontes **Matera**, **Jira** e **Bacen**, com execução agendada via Databricks Jobs |

Hoje há um único projeto sob esta pasta. A estrutura permite adicionar novos projetos de pipeline lado a lado no futuro.

---

## finance_data_pipelines — resumo

Pipeline padronizado em torno de um `BaseExtractor`: cada fonte só implementa `get_api_data()` e `normalize_data()`, e herda automaticamente a escrita em Delta Lake, BigQuery e snapshot Parquet.

**Fontes ativas:**

| Fonte | O que ingere | Jobs |
|---|---|---|
| **Matera** | Dados contábeis da core banking (fatos contábeis, conta contábil, lançamentos, saldos) | [`jobs/matera/`](finance_data_pipelines/jobs/matera/) |
| **Jira** | Issues de providers para acompanhamento operacional | [`jobs/jira/`](finance_data_pipelines/jobs/jira/) |
| **Bacen** | Scraping do ranking de câmbio (script auxiliar) | [`scripts/bacen/`](finance_data_pipelines/scripts/bacen/) |
| **Validation** | Reconciliação Databricks × BigQuery | [`jobs/validation/`](finance_data_pipelines/jobs/validation/) |

**Camadas do projeto:**

```
finance_data_pipelines/
├── core/         infra compartilhada — base_extractor, bigquery_client, job_utils, logger
├── sources/      por fonte: config/ (parâmetros, credenciais) + extractor/ (lógica de extração)
├── jobs/         entrypoints executados pelo Databricks (run_*.py)
├── transfers/    transferência BigQuery → Databricks
├── scripts/      utilitários pontuais (ex.: scraping Bacen)
└── tests/        testes unitários + fixtures
```

> **Documentação completa** (arquitetura do core, dual-write, reconciliação, setup local, como rodar, como adicionar uma nova fonte e variáveis de ambiente): [`finance_data_pipelines/README.md`](finance_data_pipelines/README.md).

---

## Contexto de migração

Os pipelines rodam hoje no Databricks, mas a arquitetura **dual-write** já grava os mesmos dados no BigQuery. Quando o Databricks for descontinuado, basta remover as chamadas Databricks — sem reescrita de lógica. Detalhes na [estratégia de migração para GCP](finance_data_pipelines/README.md#estratégia-de-migração-para-gcp).

---

## Notas de segurança e dados

- Nenhuma credencial é versionada. Configure via `.env` (fora do Git) ou secrets do ambiente.
- Dados de conta e transação de clientes são **PII confidencial** (LGPD / Reg S-P / GLBA). Mantenha os dados nos ambientes autorizados (Delta Lake / BigQuery) e prefira identificadores internos em exports e análises.
