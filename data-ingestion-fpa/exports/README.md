# queries

Queries e procedures SQL de **FP&A & Risco** executadas sobre o data warehouse no **BigQuery** (`data-prod-295413`). Cobrem modelagem de tabelas fato/dimensão, cálculos regulatórios e exportações analíticas.

---

## Organização

| Pasta | Conteúdo |
|---|---|
| [`simple_exports/`](simple_exports/) | Scripts SQL versionados, um por arquivo, nomeados no padrão `YYYY-MM-DD - <descrição>.sql` |

**Convenção de nomes:** o prefixo de data indica quando o script foi criado/entregue; o mesmo comentário aparece na primeira linha do arquivo. Não é um agendamento — a execução recorrente acontece via BigQuery Scheduled Queries / procedures.

**Datasets utilizados:**

- `avenue_finance` — camada de FP&A (fatos, dimensões e tabelas Matera).
- `avenue_data_warehouse` — data warehouse dimensional (`fact_*`, `dim_*`).
- `avenue_datalake` / `avenue_datalake_latest_snapshot` / `avenue_datalake_layer2` — camada de data lake (dados brutos de Apex, Salesforce, banking movements).

---

## Catálogo — `simple_exports/`

### Modelagem e cargas (DDL / procedures)

| Arquivo | O que faz |
|---|---|
| `2026-03-02 - FactBankings.sql` | Procedure `LoadBankingFacts` — carrega fatos de banking a partir de movimentos, com controle de data de reprocessamento (a partir de 2025-01-01). |
| `2026-06-02 - DailyAvenueFinanceRefresh.sql` | Refresh diário de FP&A — reconstrói `fact_opad_calculate_data` a partir dos saldos/contas contábeis da Matera; controla janela de datas e contadores de linhas. |
| `2026-06-09 - Carga Inicial - dim_external_client.sql` | Criação e carga inicial da dimensão `dim_external_client` (cliente externo), com registro default (SK -1) e deduplicação por CPF via Salesforce. |
| `2026-06-09 - Criação Inicial - LoadFactRevenueFPNA.sql` | Criação da tabela `fact_revenue_fpna` a partir de `fact_revenue`, enriquecida com a dimensão de cliente externo. |
| `2026-06-10 - Calculo OPAD - Contas 7 & 8.sql` | Cálculo OPAD — agrega lançamentos contábeis das contas iniciadas em 7 e 8 (meses 6 e 12), ajustando sinal por tipo de lançamento (C/D). |

### Modelo de ML

| Arquivo | O que faz |
|---|---|
| `2026-06-01 - Modelo Ordens - Câmbio - Feature Recorrencia.sql` | Cria o modelo BigQuery ML `model_cambio_ordens` (regressão linear) relacionando volume de ordens e volume de câmbio USD mensal. |

### Análise / reconciliação

| Arquivo | O que faz |
|---|---|
| `2026-06-15 - Conciliation Querie Analysis.sql` | Análise de conciliação de balance entre janelas de datas parametrizadas (`start_date` / `end_date`) com cutoffs de settlement. |
| `2026-06-16 - Covered Call Clients.sql` | Identifica clientes com estratégia de covered call a partir das posições de custódia Apex, com câmbio PTAX. |

### Exports (KPIs de contas e fluxo)

| Arquivo | O que faz |
|---|---|
| `2026-07-01 - Export Net Inflow Bank Invest.sql` | Net inflow por mês, segmentado em Banking × Invest e por tipo de movimento. |
| `2026-07-01 - Export New Accounts Invest.sql` | Contagem mensal de novos clientes Invest (exclui canal ITAU WHITE LABEL). |
| `2026-07-01 - Export New Banking Accounts.sql` | Contagem mensal de novas contas de banking abertas. |
| `2026-07-01 - Export Total Accounts Banking.sql` | Total de clientes de banking na última data, separando com/sem saldo de custódia. |
| `2026-07-01 - Export Total Accounts Invest.sql` | Total de clientes ativos Invest na última data, com saldo Balance vs Investido. |
| `2026-07-01 - Export Total Accounts Invest Segment.sql` | Segmentação de clientes Invest por composição de saldos (Balance / Invest / ambos). |
| `2026-07-01 - Export Total CPF Distinct Avenue.sql` | Total de CPFs distintos com custódia positiva na última data. |

---

## Notas de dados e compliance

- Várias queries acessam **PII de cliente** (CPF, contas, saldos, transações) — dados confidenciais sob LGPD e Reg S-P / GLBA. Ao exportar resultados, mantenha os dados em ambientes autorizados e prefira agregações ou identificadores internos.
- Os objetos abrangem entidades sob **dupla jurisdição** (Brasil e EUA). Cálculos com finalidade regulatória (ex.: OPAD, conciliação) devem ser validados com as áreas responsáveis antes de uso oficial.
- Estas queries **não constituem recomendação de investimento** — são insumos analíticos internos de FP&A e Risco.
