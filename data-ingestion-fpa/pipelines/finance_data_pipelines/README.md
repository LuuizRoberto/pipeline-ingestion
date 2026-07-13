# finance_data_pipelines

ETL pipelines para ingestão de dados financeiros — extração via API, versionamento Git e execução diária no Databricks.

---

## Índice

- [Visão geral](#visão-geral)
- [Arquitetura do Core](#arquitetura-do-core)
  - [Fluxo completo de extração](#fluxo-geral-de-extração)
  - [Como funciona o `save_to_delta_table`](#como-funciona-o-save_to_delta_table)
  - [Como funciona o `_write_bigquery`](#como-funciona-o-_write_bigquery)
  - [Como funciona o `_write_snapshot`](#como-funciona-o-_write_snapshot)
  - [Como funciona o `ExtractorJira`](#como-funciona-o-extractorjira)
  - [Como funcionam os Jobs](#como-funcionam-os-jobs-jobs)
- [Observabilidade e Validação](#observabilidade-e-validação)
  - [Log de ingestão — `data_ingestion_results`](#log-de-ingestão--data_ingestion_results)
  - [Reconciliação Databricks vs BigQuery](#reconciliação-databricks-vs-bigquery)
- [Estratégia de migração para GCP](#estratégia-de-migração-para-gcp)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Fontes de dados](#fontes-de-dados)
- [Transfers — BigQuery → Databricks](#transfers--bigquery--databricks)
- [Setup local](#setup-local)
- [Como rodar](#como-rodar)
- [Testes](#testes)
- [Conectar ao Databricks](#conectar-ao-databricks)
- [Fluxo de branches](#fluxo-de-branches)
- [Adicionando uma nova fonte](#adicionando-uma-nova-fonte)
- [Variáveis de ambiente](#variáveis-de-ambiente)

---

## Visão geral

Este repositório centraliza os pipelines de ingestão de dados do time de Finance. Cada pipeline:

1. Bate em uma API externa (ex: Matera, Jira)
2. Normaliza e deduplica os dados
3. Salva no **Delta Lake** via Merge (upsert por primary key)
4. Salva no **BigQuery** via staging table + MERGE DML (dual-write)
5. Grava um snapshot **Parquet** completo da tabela (Volume Databricks hoje, GCS no futuro)

A execução é agendada via **Databricks Jobs**, apontando diretamente para os arquivos em `jobs/`.

> **Contexto de migração:** o Databricks será desligado no futuro. A arquitetura dual-write garante que o BigQuery já cresce com dados reais desde hoje — quando a migração acontecer, basta remover as chamadas Databricks do pipeline sem qualquer reescrita de lógica.

---

## Arquitetura do Core

### Fluxo geral de extração

Todo extractor segue o mesmo fluxo, orquestrado por `run_extraction()` em `core/base_extractor.py`:

```
run_extraction()
    │
    ├── get_api_data()          ← implementado por cada extractor (Matera, Jira, ...)
    │
    ├── normalize_data()        ← implementado por cada extractor
    │
    ├── save_to_delta_table()   ← herdado, escreve no Delta Lake + BigQuery
    │       ├── _write_databricks()   → MERGE no Delta Lake
    │       └── _write_bigquery()     → MERGE no BigQuery (best-effort)
    │
    └── _write_snapshot()       ← lê tabela COMPLETA do Delta e grava Parquet
            ├── Volume Databricks  → sempre
            └── GCS                → quando gcs_snapshot_path estiver configurado
```

`get_api_data` e `normalize_data` são **abstratos** — cada fonte implementa do seu jeito. Os demais são **concretos e compartilhados** — qualquer nova fonte ganha os três destinos automaticamente.

---

### Como funciona o `save_to_delta_table`

Este é o método responsável por tudo que acontece depois que os dados chegam normalizados. Ele **não executa lógica diretamente** — delega cada etapa para um método privado especializado e controla a ordem de execução.

#### Diagrama de execução

```
save_to_delta_table(records, table_name)
    │
    ├── _build_dataframe()          → constrói o DataFrame Spark a partir dos registros
    │
    ├── tableExists(target_table)   → pergunta ao catálogo se a tabela já existe (UMA vez)
    │                                  resultado guardado em `table_exists`
    │
    ├── [se tabela EXISTE]
    │       └── _align_schema()     → alinha o DataFrame ao schema atual da tabela
    │
    ├── [se tabela NÃO EXISTE]
    │       └── _coerce_numeric_types()  → corrige tipos ambíguos (Long → Double)
    │
    ├── dropDuplicates(pk_list)     → deduplica por primary key
    │
    ├── _write_databricks()         → MERGE (upsert) ou CREATE TABLE no Delta Lake
    │
    └── _write_bigquery()           → MERGE no BigQuery via staging table (best-effort)
```

#### O que cada método privado faz

**`_align_schema(df, target_table) → DataFrame`**

Chamado quando a tabela **já existe**. Compara as colunas do DataFrame novo com as da tabela existente:
- Se tem coluna nova que a tabela não tem → executa `ALTER TABLE ADD COLUMNS` (preserva todo o histórico — nunca usa `overwrite`)
- Alinha tipos e ordem das colunas para o merge não rejeitar o DataFrame
- Retorna o DataFrame realinhado

**`_coerce_numeric_types(df, numeric_id_columns) → DataFrame`**

Chamado quando a tabela **ainda não existe**. Percorre as colunas do DataFrame e converte `long/int/integer` para `double` — evita conflito de tipos em colunas financeiras onde o Spark pode inferir `Long` mas a tabela precisa de `Double`. Colunas listadas em `numeric_id_columns` no config são excluídas (são IDs e contadores que devem permanecer inteiros).
Retorna o DataFrame com os tipos corrigidos.

**`_write_databricks(df, target_table, merge_condition, table_exists) → None`**

Recebe o DataFrame já preparado e faz a escrita no Delta Lake:
- Se `table_exists = True` → executa `MERGE` (upsert: atualiza existentes, insere novos)
- Se `table_exists = False` → executa `CREATE TABLE` via `df.write.saveAsTable`

**`_write_bigquery(df, bq_table, merge_condition) → None`**

Escreve no BigQuery via staging table + MERGE DML. Detalhes na seção [Como funciona o `_write_bigquery`](#como-funciona-o-_write_bigquery).

#### Por que alguns retornam `DataFrame` e outros `None`?

| Método | Transforma o `df`? | Retorna |
|---|---|---|
| `_align_schema` | Sim — adiciona colunas, alinha tipos e ordem | `DataFrame` |
| `_coerce_numeric_types` | Sim — converte tipos numéricos ambíguos | `DataFrame` |
| `_write_databricks` | Não — só escreve no Delta | `None` |
| `_write_bigquery` | Não — só escreve no BQ | `None` |

Os dois primeiros precisam devolver o DataFrame modificado para que o orquestrador possa continuar. Os dois últimos são escritas finais — não há nada a devolver.

---

### Adicionando `numeric_id_columns` no config

Quando uma tabela nova é criada, o Spark pode inferir `Long` para colunas que deveriam ser `Double` (ex: valores financeiros que chegam como inteiro por terem casas zeradas). Para evitar isso, declare no `table_config` quais colunas são IDs/contadores e **não** devem ser convertidas:

```python
"accounting_facts": {
    ...
    "numeric_id_columns": ["releasenumber"],  # ← não converte este para Double
}
```

Tabelas sem essa chave simplesmente não excluem nenhuma coluna do cast.

---

### Como funciona o `_write_bigquery`

O BigQuery não aceita MERGE direto de uma lista Python — precisa que os dois lados sejam tabelas. O fluxo usa uma **staging table temporária**:

```
records (batch extraído)
    │
    ▼
staging table              ← tabela temporária criada na hora
(avenue_matera_api_accounting_facts_staging_20260602090352)
    │
    ▼
MERGE staging → target     ← BigQuery lê das duas tabelas e faz o upsert
(avenue_matera_api_accounting_facts)
    │
    ▼
staging deletada           ← limpeza automática
```

#### Schema explícito — sem autodetect

O BigQuery tem um comportamento não-determinístico com `autodetect=True`: o mesmo valor Python pode ser inferido como tipos diferentes entre execuções. Para garantir que o schema da staging seja sempre compatível com o target, o método:

- **Colunas existentes no target**: usa o tipo exato do target (garante que `t.col = s.col` no MERGE nunca gera type mismatch)
- **Colunas novas**: infere do valor Python (`bool` → BOOL, `int` → INT64, `float` → FLOAT64, resto → STRING)

#### Primeira carga vs. execuções subsequentes

| Situação | Comportamento |
|---|---|
| Target não existe | Copia staging para o target diretamente (`copy_table`) |
| Target existe, sem colunas novas | MERGE com schema da staging = schema do target |
| Target existe, com colunas novas | `ALTER TABLE ADD COLUMN` antes do MERGE |

#### Best-effort

Se qualquer etapa do BigQuery falhar (erro de rede, permissão, rate limit), o pipeline Databricks **não é afetado** — a exceção é capturada, logada como `WARNING` e a execução continua normalmente.

---

### Como funciona o `_write_snapshot`

Diferente do que acontecia antes (que salvava só o batch ingerido), o `_write_snapshot` lê a **tabela completa** do Databricks após o merge:

```python
full_df = self.spark.table(target_table)  # lê o estado atual COMPLETO da tabela
```

Isso garante que o Parquet representa fielmente o estado da tabela naquele dia e pode ser usado para rollback ou auditoria histórica.

O snapshot é gravado em dois lugares (quando configurados):

```
_write_snapshot()
    ├── Volume Databricks    → sempre
    │   /Volumes/.../dl_export_date=YYYY-MM-DD/
    │
    └── GCS                  → quando gcs_snapshot_path estiver preenchido no config
        gs://bucket/tabela/dl_export_date=YYYY-MM-DD/
```

#### Ativando o GCS

Quando você tiver acesso ao bucket, basta preencher no config da tabela:

```python
"accounting_facts": {
    ...
    "gcs_snapshot_path": "gs://finance-data/matera/accounting_facts",  # antes era None
}
```

Zero mudança de código — o `_write_snapshot` verifica a presença do campo e grava automaticamente.

#### DELETE por janela — sincronizando exclusões com o BQ

Algumas tabelas usam a estratégia **DELETE por janela + INSERT** para lidar com registros que foram removidos na origem (ex: guias de `lancamento_contabil` canceladas no Matera). Um MERGE puro nunca remove registros — ele só atualiza ou insere.

Para manter o BigQuery sincronizado, os jobs chamam `delete_bq_window()` logo após o DELETE do Databricks, com a mesma condição WHERE:

```python
# Databricks DELETE — como sempre foi
spark.sql(f"DELETE FROM {target_table} WHERE idempresa = '{id_emp}' AND numguia >= {start_guia}")

# BQ DELETE — espelha a mesma janela
delete_bq_window(
    bq_client=extractor.bq_client,
    bq_table=table_config["bigquery_table_target"],
    where_clause=f"idempresa = '{id_emp}' AND ano = '{ano_alvo}' AND numguia >= {start_guia}",
)
```

Tabelas afetadas: `accounting_facts`, `saldo_contabil`, `lancamento_contabil`.

---

### Como funcionam os configs de destino

Cada tabela declara explicitamente seus três caminhos de persistência:

```python
"accounting_facts": {
    "databricks_table_target": "avenue_finance.matera.accounting_facts",
    "bigquery_table_target":   "data-prod-295413.avenue_finance.avenue_matera_api_accounting_facts",
    "volume_path":             "/Volumes/avenue_finance/default/data-finance/matera/accounting_facts",
    "gcs_snapshot_path":       None,  # preencher quando tiver acesso ao bucket
}
```

| Campo | Descrição | Quando mudar |
|---|---|---|
| `databricks_table_target` | Tabela Delta no Databricks | Migração de namespace |
| `bigquery_table_target` | Tabela no BigQuery (`project.dataset.table`) | Mudança de projeto ou dataset |
| `volume_path` | Path do Volume Databricks para o snapshot | Reorganização de Volumes |
| `gcs_snapshot_path` | Path GCS para o snapshot (`gs://bucket/path`) | Quando o bucket estiver disponível |
| `bq_project` | Projeto GCP do BigQuery (independente do `project_id` do Secret Manager) | Migração de projeto GCP |
| `bq_dataset` | Dataset do BigQuery | Reorganização de datasets |

---

### Como funciona o `ExtractorJira`

O Jira tem particularidades que exigem decisões diferentes das outras fontes. Esta seção explica as três principais.

---

#### Metadados de campos e o índice reverso (M11)

O Jira não trabalha com nomes de campos diretos — ele usa IDs internos como `customfield_10200`. Para buscar o campo `"reporter"`, você precisa primeiro descobrir qual é o ID dele.

No `__init__`, o extractor chama `_fetch_field_metadata()` uma única vez. Esse método retorna **dois dicionários**:

```
field_metadata      → { field_id: { id, name_normalizado, ... } }
_field_name_to_id   → { nome_normalizado: field_id }   ← índice reverso
```

O índice reverso existe para que `_resolve_field_ids()` consiga converter nomes em IDs de forma instantânea. Sem ele, cada conversão percorreria todos os ~300 campos do Jira. Com ele, é uma consulta direta no dicionário.

```python
# Sem índice reverso — O(n×m): para cada campo, percorre todos os metadados
for field_name_target in fields_table:
    for field_id, meta in field_metadata.items():   # ~300 campos
        if field_name_target == meta["name"]: ...

# Com índice reverso — O(1): lookup direto
field_ids = [self._field_name_to_id[name] for name in fields_table]
```

---

#### Schema do DataFrame — união de todos os registros (C4)

Campos customizados do Jira são opcionais — uma issue pode ter `"assignee"` preenchido e outra não. Por isso o schema não pode ser inferido só do primeiro registro.

O `_build_dataframe()` percorre **todos os registros** para construir o conjunto completo de campos antes de criar o DataFrame:

```
records[0] → { "issue_id": "PRV-1", "created": "2024-01-01", "assignee": None  }
records[1] → { "issue_id": "PRV-2", "created": "2024-01-02", "assignee": "João" }
records[2] → { "issue_id": "PRV-3", "created": "2024-01-03", "valor": 500.0     }

União de campos → { "issue_id", "created", "assignee", "valor" }
Schema final    → todos os 4 campos estão no DataFrame
```

Após montar o schema, cada registro é normalizado com `None` para os campos que não possui — garantindo que o `createDataFrame` não rejeite registros com colunas faltando.

---

#### Resiliência HTTP — Session com retry (C3)

Todas as chamadas HTTP do `ExtractorJira` usam `self.session` — uma `requests.Session` configurada com retry exponencial:

| Parâmetro | Valor |
|---|---|
| Total de tentativas | 5 |
| Backoff factor | 1s (1s, 2s, 4s, 8s, 16s) |
| Status codes com retry | 429, 500, 502, 503, 504 |
| Timeout por chamada | 60s |

Isso vale para a busca de metadados (`_fetch_field_metadata`) e para todas as páginas de issues (`get_api_data`). Uma falha de rede transiente não derruba o job.

---

### Como funcionam os Jobs (`jobs/`)

Os jobs são os entry points executados pelo Databricks. Esta seção explica três decisões de design da camada de jobs.

---

#### Parsing de argumentos centralizado — `parse_reprocess_args` (M13)

Todos os jobs que suportam reprocessamento precisam ler os parâmetros passados pelo Databricks no formato `key=value`:

```
["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
```

Em vez de duplicar esse bloco de parsing em cada job, a função `parse_reprocess_args()` em `core/job_utils.py` centraliza essa lógica. Todos os jobs chamam:

```python
reprocess_start, reprocess_end = parse_reprocess_args()

if reprocess_start and reprocess_end:
    # modo reprocessamento — usa as datas passadas via parâmetro
else:
    # modo diário padrão — calcula as datas automaticamente
```

A função aceita um parâmetro `argv` opcional — em produção usa `sys.argv[1:]` automaticamente, em testes recebe a lista diretamente sem precisar mockar variável global.

---

#### Janela de guias configurável — `guia_lookback` e `guia_lookahead` (M14)

O job `run_lancamento_contabil` coleta dados por número de guia, não por data. A estratégia é: pegar o maior número de guia já salvo, voltar alguns para trás (segurança contra atualizações tardias) e coletar até alguns à frente (para capturar guias novas).

```
guia 1 ──── guia 850 (max_atual na tabela) ──── guia 950 ──── guia 1050
                  │
     max_atual - guia_lookback (100) = guia 750 = start_guia
                  │
     start_guia + guia_lookahead (200) = guia 950 = end_guia
                  │
     janela coletada: guias 750 → 950
```

Esses valores ficam no `MATERA_CONFIG` com nomes descritivos:

```python
"lancamento_contabil": {
    "guia_lookback":  100,  # guias para trás a partir do max atual
    "guia_lookahead": 200,  # guias para frente a partir do ponto de início
}
```

Para ajustar a janela de reprocessamento, basta alterar o config — sem tocar no código do job.

---

#### Validação de SQL injection — `validate_sql_identifier` (M15)

Os jobs constroem queries DELETE com f-string, interpolando `company_code` e `id_empresa` diretamente na string SQL. Mesmo que esses valores venham do config interno, interpolação sem validação é uma vulnerabilidade:

```python
# se o config tivesse um valor malicioso...
"company_code": ["1; DROP TABLE avenue_finance.matera.accounting_facts; --"]

# ...a query gerada seria destrutiva:
DELETE FROM avenue_finance.matera.accounting_facts
WHERE companycode = '1; DROP TABLE avenue_finance.matera.accounting_facts; --'
```

A função `validate_sql_identifier()` em `core/job_utils.py` garante que apenas strings numéricas chegam ao SQL:

```python
validate_sql_identifier("328", "company_code")   # → OK, retorna "328"
validate_sql_identifier("1; DROP TABLE ...", "company_code")
# → ValueError: Valor inválido para 'company_code'. Apenas strings numéricas são aceitas.
```

A validação acontece uma vez por empresa, antes de qualquer interpolação, em todos os jobs que executam DELETE.

> **Nota:** a solução ideal seria queries parametrizadas (`spark.sql("DELETE ... WHERE id = ?", [value])`), mas o suporte no Databricks/Spark SQL é limitado dependendo da versão do cluster. A validação com `re.fullmatch(r"\d+", value)` é a alternativa pragmática que garante o mesmo nível de segurança para os tipos de dados usados neste projeto.

---

## Observabilidade e Validação

---

### Log de ingestão — `data_ingestion_results`

Após cada chamada a `run_extraction()`, independentemente de sucesso ou falha, o `BaseExtractor` grava automaticamente um registro na tabela `avenue_finance.data_ingestion_results` no BigQuery.

**Schema:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `export_date` | STRING (YYYY-MM-DD) | Data de referência da extração |
| `table_name` | STRING | Nome completo da tabela Databricks (ex: `avenue_finance.matera.saldo_contabil`) |
| `record_count` | INT64 | Quantidade de registros do batch ingerido (0 em caso de falha ou sem dados) |
| `resultado_ingestao` | STRING | `"Sucesso"` ou `"Falha"` |
| `error_message` | STRING | Mensagem da exceção em caso de falha; `null` em caso de sucesso |

**Exemplo de saída:**

```
export_date | table_name                             | record_count | resultado_ingestao | error_message
2026-06-02  | avenue_finance.matera.saldo_contabil   | 224          | Sucesso            | null
2026-06-02  | avenue_finance.matera.accounting_facts | 0            | Falha              | HTTPError: 503 Service Unavailable
```

O log é gravado via `try/finally` no `run_extraction()` — garante registro mesmo quando a extração falha e lança exceção. Best-effort: falha no próprio log não afeta o pipeline.

---

### Reconciliação Databricks vs BigQuery

O script `jobs/validation/run_reconciliation.py` roda como task separada no Databricks após todos os jobs de ingestão do dia. Ele gera dois SQLs `UNION ALL` — um para o BigQuery e outro para o Databricks — e compara os resultados via join Spark.

#### Como funciona

**1. Geração dos SQLs**

`_build_bq_sql()` e `_build_databricks_sql()` percorrem os configs de todas as fontes e geram um SQL `UNION ALL` com uma linha por métrica:

```sql
-- BigQuery (gerado automaticamente a partir do config)
SELECT
    'avenue_finance.matera.accounting_facts' AS tabela,
    'quantidade'                              AS metrica,
    CAST(COUNT(*) AS FLOAT64)                 AS total
FROM `data-prod-295413.avenue_finance.avenue_matera_api_accounting_facts`
UNION ALL
SELECT
    'avenue_finance.matera.accounting_facts',
    'valor(amount)',
    CAST(COALESCE(SUM(amount), 0) AS FLOAT64)
FROM `data-prod-295413.avenue_finance.avenue_matera_api_accounting_facts`
UNION ALL
...
```

**2. Execução e join**

```
bq_df   ← bq_client.query(bq_sql)    → Spark DataFrame
db_df   ← spark.sql(db_sql)          → Spark DataFrame
result  ← db_df.join(bq_df, on=[tabela, metrica])
        → adiciona colunas resultado e diferenca
```

**3. Resultado**

```
tabela                                | metrica         | databricks  | bigquery    | resultado | diferenca
avenue_finance.matera.saldo_contabil  | quantidade      | 3547.0      | 3547.0      | OK        | 0.0
avenue_finance.matera.saldo_contabil  | valor(vlrsaldo) | 8750000.0   | 8750000.0   | OK        | 0.0
avenue_finance.matera.accounting_facts| quantidade      | 150000.0    | 149998.0    | WARNING   | 2.0
```

- `resultado`: `"OK"` se `diferenca < 0.01`, `"WARNING"` caso contrário
- `diferenca`: valor absoluto da diferença entre os dois destinos

#### Adicionando colunas financeiras para comparação

Para incluir SUM de colunas de valor em novas tabelas, declare `value_columns` no `table_config`:

```python
"saldo_contabil": {
    ...
    "value_columns": ["vlrsaldo"],  # SUM desta coluna entra na reconciliação
}
```

Tabelas sem `value_columns` só têm a linha de `quantidade` comparada.

---

## Estratégia de migração para GCP

O Databricks será desligado no futuro. A arquitetura foi desenhada para tornar essa migração o mais simples possível.

### Estado atual

```
run_extraction()
    ├── save_to_delta_table()
    │       ├── _write_databricks()   → Delta Lake       (Databricks)
    │       └── _write_bigquery()     → BigQuery         (GCP) ← já ativo
    └── _write_snapshot()
            ├── Volume Databricks     → Parquet           (Databricks)
            └── GCS                  → Parquet           (GCP) ← ativar quando tiver bucket
```

### O que muda quando o Databricks for desligado

**Em `run_extraction()` — remover:**
```python
self.save_to_delta_table(normalized, table_name)   # ← remove (Delta Lake)
if normalized:
    self._write_snapshot(table_name)               # ← remove (Volume Databricks)
```

**Nos jobs — remover os DELETEs do Spark:**
```python
spark.sql(delete_sql)    # ← remove
delete_bq_window(...)    # ← fica (continua sendo necessário no BQ)
```

**O que permanece intacto:**
```python
self._write_bigquery(df, bq_table, merge_condition)   # ← continua
# _write_snapshot com gcs_snapshot_path               # ← continua (GCS)
```

### Checklist de migração

- [ ] Preencher `gcs_snapshot_path` em todos os `table_config` com o path do bucket GCS
- [ ] Validar que os dados do BigQuery estão completos e com tipos corretos
- [ ] Remover chamada `save_to_delta_table()` do `run_extraction()`
- [ ] Remover bloco `_write_snapshot()` do `run_extraction()` (ou adaptar para GCS only)
- [ ] Remover `spark.sql(delete_sql)` dos jobs (manter `delete_bq_window`)
- [ ] Migrar agendamento de Databricks Jobs para Cloud Composer / Cloud Run / outro orquestrador GCP
- [ ] Remover dependências PySpark e Delta Lake do `pyproject.toml`

---

## Estrutura do projeto

> A pasta vive em `pipelines/finance_data_pipelines/` a partir da raiz do repositório `data-fpa-risk`. Os imports usam o caminho completo `pipelines.finance_data_pipelines.<pacote>` (ver [Setup local](#setup-local)).

```
pipelines/finance_data_pipelines/
│
├── core/                              # Camada base — compartilhada por todas as fontes
│   ├── __init__.py
│   ├── base_extractor.py              # Classe abstrata: contrato para todo extractor
│   ├── bigquery_client.py             # Fábrica do BigQuery client (local via Databricks secrets / produção via ADC)
│   ├── job_utils.py                   # Utilitários compartilhados: parse_reprocess_args, validate_sql_identifier, delete_bq_window
│   └── logger.py                      # Logger padronizado do projeto
│
├── sources/                           # Lógica específica por fonte de dados
│   ├── matera/                        # Fonte: Matera (API contábil)
│   │   ├── config/
│   │   │   └── config.py              # Endpoints, tabelas alvo, primary keys, value_columns
│   │   └── extractor/
│   │       └── extractor.py           # ExtractorMatera — herda BaseExtractor
│   └── jira/                          # Fonte: Jira (gestão de issues)
│       ├── config/
│       │   └── config.py              # Projeto, campos, changed_range, tabelas alvo
│       └── extractor/
│           └── extractor.py           # ExtractorJira — herda BaseExtractor (retry HTTP, índice reverso de campos)
│
├── jobs/                              # Entry points executados pelo Databricks
│   ├── matera/
│   │   ├── run_accounting_facts.py    # Job diário — accounting_facts (paginado, multi-empresa, delete por janela)
│   │   ├── run_saldo_contabil.py      # Job diário — saldo_contabil (delete por janela mensal, multi-empresa)
│   │   ├── run_conta_contabil.py      # Job diário — conta_contabil (merge por cod_conta_contabil)
│   │   └── run_lancamento_contabil.py # Job diário — lancamento_contabil (janela de guias)
│   ├── jira/
│   │   └── run_issues_providers.py    # Job incremental — issues do projeto PROVIDER (janela de dias)
│   └── validation/
│       └── run_reconciliation.py      # Job de validação — compara contagens e somas Databricks vs BigQuery
│
├── transfers/                         # Cópia reversa: BigQuery → Databricks Delta Lake
│   ├── config.py                      # Lista de tabelas a copiar (bq_table, databricks_table, mode)
│   └── run_bq_to_databricks.py        # Job Databricks — lê do BQ via conector Spark e grava no Delta
│
├── tests/
│   ├── unit/                          # Testes sem dependências externas (pytest + mocks)
│   │   ├── test_extractor_matera.py
│   │   ├── test_extractor_jira.py
│   │   ├── test_bigquery_client.py
│   │   └── test_job_utils.py
│   └── fixtures/
│       └── matera_responses.py        # Respostas mock da API Matera
│
├── scripts/                           # Scripts manuais — rodam localmente, fora do Databricks
│   └── bacen/
│       ├── scrape_ranking_cambio.py   # Web scraping ranking de câmbio (Bacen → BigQuery)
│       └── README.md                  # Instruções de uso e contexto
│
├── .github/
│   └── workflows/
│       └── ci.yml                     # CI: lint + testes em todo PR para main
│
├── .vscode/
│   ├── settings.json                  # Formatter, testes, exclusões
│   ├── extensions.json                # Extensões recomendadas
│   └── launch.json                    # Debug configs para jobs e testes
│
├── .env.example                       # Variáveis de ambiente necessárias (não commitar .env)
├── .gitignore
├── CONTRIBUTING.md                    # Guia de contribuição e convenções
├── Makefile                           # Atalhos: make test, make lint, make install
├── pyproject.toml                     # Dependências e configuração do projeto
└── README.md
```

---

## Fontes de dados

### Matera (contábil)

API REST que expõe dados contábeis da Avenue. Autenticação via OAuth2 com credenciais no **GCP Secret Manager**.

| Tabela | Job | Estratégia | Frequência |
|---|---|---|---|
| `accounting_facts` | `run_accounting_facts.py` | Paginação while + multi-empresa + delete por janela | Diária |
| `saldo_contabil` | `run_saldo_contabil.py` | Delete por janela mensal + insert, multi-empresa | Diária |
| `conta_contabil` | `run_conta_contabil.py` | Merge por `cod_conta_contabil` (IDs via arquivo `.txt`) | Diária |
| `lancamento_contabil` | `run_lancamento_contabil.py` | Janela de guias incremental + delete por guia | Diária |

### Jira (gestão de issues)

API REST do Jira Cloud. Extração incremental por data de modificação (`changed`). Autenticação via API Token com credenciais no **Databricks Secret Scope**.

| Tabela | Job | Estratégia | Frequência |
|---|---|---|---|
| `issues` (projeto PROVIDER) | `run_issues_providers.py` | Janela de dias configurável via `changed_range` | Diária |

### Bacen (câmbio)

Web scraping do ranking mensal de câmbio por instituição financeira publicado pelo Banco Central. **Roda apenas localmente** — a API do Bacen bloqueia IPs de datacenter/cloud.

| Tabela | Script | Estratégia | Frequência |
|---|---|---|---|
| `avenue_bacen_api_ranking_cambio_instituicao_financeiras` | `scripts/bacen/scrape_ranking_cambio.py` | Delete + insert por ano | Manual (mensal) |

Veja [scripts/bacen/README.md](scripts/bacen/README.md) para detalhes de execução.

---

## Transfers — BigQuery → Databricks

O módulo `transfers/` faz o **fluxo inverso**: copia tabelas do BigQuery para o Databricks Delta Lake. É usado para disponibilizar no Databricks tabelas que são produzidas diretamente no GCP (ex: modelos FP&A, tabelas de dimensão geradas por dbt ou pipelines GCP).

### Como funciona

```
BigQuery (tabela de origem)
    │
    ▼
Spark BigQuery Connector    ← lê via conector nativo do cluster Databricks
    │
    ▼
Delta Lake (tabela destino) ← sobrescreve (overwrite) ou acrescenta (append)
```

O job roda no Databricks, lê cada tabela listada em `transfers/config.py` e grava no Delta Lake. O schema é detectado automaticamente — `overwriteSchema=true` garante que mudanças de schema não quebram a execução.

### Configuração

Adicionar uma nova tabela é só incluir um dict em `transfers/config.py`:

```python
TABLES = [
    {
        "bq_table":         "data-prod-295413.avenue_finance.fact_revenue_fpna",
        "databricks_table": "avenue_finance.fpna.fact_revenue_fpna",
        "mode": "overwrite",   # ou "append"
    },
]
```

| Campo | Descrição |
|---|---|
| `bq_table` | Tabela BigQuery no formato `project.dataset.table` |
| `databricks_table` | Tabela Delta no formato `catalog.schema.table` |
| `mode` | `"overwrite"` (substitui tudo) ou `"append"` (acrescenta). Padrão: `"overwrite"` |

### Tabelas atualmente sincronizadas

| BigQuery | Databricks | Mode |
|---|---|---|
| `avenue_finance.fact_revenue_fpna` | `avenue_finance.fpna.fact_revenue_fpna` | overwrite |
| `avenue_finance.dim_external_client` | `avenue_finance.fpna.dim_external_client` | overwrite |

### Agendamento

Job Databricks independente apontando para `transfers/run_bq_to_databricks.py`. Sem parâmetros. Se qualquer tabela falhar, o job termina com exit code 1 e as demais continuam sendo processadas.

---

## Setup local

### Pré-requisitos

- Python 3.10+
- Git
- Acesso ao GCP (para Secret Manager)

### Instalação

```bash
# Clone o repositório
git clone https://github.com/avenuesec/data-fpa-risk
cd data-fpa-risk

# Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate        # Linux / Mac
.venv\Scripts\activate           # Windows

# Instale as dependências (incluindo dev)
# O install editável expõe o pacote pipelines.finance_data_pipelines,
# resolvendo os imports por caminho completo em qualquer diretório.
pip install -e ".[dev]"

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com seus valores
```

### Extensões recomendadas no VSCode

O arquivo `.vscode/extensions.json` já lista as extensões. Ao abrir o projeto, o VSCode vai sugerir instalá-las automaticamente:

| Extensão | Função |
|---|---|
| `charliermarsh.ruff` | Linter e formatter Python |
| `ms-python.python` | Suporte Python |
| `ms-python.vscode-pylance` | IntelliSense e type checking |
| `eamodio.gitlens` | Histórico e blame do Git |
| `github.vscode-pull-request-github` | PRs direto no VSCode |

---

## Como rodar

### Via Makefile (recomendado)

```bash
make install   # instala dependências
make test      # roda testes unitários
make lint      # verifica estilo de código
make fmt       # formata o código
```

### Via terminal

```bash
# Rode a partir da raiz do repositório (data-fpa-risk), com `pip install -e .` já feito.
BASE=pipelines/finance_data_pipelines

# Jobs Matera (requerem SparkSession ativa no Databricks)
python $BASE/jobs/matera/run_accounting_facts.py
python $BASE/jobs/matera/run_saldo_contabil.py
python $BASE/jobs/matera/run_conta_contabil.py
python $BASE/jobs/matera/run_lancamento_contabil.py

# Job Jira
python $BASE/jobs/jira/run_issues_providers.py

# Job de validação (roda após os jobs de ingestão)
python $BASE/jobs/validation/run_reconciliation.py

# Transfer BQ → Databricks
python $BASE/transfers/run_bq_to_databricks.py

# Scripts locais (rodam na sua máquina)
python $BASE/scripts/bacen/scrape_ranking_cambio.py                          # ano atual
python $BASE/scripts/bacen/scrape_ranking_cambio.py --ano-inicio 2023 --ano-fim 2026
```

### Via VSCode (debug)

O arquivo `.vscode/launch.json` tem configurações prontas. Pressione `F5` e escolha o job ou os testes que quer executar.

---

## Testes

Os testes ficam em `pipelines/finance_data_pipelines/tests/unit/` e não requerem conexão com APIs ou Spark — usam mocks. Rode a partir da raiz do repositório (o `testpaths` no `pyproject.toml` já aponta para lá — basta `pytest`).

```bash
# Rodar todos os testes
pytest -v

# Arquivo específico
pytest pipelines/finance_data_pipelines/tests/unit/test_extractor_matera.py -v

# Com cobertura
pytest --cov=pipelines.finance_data_pipelines
```

O CI roda os testes automaticamente em todo Pull Request para `main`.

---

## Conectar ao Databricks

### 1. Configurar Git Integration

```
Databricks → User Settings → Git Integration
→ Conectar com GitHub via OAuth ou Personal Access Token
```

### 2. Adicionar o Repo

```
Workspace → Repos → Add Repo
→ URL: https://github.com/avenuesec/data-fpa-risk
→ Branch: main
```

### 3. Configurar o Job

```
Workflows → Create Job → Add Task
→ Type: Python Script
→ Path: pipelines/finance_data_pipelines/jobs/matera/run_accounting_facts.py
→ Source: Git (repo acima, branch main)
```

Repita para cada job, ajustando o `Path` para o entrypoint correspondente.

> **Requisito:** cluster em **Databricks Runtime 14.3+**. A partir dessa versão a raiz da Git folder (a raiz do repo) entra automaticamente no `sys.path`, o que permite os imports por caminho completo `pipelines.finance_data_pipelines.<pacote>` sem nenhum ajuste de path no código. Em runtimes anteriores seria necessário adicionar a raiz manualmente via `sys.path`.

### 4. Dependências no cluster

Adicione nas Libraries do cluster via PyPI:

```
google-cloud-secret-manager
google-cloud-bigquery
google-cloud-storage
google-cloud-datastore
```

---

## Fluxo de branches

```
feat/minha-mudanca  →  dev  →  PR  →  main
```

| Branch | Papel |
|---|---|
| `main` | Produção — o que o Databricks executa |
| `dev` | Integração — validação antes de ir para main |
| `feat/*` | Desenvolvimento de cada mudança |

Nenhum push direto em `main`. Todo merge requer PR com CI passando.

Veja [CONTRIBUTING.md](./CONTRIBUTING.md) para convenções de commit e checklist de PR.

---

## Adicionando uma nova fonte

**1. Configuração** — `sources/<nova_fonte>/config/config.py`

```python
NOVA_FONTE_CONFIG = {
    "project_id": "data-prod-295413",
    "sec": { ... },
    "tables": { ... },
}
```

**2. Extractor** — `sources/<nova_fonte>/extractor/extractor.py`

```python
from pipelines.finance_data_pipelines.core import BaseExtractor

class ExtractorNovaFonte(BaseExtractor):

    def get_api_data(self, table_name, **kwargs) -> list[dict]:
        ...

    def normalize_data(self, raw_data) -> list[dict]:
        ...
```

> `save_to_delta_table()` e `run_extraction()` são herdados automaticamente.

**3. Job** — `jobs/<nova_fonte>/run_<tabela>.py`

```python
from pipelines.finance_data_pipelines.sources.nova_fonte.config.config import NOVA_FONTE_CONFIG
from pipelines.finance_data_pipelines.sources.nova_fonte.extractor.extractor import ExtractorNovaFonte

def main():
    extractor = ExtractorNovaFonte(NOVA_FONTE_CONFIG, export_date=...)
    extractor.run_extraction("nome_da_tabela")

if __name__ == "__main__":
    main()
```

**4. Testes** — `tests/unit/test_<nova_fonte>.py`

---

## Variáveis de ambiente

Copie `.env.example` para `.env` e preencha:

| Variável | Descrição |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Caminho para o service account JSON do GCP |
| `GCP_PROJECT_ID` | ID do projeto GCP |
| `DATABRICKS_HOST` | URL do workspace Databricks |
| `DATABRICKS_TOKEN` | Personal Access Token do Databricks |

> O `.env` está no `.gitignore` — nunca commite credenciais.
