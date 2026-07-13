# dataform — transformação (silver) e modelagem (gold)

Projeto Dataform único sobre o BigQuery. Modela `raw → silver → gold` em um só grafo,
com lineage e dependências resolvidas entre as camadas.

## Estrutura
- `definitions/sources/` — declarations das tabelas **raw** (contrato com a ingestão)
- `definitions/silver/` — **frente 2**: transformação raw → silver (limpeza, tipagem, dedup, conformância)
- `definitions/gold/`   — **frente 3**: modelagem silver → gold (dims, facts, métricas)
- `includes/`           — constantes e macros JS (project id, nomes de dataset)

## Convenções
- Nome do arquivo `.sqlx` = nome da tabela gerada.
- Use `config { schema: ..., tags: ["silver"|"gold"] }` para agendar/rodar por camada.
- Agrupe por domínio/fonte dentro de cada camada (`silver/matera/`, `gold/finance/`).

---

## `definitions/sources/` — declarations das tabelas de entrada

No Dataform você **não** referencia tabelas do BigQuery direto pelo nome. Primeiro você
**declara** as tabelas que já existem e que o Dataform **não gerencia** (não cria nem
popula) — tipicamente as tabelas **raw** que a ingestão gravou. Isso é feito com arquivos
`.sqlx` do tipo `declaration`.

Exemplo — `definitions/sources/matera.sqlx`:

```javascript
config {
  type: "declaration",
  database: "data-prod-295413",
  schema: "avenue_datalake",
  name: "avenue_matera_api_saldo_contabil"
}
```

Depois, no model silver, em vez de hardcodar o nome, use `ref()`:

```javascript
-- definitions/silver/matera/saldo_contabil.sqlx
config { type: "table", schema: "avenue_finance", tags: ["silver"] }

SELECT ...
FROM ${ref("avenue_matera_api_saldo_contabil")}
```

### Por que isso importa

| Benefício | O que ganha |
|---|---|
| **Lineage completo** | A tabela raw aparece no grafo de dependências — raw → silver → gold ponta a ponta |
| **Ordem de execução** | O Dataform sabe que o silver depende daquela raw e agenda na ordem certa |
| **Sem hardcode** | Troca de projeto/dataset (dev vs prod) num lugar só, via `includes/` |
| **Contrato explícito** | `sources/` documenta exatamente quais tabelas externas o projeto consome |

### Resumo

- `sources/` → **entrada** (raw): tabelas que vêm de fora do Dataform (ingestão
  Databricks/Cloud Run). Só declarações, nenhuma transformação.
- `silver/` e `gold/` → **saída**: tabelas/views que o Dataform **cria e gerencia** a
  partir das sources.

É a fronteira entre a **frente 1 (ingestão)** e o Dataform: tudo que a ingestão produz na
camada raw é declarado em `sources/` para poder ser referenciado com `ref()`.

---

> Ainda não inicializado — este é só o esqueleto de pastas.
