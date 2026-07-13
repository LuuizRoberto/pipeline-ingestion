# ADR 0001 — conta_contabil: união arquivo + BigQuery com arquivo auto-atualizável

- **Status:** Aceito
- **Data:** 2026-07-08
- **Componente:** `sources/matera/extractor` (ingestão Matera → BigQuery)

## Contexto

A coleta de `conta_contabil` iterava apenas sobre a lista de IDs em
`sources/matera/config/conta_contabil_ids.txt`. Como o arquivo era mantido
manualmente, contas que já tinham lançamento na Matera mas não estavam listadas
nunca eram buscadas — ficavam ausentes da tabela
`avenue_finance.avenue_matera_api_conta_contabil`.

A tabela `avenue_finance.avenue_matera_api_accounting_facts` já contém, em
`ledgeraccountcode`, todas as contas que tiveram movimento. Ela é a fonte natural
para completar a lista.

## Decisão

1. **Fonte da lista = união.** A lista de códigos coletados passa a ser
   `arquivo ∪ distinct(ledgeraccountcode da accounting_facts)`, deduplicada e
   ordenada.

2. **Lógica no extractor, não no job.** A montagem da lista fica em
   `_load_conta_contabil_codes` (com os helpers `_read_codes_from_file` e
   `_read_codes_from_bq`), mantendo `_fetch_conta_contabil` focado no loop HTTP.
   Motivo: "quais contas existem/devem ser buscadas" é decisão de fonte de
   extração; o job (`run_conta_contabil.py`) permanece orquestração fina.

3. **Sem escrita no arquivo.** A lista é montada em memória a cada run; o `.txt`
   é somente leitura (IDs fixos mantidos à mão). A união é recalculada
   sempre a partir do BigQuery, então não há estado a persistir.

4. **Config, não hardcode.** A fonte é parametrizada em
   `conta_contabil.codes_source_table` / `codes_source_column`.

5. **Log por conta.** Cada conta chamada é logada com índice/total
   (`(i/total) buscando conta X`) e a quantidade de registros retornados, para
   acompanhar o progresso da coleta.

### Salvaguardas

- Falha na consulta BQ é logada e resulta em lista vazia — a coleta **degrada
  para o arquivo** em vez de quebrar o job.

## Consequências

- A tabela passa a refletir todas as contas com movimento, sem depender de
  manutenção manual completa do `.txt`.
- O `.txt` continua sendo a lista-base editável à mão para IDs que se queira
  garantir independentemente do BigQuery.

### Histórico

Uma versão inicial reescrevia o `.txt` com a união (fallback auto-atualizável).
Foi removida: no filesystem do Databricks Repos a escrita falhava com
`OSError [Errno 22]` (não suporta `tmp` + replace), e a união recalculada a
cada run a partir do BigQuery já torna a persistência desnecessária.

## Cobertura de testes

`tests/unit/test_extractor_matera.py::TestLoadContaContabilCodes` cobre:
união sem duplicar e degradação para o arquivo quando o BQ falha.
