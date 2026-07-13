-- 2026-06-15 - Conciliation Querie Analysis
-- ============================================================
-- PARÂMETROS — altere apenas aqui
-- ============================================================
DECLARE start_date DATE DEFAULT '2026-05-08';
DECLARE end_date   DATE DEFAULT '2026-05-15';

-- Datas derivadas (não alterar)
DECLARE start_export_date DATE;
DECLARE end_export_date   DATE;
DECLARE start_settlement  INT64;
DECLARE end_settlement    INT64;
DECLARE cutoff_start      TIMESTAMP;
DECLARE cutoff_end        TIMESTAMP;

SET start_export_date = DATE_ADD(start_date, INTERVAL 1 DAY);
SET end_export_date   = DATE_ADD(end_date,   INTERVAL 1 DAY);
SET start_settlement  = CAST(FORMAT_DATE('%Y%m%d', start_export_date) AS INT64);
SET end_settlement    = CAST(FORMAT_DATE('%Y%m%d', end_export_date)   AS INT64);
SET cutoff_start      = TIMESTAMP(DATETIME(start_export_date, TIME '08:08:00'));
SET cutoff_end        = TIMESTAMP(DATETIME(end_export_date,   TIME '08:08:00'));

-- ============================================================
-- ANÁLISE DE CONCILIAÇÃO DE BALANCE
-- ============================================================
WITH

-- Ambos os balances numa tabela única (já com o label da data)
balance_raw AS (
  SELECT
    DATE_SUB(dl_export_date, INTERVAL 1 DAY) AS data_referencia,
    account_id,
    ROUND(settled, 2) AS settled,
    'start' AS tipo
  FROM avenue_datalake.cloudsql_trading_prod_av_banking_banking_prod_public_balance
  WHERE currency_id = 'USD'
    AND ROUND(settled, 2) - ROUND(prov_debit, 2) <> 0
    AND dl_export_date = start_export_date

  UNION ALL

  SELECT
    DATE_SUB(dl_export_date, INTERVAL 1 DAY) AS data_referencia,
    account_id,
    ROUND(settled, 2) AS settled,
    'end' AS tipo
  FROM avenue_datalake.cloudsql_trading_prod_av_banking_banking_prod_public_balance
  WHERE currency_id = 'USD'
    AND ROUND(settled, 2) - ROUND(prov_debit, 2) <> 0
    AND dl_export_date = end_export_date
),

-- Movimentações de corte (madrugada) — para ambas as datas
mov_cutoff_raw AS (
  SELECT
    account_id,
    ROUND(SUM(Amount), 2) AS AmountTransaction,
    'start' AS tipo
  FROM (
    SELECT
      account_id,
      CASE
        WHEN jnl_kind = 'D' THEN (amount + COALESCE(fee_amount, 0)) * -1
        ELSE (amount + COALESCE(fee_amount, 0))
      END AS Amount
    FROM 
      avenue_datalake_latest_snapshot.cloudsql_trading_prod_av_banking_banking_prod_public_journal_entry
    WHERE 
      currency_id  = 'USD'
    AND 
      action_kind IN ('F', 'R', 'Z', 'W')
    AND 
      created >= TIMESTAMP(start_date)
    AND 
      created <= cutoff_start
    AND 
      settlement   = start_settlement
  )
  GROUP BY ALL

  UNION ALL

  SELECT
    account_id,
    ROUND(SUM(Amount), 2) AS AmountTransaction,
    'end' AS tipo
  FROM (
    SELECT
      account_id,
      CASE
        WHEN jnl_kind = 'D' THEN (amount + COALESCE(fee_amount, 0)) * -1
        ELSE (amount + COALESCE(fee_amount, 0))
      END AS Amount
    FROM 
      avenue_datalake_latest_snapshot.cloudsql_trading_prod_av_banking_banking_prod_public_journal_entry
    WHERE 
      currency_id  = 'USD'
    AND 
      action_kind IN ('F', 'R', 'Z', 'W')
    AND 
      created >= TIMESTAMP(end_date)
    AND 
      created <= cutoff_end
    AND 
      settlement = end_settlement
  )
  GROUP BY ALL
),

-- Balance ajustado por data (desconta o corte de madrugada)
balance_adj AS (
  SELECT
    b.data_referencia,
    b.account_id,
    b.tipo,
    b.settled - COALESCE(c.AmountTransaction, 0) AS balance_ajustado
  FROM 
    balance_raw b
  LEFT JOIN 
    mov_cutoff_raw c
      ON b.account_id = c.account_id
        AND b.tipo = c.tipo
),

-- Movimentações que liquidaram entre D0+1 e Dn (exclusive Dn+1)
mov_between AS (
  SELECT
    account_id,
    ROUND(SUM(Amount), 2) AS mov_entre_datas
  FROM (
    SELECT
      account_id,
      CASE
        WHEN jnl_kind = 'D' THEN (amount + COALESCE(fee_amount, 0)) * -1
        ELSE (amount + COALESCE(fee_amount, 0))
      END AS Amount
    FROM 
      avenue_datalake_latest_snapshot.cloudsql_trading_prod_av_banking_banking_prod_public_journal_entry
    WHERE 
      currency_id  = 'USD'
    AND 
      action_kind IN ('F', 'R', 'Z', 'W')
    AND 
      settlement  >= start_settlement
    AND 
      settlement  <= end_settlement - 1
  )
  GROUP BY ALL
),

-- Pivot: uma linha por conta com start e end lado a lado
pivot AS (
  SELECT
    account_id,
    MAX(IF(tipo = 'start', data_referencia,  NULL)) AS data_inicio,
    MAX(IF(tipo = 'end',   data_referencia,  NULL)) AS data_fim,
    MAX(IF(tipo = 'start', balance_ajustado, NULL)) AS balance_iniciO,
    MAX(IF(tipo = 'end',   balance_ajustado, NULL)) AS balance_fim
  FROM balance_adj
  GROUP BY account_id
)

-- ============================================================
-- RESULTADO FINAL
-- ============================================================
SELECT
  p.account_id,
  p.data_inicio AS StartDate,
  p.data_fim AS EndDate,
  p.balance_inicio AS StartBalance,
  p.balance_fim AS EndBalance,
  p.balance_fim - p.balance_inicio AS Diffbalance,
  COALESCE(mov.mov_entre_datas, 0) AS mov_entre_datas,
  ROUND((p.balance_fim - p.balance_inicio) - COALESCE(mov.mov_entre_datas, 0), 2) AS diff_nao_explicada
FROM pivot p
LEFT JOIN mov_between mov ON p.account_id = mov.account_id
WHERE
  ROUND((p.balance_fim - p.balance_inicio) - COALESCE(mov.mov_entre_datas, 0), 2) <> 0
ORDER BY ABS(diff_nao_explicada) DESC;




