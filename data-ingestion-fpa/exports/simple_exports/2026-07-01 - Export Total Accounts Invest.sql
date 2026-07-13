-- 2026-07-01 - Export Total Accounts Invest

WITH custodia AS (
  -- Saldo total de custódia, última data disponível, separado em Balance (BR/US/EUR) vs Investido (resto)
  SELECT
    c.AvenueAccountId,
    SUM(CASE WHEN p.Product_N2 IN ('Balance BR', 'Balance US', 'Balance EUR') THEN c.TotalNetDol ELSE 0 END) AS saldo_balance_dol,
    SUM(CASE WHEN p.Product_N2 NOT IN ('Balance BR', 'Balance US', 'Balance EUR') OR p.Product_N2 IS NULL THEN c.TotalNetDol ELSE 0 END) AS saldo_investido_dol
  FROM `avenue_finance.fact_custody` c
  JOIN `avenue_finance.dim_product` p
    ON c.DimProductSK = p.DimProductSK
  WHERE c.DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_custody`)
  GROUP BY c.AvenueAccountId
),

clientes_ativos AS (
  -- Clientes ativos, última data disponível, excluindo canal ITAU WHITE LABEL
  SELECT DISTINCT
    a.AvenueAccountId
  FROM `avenue_finance.fact_active_clients` a
  JOIN `avenue_finance.dim_office_channel` d
    ON a.DimOfficeChannelSK = d.DimOfficeChannelSK
  WHERE a.DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_active_clients`)
    AND d.Channel_N3 != 'ITAU WHITE LABEL'
)

SELECT
  COUNT(DISTINCT a.AvenueAccountId) AS total_clientes_ativos,
  COUNT(DISTINCT CASE WHEN c.saldo_balance_dol > 0 AND (c.saldo_investido_dol <= 0 OR c.saldo_investido_dol IS NULL) THEN a.AvenueAccountId END) AS so_saldo_balance,
  COUNT(DISTINCT CASE WHEN c.saldo_investido_dol > 0 AND (c.saldo_balance_dol <= 0 OR c.saldo_balance_dol IS NULL) THEN a.AvenueAccountId END) AS so_investido,
  COUNT(DISTINCT CASE WHEN c.saldo_balance_dol > 0 AND c.saldo_investido_dol > 0 THEN a.AvenueAccountId END) AS ambos,
  COUNT(DISTINCT CASE WHEN (c.saldo_balance_dol <= 0 OR c.saldo_balance_dol IS NULL) AND (c.saldo_investido_dol <= 0 OR c.saldo_investido_dol IS NULL) THEN a.AvenueAccountId END) AS nenhum
FROM clientes_ativos a
LEFT JOIN custodia c
  ON a.AvenueAccountId = c.AvenueAccountId



