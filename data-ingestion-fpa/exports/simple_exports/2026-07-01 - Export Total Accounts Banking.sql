-- 2026-07-01 - Export Total Accounts Banking
WITH custodia_banking AS (
  -- Saldo de custódia em produtos de banking (USD e EUR), última data disponível, 1 linha por cliente
  SELECT
    c.AvenueAccountId,
    SUM(c.TotalNetDol) AS saldo_banking_dol
  FROM `avenue_finance.fact_custody` c
  JOIN `avenue_finance.dim_product` p
    ON c.DimProductSK = p.DimProductSK
  WHERE c.DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_custody`)
    AND p.Product_N3 IN ('Balance US Banking', 'Balance EUR Banking')
  GROUP BY c.AvenueAccountId
),

clientes_banking AS (
  -- Clientes ativos de banking na última data disponível
  SELECT DISTINCT
    AvenueAccountid
  FROM `avenue_finance.fact_banking_active_accounts`
  WHERE DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_banking_active_accounts`)
)

SELECT
  COUNT(DISTINCT b.AvenueAccountid) AS total_clientes_banking,
  COUNT(DISTINCT CASE WHEN cb.saldo_banking_dol > 0 THEN b.AvenueAccountid END) AS clientes_com_saldo,
  COUNT(DISTINCT CASE WHEN cb.saldo_banking_dol IS NULL OR cb.saldo_banking_dol <= 0 THEN b.AvenueAccountid END) AS clientes_sem_saldo
FROM clientes_banking b
LEFT JOIN custodia_banking cb
  ON b.AvenueAccountid = cb.AvenueAccountId


