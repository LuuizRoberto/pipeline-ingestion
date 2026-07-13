-- 2026-07-01 - Export New Banking Accounts
WITH contas AS (
  SELECT DISTINCT AvenueAccountid, AccountCreationDate
  FROM `avenue_finance.fact_banking_active_accounts`
  WHERE AccountCreationDate >= '2025-06-01'
)
SELECT
  CONCAT(EXTRACT(YEAR FROM AccountCreationDate), '-', LPAD(CAST(EXTRACT(MONTH FROM AccountCreationDate) AS STRING), 2, '0')) AS ano_mes,
  COUNT(DISTINCT AvenueAccountid) AS qtd_contas_abertas
FROM contas
GROUP BY 1
ORDER BY 1

