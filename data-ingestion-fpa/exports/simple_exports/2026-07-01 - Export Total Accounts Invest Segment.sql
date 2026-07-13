-- 2026-07-01 - Export Total Accounts Invest Segment
select Saldos, COUNT(DISTINCT AvenueAccountId) TotalClients FROM (
select 
  a.*,
  b.saldo_balance_dol, 
  b.saldo_investido_dol, 
  Saldos
from 
(
  SELECT DISTINCT
    a.AvenueAccountId
  FROM `avenue_finance.fact_active_clients` a
  JOIN `avenue_finance.dim_office_channel` d
    ON a.DimOfficeChannelSK = d.DimOfficeChannelSK
  WHERE a.DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_active_clients`)
    AND d.Channel_N3 != 'ITAU WHITE LABEL'
) a
LEFT JOIN 
(
  SELECT 
     a.AvenueAccountId, 
     saldo_balance_dol, 
     saldo_investido_dol,
     CASE 
      WHEN saldo_balance_dol > 0 AND saldo_investido_dol > 0   then 'Saldos Invest e Balance'
      WHEN saldo_balance_dol > 0 AND saldo_investido_dol <= 0  then 'Saldos Balance'
      WHEN saldo_balance_dol <= 0 AND saldo_investido_dol > 0  then 'Saldos Invest'
      WHEN saldo_balance_dol <= 0 AND saldo_investido_dol <= 0 then 'Sem custodia'
      END AS Saldos
  FROM 
  (
  SELECT
    c.AvenueAccountId,
    SUM(CASE WHEN p.Product_N2 IN ('Balance BR', 'Balance US', 'Balance EUR') THEN c.TotalNetDol ELSE 0 END) AS saldo_balance_dol,
    SUM(CASE WHEN p.Product_N2 NOT IN ('Balance BR', 'Balance US', 'Balance EUR') OR p.Product_N2 IS NULL THEN c.TotalNetDol ELSE 0 END) AS saldo_investido_dol
  FROM `avenue_finance.fact_custody` c
  JOIN `avenue_finance.dim_product` p
    ON c.DimProductSK = p.DimProductSK
  WHERE c.DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_custody`)
  GROUP BY c.AvenueAccountId
  ) a

)  b 
ON a.AvenueAccountId = b.AvenueAccountID 
) GROUP BY ALL 

