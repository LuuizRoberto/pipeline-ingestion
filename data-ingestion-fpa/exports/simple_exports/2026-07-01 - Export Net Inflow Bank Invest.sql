-- 2026-07-01 - Export Net Inflow Bank Invest
SELECT
  dmt.Year, 
  dmt.Month, 
  CASE 
    WHEN fac.DescriptionSummary IS NULL AND Description = 'Debit Card Standard Delivery' THEN 'CARD DEBIT/WITHDRAWAL'
    WHEN fac.DescriptionSummary IS NULL AND Description = 'Debit Card Standard Delivery Refund' THEN 'CARD DEBIT/WITHDRAWAL'
    WHEN fac.DescriptionSummary IS NULL AND Description = 'BI Estorno Remessa de Câmbio Padrão Disponibilidade EUR-BR' THEN 'FX EUA-BR'
    WHEN fac.DescriptionSummary IS NULL AND Description = 'Third Party Deposit' THEN 'WIRE - BANKING'
    WHEN fac.DescriptionSummary = 'WIRE BANKING' THEN 'WIRE - BANKING'
    WHEN fac.DescriptionSummary = 'WIRE' AND AccountType = 4 THEN 'WIRE - BANKING'
    ELSE fac.DescriptionSummary
   END AS DescriptionSummary,
  CASE 
    WHEN fac.AccountType = 4 THEN 'Banking'
    WHEN fac.AccountType = 1 THEN 'Invest'
    END AS AccountType,
  Kind, 
  SUM(AmountTotalDol) AS AmountTotalDol, 
  COUNT(DISTINCT AvenueAccountId) AS TotalClients, 
  COUNT(DISTINCT EntryId) AS TotalMovs 
FROM 
  `avenue_data_warehouse.fact_net_money_inflow_bank_invest` fac 
LEFT JOIN 
  avenue_data_warehouse.dim_time dmt 
    ON fac.DimTimeSK = dmt.DimTimeSK 
WHERE 
  dmt.DimTimeSK >= 20240601 
GROUP BY 
  ALL 
ORDER BY 
  dmt.Year, 
  dmt.Month, 
  AccountType, 
  Kind, 
  DescriptionSummary 


