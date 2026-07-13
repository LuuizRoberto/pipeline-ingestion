-- 2026-06-09 - Criação Inicial - LoadFactRevenueFPNA
CREATE OR REPLACE TABLE avenue_finance.fact_revenue_fpna AS 
SELECT
     f.DimTimeSk
    ,f.DimAccountSK
    ,f.DimOfficeChannelSK
    ,f.DimForeignFinderSK
    ,f.DimBranchSK
    ,f.DimAucRangeSK
    ,f.DimClientSegmentSK
    ,f.DimProductSK
    ,COALESCE(ext.DimExternalClientSK, -1) AS DimExternalClientSK
    ,f.OfficeId
    ,f.ForeignFinderId
    ,f.BranchId
    ,f.AvenueAccountId
    ,f.TransactionId
    ,f.RevenueSource
    ,f.RevenueDolar
    ,f.RevenueReal
FROM
    `avenue_finance.fact_revenue` f
LEFT JOIN
    `avenue_finance.dim_time` dt
        ON f.DimTimeSk = dt.DimTimeSK
LEFT JOIN
    `avenue_finance.dim_external_client` ext
        ON  ext.AvenueAccountId  = f.AvenueAccountId
        AND dt.Date >= DATE(ext.DimStartDate)
          AND dt.Date < DATE(ext.DimEndDate)
        AND ext.AvenueAccountId IS NOT NULL
WHERE
    f.RevenueSource IN (
        'Securities','FX','Options','CardIncentives',
        'CrossBorderFee','DebitCardDelivery','InterchangeFee',
        'WireOutFee','Structured Notes','CertificateOfDeposit',
        'Treasuries','Bonds', 'InactivityFee', 'MutualFunds'
    ) -- falta floating & Stock Lending 

UNION ALL 

SELECT
  dmt.DimTimeSK, 
  COALESCE(dma.DimAccountSK, -100) AS DimAccountSK, -- Client not applicable
  COALESCE(act.DimOfficeChannelSK,-100) as DimOfficeChannelSK, -- Office not applicable
  COALESCE(act.DimForeignFinderSK,-100) as DimForeignFinderSK, -- Foreign Finder not applicable
  COALESCE(act.DimBranchSK,-1) as DimBranchSK, -- Branch not applicable
  COALESCE(act.DimAucRangeSK,1) AS DimAucRangeSK,
  COALESCE(act.DimClientSegmentSK, 1) AS DimClientSegmentSK,
  -99 AS DimProductSK, -- Digital Content
  COALESCE(ext.DimExternalClientSK, -1)           AS DimExternalClientSK, -- Client not Applicable
  COALESCE(act.OfficeId,'Office not applicable') as OfficeId,-- Avenue Invest.,
  COALESCE(act.ForeignFinderId,'Foreign Finder not applicable')     AS ForeignFinderId,
  COALESCE(act.BranchId,'Branch not applicable' ) as BranchId,
  COALESCE(dma.AvenueAccountId, 'Client not applicable') AS AvenueAccountId,
  iugu.Id AS TransactionId, 
  'Digital Content' AS RevenueSource,
  TotalCentsPerMonth / dol.sell AS RevenueDolar,
  TotalCentsPerMonth AS RevenueReal, 
FROM
(
SELECT
  CPF, 
  email,
  customer_id,
  id,
  currency, 
  StartDate, 
  EndDate, 
  CASE
    WHEN EXTRACT(DAY FROM DATE_ADD(StartDate, INTERVAL MonthsAdds MONTH)) != EXTRACT(DAY FROM StartDate)
    THEN LAST_DAY(DATE_ADD(StartDate, INTERVAL MonthsAdds MONTH))
    ELSE DATE_ADD(StartDate, INTERVAL MonthsAdds MONTH)
  END AS CompetenciaDate,
  Compensacao, 
  TotalCentsPerMonth
FROM
(
SELECT
  payer_cpf_cnpj AS CPF,
  email,
  customer_id,
  id,
  currency,  
  paid_at AS StartDate, 
  DATE_ADD(DATE(paid_at), INTERVAL Compensacao MONTH) AS EndDate,
  Compensacao, 
  GENERATE_ARRAY(0, Compensacao - 1) AS ArrayDateAddMonth,
  ROUND(TotalCents / Compensacao,2) AS TotalCentsPerMonth
FROM 
(
SELECT 
    payer_cpf_cnpj, 
    email,
    DATE_ADD(DATE(DATETIME(paid_at,'America/Sao_Paulo')), INTERVAL 7 DAY) AS paid_at, -- As compras que sao feitas no final do mes, as notas sao emitidas no inicio do mes seguinte. 
    currency,
    customer_id,
    id,  
    CASE 
      WHEN CAST(DATETIME(paid_at,'America/Sao_Paulo') AS DATE) BETWEEN '2024-11-24' AND '2024-11-30' THEN 36
      WHEN CAST(DATETIME(paid_at,'America/Sao_Paulo') AS DATE) BETWEEN '2025-11-24' AND '2025-11-30' THEN 6
    ELSE 12 END AS Compensacao, 
    ROUND(total_cents/100, 2) AS TotalCents
FROM  
  avenue_datalake_latest_snapshot.iugu_myprofit_api_reports_invoices
WHERE 
  paid_at IS NOT NULL 
AND 
  customer_id IS NOT NULL 
) 
), 
UNNEST (ArrayDateAddMonth) AS MonthsAdds
ORDER BY CPF, CompetenciaDate
) iugu 
LEFT JOIN 
  avenue_finance.dim_time dmt 
    ON iugu.CompetenciaDate = dmt.Date 
LEFT JOIN 
  avenue_finance.dim_account dma 
    ON iugu.CPF = dma.CPF 
      AND iugu.CompetenciaDate >= dma.DimStartDate 
        AND iugu.CompetenciaDate < dma.DimEndDate
LEFT JOIN 
  `avenue_finance.fact_active_clients` act
      ON dma.AvenueAccountId = act.AvenueAccountId 
        AND dmt.DimTimeSK = act.DimTimeSK 
LEFT JOIN 
    `avenue_data_warehouse.fact_dollar_ptax` dol 
        ON dmt.Date = dol.Date
LEFT JOIN 
  `avenue_finance.dim_external_client` ext
    ON  ext.CustomerExternalId = iugu.customer_id  
    AND iugu.CompetenciaDate >= DATE(ext.DimStartDate)
    AND iugu.CompetenciaDate < DATE(ext.DimEndDate)
WHERE 
  dmt.Date <= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)



