-- 2026-06-01 - Modelo Ordens - Câmbio - Feature Recorrencia 
CREATE OR REPLACE MODEL `avenue_finance.model_cambio_ordens`
OPTIONS (
  model_type = 'linear_reg',
  input_label_cols = ['CambioUSD']
) AS
WITH monthly AS (
SELECT
  funds.Year, 
  funds.Month, 
  funds.AmountUSD AS CambioUSD, 
  ords.OrdersVolumeUSD AS OrdensUSD, 
FROM 
(
SELECT
  Year, 
  Month, 
  SUM(AmountUsd) AS AmountUSD
FROM 
  `avenue_data_warehouse.fact_funds_transfers` fac 
LEFT JOIN
  `avenue_data_warehouse.dim_time` dmt 
    ON fac.DimTimeSK = dmt.DimTimeSK 
WHERE 
  fac.Direction = 'BU'
AND 
  fac.Purpose != 'Availability'
AND 
  fac.DimTimeSK >= 20240101
GROUP BY 
  ALL 
) funds 

LEFT JOIN 
  (
      SELECT
        Year, 
        Month, 
        SUM(TotalExecutedVolume) AS OrdersVolumeUSD
      FROM 
        `avenue_data_warehouse.fact_orders` fac 
      LEFT JOIN
        `avenue_data_warehouse.dim_time` dmt 
          ON fac.DimTimeSK = dmt.DimTimeSK 
      WHERE 
        fac.BuySell = 'B'
      AND 
        fac.DimTimeSK >= 20240101
      GROUP BY 
        ALL 
  ) ords 
    ON funds.Year = ords.Year 
      AND funds.Month = ords.Month 
ORDER BY 
  1,2
)
SELECT CambioUSD, OrdensUSD FROM monthly; 


SELECT *
FROM ML.EVALUATE(MODEL `avenue_finance.model_cambio_ordens`); 

SELECT *
FROM ML.WEIGHTS(MODEL `avenue_finance.model_cambio_ordens`); 


WITH monthly AS (
SELECT
  funds.Year, 
  funds.Month, 
  funds.AmountUSD AS CambioUSD, 
  ords.OrdersVolumeUSD AS OrdensUSD, 
FROM 
(
SELECT
  Year, 
  Month, 
  SUM(AmountUsd) AS AmountUSD
FROM 
  `avenue_data_warehouse.fact_funds_transfers` fac 
LEFT JOIN
  `avenue_data_warehouse.dim_time` dmt 
    ON fac.DimTimeSK = dmt.DimTimeSK 
WHERE 
  fac.Direction = 'BU'
AND 
  fac.Purpose != 'Availability'
AND 
  fac.DimTimeSK >= 20240101
GROUP BY 
  ALL 
) funds 

LEFT JOIN 
  (
      SELECT
        Year, 
        Month, 
        SUM(TotalExecutedVolume) AS OrdersVolumeUSD
      FROM 
        `avenue_data_warehouse.fact_orders` fac 
      LEFT JOIN
        `avenue_data_warehouse.dim_time` dmt 
          ON fac.DimTimeSK = dmt.DimTimeSK 
      WHERE 
        fac.BuySell = 'B'
      AND 
        fac.DimTimeSK >= 20240101
      GROUP BY 
        ALL 
  ) ords 
    ON funds.Year = ords.Year 
      AND funds.Month = ords.Month)

SELECT
  CORR(CambioUSD, OrdensUSD) AS coeficiente_pearson
FROM
  monthly;


