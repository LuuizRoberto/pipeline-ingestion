SELECT
  dmt.year, 
  dmt.month, 
  dmt.Date, 
  o.AvenueAccountId, 
  o.OrderId,
  dmp.Product_L4, 
  dmp.Product_L3, 
  dmo.Channel_N3, 
  o.Commission AS Revenue, 
  o.TotalExecutedVolume, 
  CASE 
    WHEN o.Commission = 0 THEN 0 
    WHEN o.TotalExecutedVolume < 10000 THEN 50
    WHEN o.TotalExecutedVolume >= 10000 THEN o.TotalExecutedVolume * 0.005
    END AS NewRevenueMesa, 
  CASE 
    WHEN o.Commission = 0 THEN 0 
    WHEN o.TotalExecutedVolume < 10000 THEN 100
    WHEN o.TotalExecutedVolume >= 10000 THEN o.TotalExecutedVolume * 0.0025
    END AS RevenueAnteriorEstudoMesa
FROM 
  av_datalake_l3.data_warehouse.fact_orders o
JOIN 
  av_datalake_l3.data_warehouse.dim_office_channel dmo 
    ON o.DimOfficeChannelSK = dmo.DimOfficeChannelSK
JOIN 
  av_datalake_l3.data_warehouse.dim_product dmp 
    ON o.DimProductSK = dmp.DimProductSK 
JOIN  
  av_datalake_l3.data_warehouse.dim_time dmt 
    ON o.DimTimeSK = dmt.DimTimeSK 
WHERE 
  o.DimTimeSK >= 20260511
AND 
  o.OriginatorUser IN (
  ) 
AND 
  Product_L3 = "Funds"
