--QUERY INICIAL
CREATE OR REPLACE TABLE avenue_finance.default.ucits AS 
SELECT 
  event_name, 
  user_id, 
  event_date, 
  event_time, 
  ProductType, 
  FirebaseScreen, 
  CASE 
    WHEN TIMESTAMP(event_time) >= '05:00:00' AND  TIMESTAMP(event_time) <= '13:30:00' THEN "Open Market"
    ELSE "Closed Market"
    END AS AccessMoment  
FROM 
  av_datalake_l3.data_warehouse.ga4_intranet_events
WHERE 
  (event_date BETWEEN '20250820' AND '20260331' AND lower(event_name) IN ('home_ucits_screenview', 'home_ucits_screen_view'))
OR 
  (event_date BETWEEN '20250820' AND '20260331' AND lower(event_name) IN ('screenview', 'screen_view') AND FirebaseScreen = '/invest/ucits')
OR
  (event_date BETWEEN '20260201' AND '20260331' AND lower(event_name) = 'invest_landing_view' AND lower(producttype) LIKE '%ucit%')


CREATE OR REPLACE TEMPORARY VIEW  orders_ucits_intra_month AS 
SELECT 
  dmt.Year, 
  dmt.Month, 
  dmp.Product_N3, 
  COUNT(DISTINCT fact.OrderId) AS TotalOrders, 
  SUM(fact.Commission) AS RevenueDolar, 
  COUNT(DISTINCT fact.AvenueAccountId) AS TotalClients 
FROM 
  av_datalake_l3.data_warehouse.fact_orders fact 
LEFT JOIN 
  av_datalake_l3.data_warehouse.dim_time dmt 
    ON fact.DimTimeSK = dmt.DimTimeSK 
LEFT JOIN 
  av_datalake_l3.data_warehouse.dim_product dmp
    ON fact.DimProductSK = dmp.DimProductSK  
WHERE 
  dmp.Product_N3 = "UCITs"
and 
  dmT.DimTimeSK >= 20250801
GROUP BY ALL
ORDER BY dmt.Year, dmt.Month 
;

CREATE OR REPLACE TEMPORARY VIEW access_intra_month AS
SELECT
  acc.*, 
  QtdClientClosedOpenMarket + QtdClientOnlyOpenMarket AS TotalAccessOpenMarket
FROM
(
SELECT 
    Year,
    Month,
    COUNT(DISTINCT user_id) TotalClients,
    COUNT(DISTINCT(CASE WHEN acessou_closed = 1 AND acessou_open = 0 THEN user_id END)) AS QtdClientOnlyClosedMarket,
    COUNT(DISTINCT(CASE WHEN acessou_closed = 0 AND acessou_open = 1 THEN user_id END)) AS QtdClientOnlyOpenMarket, 
    COUNT(DISTINCT(CASE WHEN acessou_closed = 1 AND acessou_open = 1 THEN user_id END)) AS QtdClientClosedOpenMarket
FROM 
(
  SELECT 
      dmt.Year,
      dmt.Month,
      user_id,
      MAX(CASE WHEN AccessMoment = 'Closed Market' THEN 1 ELSE 0 END) as acessou_closed,
      MAX(CASE WHEN AccessMoment = 'Open Market' THEN 1 ELSE 0 END) as acessou_open
  FROM avenue_finance.default.ucits view_ucits
  LEFT JOIN 
    av_datalake_l3.data_warehouse.dim_time dmt 
      ON view_ucits.event_date = dmt.DimTimeSK
  GROUP BY 1, 2, 3
)
GROUP BY 
  Year, 
  Month
ORDER BY 
  Year, 
  Month
) acc 
;


-- ProjShareAderenciaClient é o share de aderencia, aplicado somente para os clientes que acessaram no mercado fechado (para ter a ideia de quantos deles -- teriam enviado uma ordem)

-- ProjTotalOrdersShareAderenciaClient é o calculo do total de ordens que eles teriam enviado, considerando 2 ordens média por cliente e também 
-- considerando que apenas 20% dessas ordens enviadas teriam sido executadas (estamos com um problema e a apex nao está executando todas as ordens)

-- ProjRevenue é a projecao de receita, dado as ordens enviadas e executadas, qual seria nossa receita com base na receita média por ordem. 

CREATE OR REPLACE TEMPORARY VIEW ucits_table_analysis AS 
SELECT
  Year,
  Month,
  TotalAcessos,
  QtdClientOnlyClosedMarket,
  QtdClientOnlyOpenMarket, 
  QtdClientClosedOpenMarket, 
  TotalClientsAccessOpenMarket, 
  TotalOrders, 
  TotalClientsSendOrder, 
  AverageOrderPerClient, 
  ShareAderencia,
  ProjShareAderenciaClient
FROM
(
SELECT 
  a.*, 
  round(a.ShareAderencia * 0.3 * a.QtdClientOnlyClosedMarket, 0) AS ProjShareAderenciaClient 
FROM 
(
SELECT
  acss.Year,
  acss.Month, 
  acss.TotalAcessos, 
  acss.TotalAcessosClosedMarket, 
  acss.TotalAcessosOpenMarket,
  acs.TotalClients, 
  acs.QtdClientOnlyClosedMarket, 
  acs.QtdClientOnlyOpenMarket, 
  acs.QtdClientClosedOpenMarket, 
  acs.TotalAccessOpenMarket AS TotalClientsAccessOpenMarket, 
  ord.TotalOrders, 
  ord.TotalClients AS TotalClientsSendOrder, 
  ord.RevenueDolar, 
  ord.TotalOrders/ord.TotalClients AS AverageOrderPerClient, 
  ROUND(ord.RevenueDolar/ord.TotalOrders,2) AS AverageRevenuePerOrder,
  ord.TotalClients/acs.TotalAccessOpenMarket AS ShareAderencia 
FROM 
(
SELECT 
  dmt.Year, 
  dmt.Month, 
  COUNT(view_ucits.user_id) AS TotalAcessos, 
  COUNT(CASE WHEN AccessMoment = 'Closed Market' THEN user_id END) AS TotalAcessosClosedMarket, 
  COUNT(CASE WHEN AccessMoment = 'Open Market' THEN user_id END) AS TotalAcessosOpenMarket
FROM 
  avenue_finance.default.ucits view_ucits
LEFT JOIN 
  av_datalake_l3.data_warehouse.dim_time dmt 
    ON view_ucits.event_date = dmt.DimTimeSK
GROUP BY 
  ALL 
)  acss 
  LEFT JOIN 
    orders_ucits_intra_month ord 
      ON acss.Year = ord.Year 
        AND acss.Month = ord.Month 
  LEFT JOIN 
    access_intra_month acs 
      ON acss.Year = acs.Year 
        AND acss.Month = acs.Month
ORDER BY 
  Year, Month 
) a
) 
; 



CREATE OR REPLACE TEMPORARY VIEW ucits_table_base_projecao AS 
SELECT
  Year,
  Month,
  ROW_NUMBER() OVER (ORDER BY Year DESC, Month DESC) AS MesesProjecao, 
  TotalAcessos,
  QtdClientOnlyClosedMarket,
  QtdClientOnlyOpenMarket, 
  QtdClientClosedOpenMarket, 
  TotalClientsAccessOpenMarket, 
  TotalOrders, 
  TotalClientsSendOrder, 
  AverageOrderPerClient, 
  ShareAderencia,
  ProjShareAderenciaClient, 
  'Real' AS Tipo 
FROM 
  ucits_table_analysis

UNION ALL 

SELECT
  MIN(Year) AS Year, 
  MIN(Month) AS Month, 
  ROW_NUMBER() OVER (ORDER BY Year, Month) AS MesesProjecao, 
  0 AS TotalAcessos,
  0 AS QtdClientOnlyClosedMarket,
  0 AS QtdClientOnlyOpenMarket, 
  0 AS QtdClientClosedOpenMarket, 
  0 AS TotalClientsAccessOpenMarket, 
  0 AS TotalOrders, 
  0 AS TotalClientsSendOrder, 
  0 AS AverageOrderPerClient, 
  0 AS ShareAderencia,
  0 AS ProjShareAderenciaClient, 
  'Projetado' AS Tipo 
FROM
  av_datalake_l3.data_warehouse.dim_time
WHERE 
  DimTimeSK >= 20260401 
AND 
  DimTimeSK <= 20270601
GROUP BY Year, Month 
ORDER BY 
  Year, Month
; 

CREATE OR REPLACE TEMPORARY VIEW last_value_clients AS 
SELECT 
  MAX(ProjShareAderenciaClient) AS LastShareAderenciaClient 
FROM 
  ucits_table_base_projecao 
WHERE 
  Tipo = 'Real'
AND 
  MesesProjecao = 1
; 

SELECT
  a.*, 
  CASE 
    WHEN (a.Year = 2025 OR (a.Year = 2026 AND a.Month < 4)) THEN ROUND(2 * 0.2 * ProjShareAderenciaClient,0) 
    WHEN a.Year = 2026 and a.Month >=4 AND a.Month <=6 THEN ROUND(2 * 0.2 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=7 AND a.Month <=9 THEN ROUND(2 * 0.3 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=10 AND a.Month <=12 THEN ROUND(2 * 0.4 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2027 THEN ROUND(2 * 0.5 * ProjShareAderenciaClient,0) 
  END AS ProjTotalOrdersShareAderenciaClient,
  CASE 
    WHEN (a.Year = 2025 OR (a.Year = 2026 AND a.Month < 4)) THEN 0
    WHEN a.Year = 2026 and a.Month >=4 AND a.Month <=6 THEN ROUND(2 * 0.2 * 11.825 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=7 AND a.Month <=9 THEN ROUND(2 * 0.3 * 11.825 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=10 AND a.Month <=12 THEN ROUND(2 * 0.4 * 11.825 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2027 THEN ROUND(2 * 0.5 * 11.8 * ProjShareAderenciaClient,0) 
  END AS ProjRevenueShareAderenciaClient,
  CASE 
    WHEN (a.Year = 2025 OR (a.Year = 2026 AND a.Month < 4)) THEN 0
    WHEN a.Year = 2026 and a.Month >=4 AND a.Month <=6 THEN ROUND(2 * 0.2 * 5074.911 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=7 AND a.Month <=9 THEN ROUND(2 * 0.3 * 5074.911 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2026 and a.Month >=10 AND a.Month <=12 THEN ROUND(2 * 0.4 * 5074.911 * ProjShareAderenciaClient,0)
    WHEN a.Year = 2027 THEN ROUND(2 * 0.5 * 11.8 * ProjShareAderenciaClient,0) 
  END AS ProjExecutedVolumeShareAderenciaClient
FROM 
(
  SELECT
      a.Year,
      a.Month,
      TotalAcessos,
      QtdClientOnlyClosedMarket,
      QtdClientOnlyOpenMarket, 
      QtdClientClosedOpenMarket, 
      TotalClientsAccessOpenMarket, 
      TotalOrders, 
      TotalClientsSendOrder, 
      AverageOrderPerClient, 
      ShareAderencia,
      CASE
        WHEN Tipo = 'Real'      THEN ProjShareAderenciaClient
        WHEN Tipo = 'Projetado' THEN ROUND((SELECT MAX(LastShareAderenciaClient) FROM last_value_clients) * POW(1.015, MesesProjecao), 0)
      END AS ProjShareAderenciaClient
  FROM 
    ucits_table_base_projecao a
) a
;
