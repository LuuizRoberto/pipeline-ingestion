-- 2026-07-01 - Export New Accounts Invest
SELECT
  CONCAT(t.Year, '-', LPAD(CAST(t.Month AS STRING), 2, '0')) AS ano_mes,
  COUNT(DISTINCT n.AvenueAccountId) AS qtd_clientes_novos
FROM `avenue_data_warehouse.fact_new_clients` n
JOIN `avenue_data_warehouse.dim_time` t
  ON n.DimTimeSK = t.DimTimeSK
JOIN `avenue_data_warehouse.dim_office_channel` d
  ON n.DimOfficeChannelSK = d.DimOfficeChannelSK
WHERE d.Channel_N3 != 'ITAU WHITE LABEL'
AND t.DimTimeSK >= 20250601
GROUP BY 1, t.Year, t.Month
ORDER BY t.Year, t.Month


