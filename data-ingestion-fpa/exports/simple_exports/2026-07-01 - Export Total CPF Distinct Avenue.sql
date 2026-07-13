-- 2026-07-01 - Export Total CPF Distinct Avenue
 SELECT * FROM (
 SELECT 
    CPF, 
    SUM(TotalNetDol) as Custodia
  FROM `avenue_finance.fact_active_clients` a 
  LEFT JOIN avenue_finance.dim_account b 
    ON a.DimAccountSK = b.DimAccountSK 
  WHERE DimTimeSK = (SELECT MAX(DimTimeSK) FROM `avenue_finance.fact_active_clients`)
  aND cpf is not null 
  group by CPF) 
  where Custodia > 0 




