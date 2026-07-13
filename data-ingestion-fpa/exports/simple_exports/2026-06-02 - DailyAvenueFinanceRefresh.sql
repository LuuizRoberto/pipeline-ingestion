-- 2026-06-02 - DailyAvenueFinanceRefresh 

DECLARE inserted    INT64;
DECLARE updated     INT64;
DECLARE ChangedDate DATETIME;
DECLARE RevenueStartDate DATE;

SET ChangedDate = DATETIME(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY));
SET RevenueStartDate = DATE_SUB(DATE_ADD(LAST_DAY(CURRENT_DATE('America/Sao_Paulo')), INTERVAL 1 DAY), INTERVAL 5 MONTH);

CREATE OR REPLACE TABLE avenue_finance.fact_opad_calculate_data AS
SELECT 
    CAST(cnt.codcontacontabilpai AS STRING) AS codcontacontabilpai,
    CAST(sld.codcontacontabil AS STRING) AS codcontacontabil, 
    CAST(sld.dtperiodo AS DATE) AS dtperiodo, 
    sld.tipolanc,
    sld.vlrsaldo
FROM    
    avenue_finance.avenue_matera_api_saldo_contabil sld
LEFT JOIN 
    avenue_finance.avenue_matera_api_conta_contabil cnt
        ON sld.codcontacontabil = cnt.codcontacontabil
WHERE 
    (sld.codcontacontabil LIKE '8%' OR sld.codcontacontabil LIKE '7%')
OR
    (CAST(cnt.codcontacontabilpai AS STRING) IN (
                                '121100110000000',
                                '121103110000000',
                                '121107010000000',
                                '121108510000000',
                                '121109110000000',
                                '121109910000000',
                                '121200110000000',
                                '121203110000000',
                                '121207010000000',
                                '121208510000000',
                                '121209910000000',
                                '121300110000000',
                                '121303110000000',
                                '121309910000000',
                                '122100110000000',
                                '125100000000000',
                                '131100110000000',
                                '131103110000000',
                                '131103210000000',
                                '131103310000000',
                                '131109510000000',
                                '131109910000000',
                                '131131000000000',
                                '131133000000000',
                                '131151500000000',
                                '131152500000000',
                                '131153000000000',
                                '131153500000000',
                                '131154000000000',
                                '131154500000000',
                                '131155000000000',
                                '131155500000000',
                                '131156000000000',
                                '131156500000000',
                                '131157000000000',
                                '131157500000000',
                                '131159000000000',
                                '131159900000000',
                                '131201000000000',
                                '131202000000000',
                                '131203000000000',
                                '131204000000000',
                                '131209500000000',
                                '131209900000000',
                                '131851010000000',
                                '131852010000000',
                                '131852510000000',
                                '131852610000000',
                                '131853110000000',
                                '131859010000000',
                                '132100110000000',
                                '132103110000000',
                                '132103210000000',
                                '132103310000000',
                                '132107010000000',
                                '132108510000000',
                                '132109910000000',
                                '134100110000000',
                                '134109910000000',
                                '134200110000000',
                                '134209910000000',
                                '134300110000000',
                                '134309910000000',
                                '134400110000000',
                                '134409910000000',
                                '134450110000000',
                                '136100110000000',
                                '136103110000000',
                                '136108000000000',
                                '136109910000000',
                                '136150110000000',
                                '136153110000000',
                                '136158000000000',
                                '136159910000000',
                                '136160110000000',
                                '136171010000000',
                                '136200110000000',
                                '136203110000000',
                                '136208000000000',
                                '136209910000000',
                                '136250110000000',
                                '137100110000000',
                                '137109010000000',
                                '139100110000000',
                                '139101110000000',
                                '139103000000000',
                                '161100110000000',
                                '161200110000000',
                                '161300110000000',
                                '162100110000000',
                                '162150110000000',
                                '162200110000000',
                                '162400110000000',
                                '163050110000000',
                                '163150110000000',
                                '163250110000000',
                                '163350110000000',
                                '163450110000000',
                                '164100110000000',
                                '164300110000000',
                                '165200110000000',
                                '166100110000000',
                                '174101000000000',
                                '174201000000000',
                                '175101000000000',
                                '175201000000000',
                                '181101010000000',
                                '181201010000000',
                                '181301010000000',
                                '181401010000000',
                                '181901010000000'  
                            ))

UNION ALL 

SELECT 
    CAST(conta_contabil AS STRING) AS codcontacontabilpai,
    CAST(conta_contabil AS STRING) AS codcontacontabil, 
    data AS dtperiodo, 
    tipo_de_lancamento AS tipolanc, 
    saldo AS vlrsaldo
FROM 
    avenue_finance.avenue_matera_api_saldo_contabil_legado sld
WHERE 
    CAST(sld.conta_contabil AS STRING) IN  (
                                '71410100000002',
                                '71510000000007',
                                '12110030000008',
                                '12110040000000',
                                '12110050000003',
                                '12110060000006',
                                '12110070000009',
                                '12110080000001',
                                '30330020000000',
                                '71780000000000',
                                '71799000000001',
                                '71330000000009',
                                '71370000000008',
                                '81450000000004',
                                '81754000000006',
                                '81757000000000',
                                '71999000000004',
                                '81999000000009',
                                '81580100000004',
                                '71590100000007'
                            )
; 

CREATE OR REPLACE TEMP TABLE avenue_funds_transfers AS
SELECT
    Year, 
    Month, 
    MonthName,
    Quarter, 
    QuarterName, 
    Company, 
    BUTotalFx + BETotalFx AS TotalFXQntParaExterior, 
    UBTotalFx + EBTotalFx AS TotalFXQntDoExterior, 
    BUTotalFx + BETotalFx + UBTotalFx + EBTotalFx AS TotalFXQnt, 
    BUAmountUsd + BEAmountUsd AS TotalFXVolumeParaExterior, 
    UBAmountUsd + EBAmountUsd AS TotalFXVolumeDoExterior, 
    BUAmountUsd + BEAmountUsd + UBAmountUsd + EBAmountUsd AS TotalFXVolume
FROM 
(
SELECT
  dmt.Year, 
  dmt.Month, 
  dmt.MonthName,
  dmt.Quarter, 
  dmt.QuarterName, 
  'AVENUE SECURITIES' AS Company, 
  CASE
    WHEN fac.Purpose = 'Availability' THEN 'Banking' ELSE 'Investments' END AS Purpose,
  COUNT(DISTINCT(CASE WHEN fac.Direction = 'BU' THEN fac.AvenueAccountId END)) BUTotalClients, 
  COUNT(DISTINCT(CASE WHEN fac.Direction = 'BU' THEN fac.FundsTransferId END)) BUTotalFx, 
  SUM(CASE WHEN fac.Direction = 'BU' THEN fac.AmountUsd ELSE 0 END) BUAmountUsd, 

  COUNT(DISTINCT(CASE WHEN fac.Direction = 'BE' THEN fac.AvenueAccountId END)) BETotalClients, 
  COUNT(DISTINCT(CASE WHEN fac.Direction = 'BE' THEN fac.FundsTransferId END)) BETotalFx, 
  SUM(CASE WHEN fac.Direction = 'BE' THEN fac.AmountUsd ELSE 0 END) BEAmountUsd, 

  COUNT(DISTINCT(CASE WHEN fac.Direction = 'UB' THEN fac.AvenueAccountId END)) UBTotalClients, 
  COUNT(DISTINCT(CASE WHEN fac.Direction = 'UB' THEN fac.FundsTransferId END)) UBTotalFx, 
  SUM(CASE WHEN fac.Direction = 'UB' THEN fac.AmountUsd ELSE 0 END) UBAmountUsd, 

  COUNT(DISTINCT(CASE WHEN fac.Direction = 'EB' THEN fac.AvenueAccountId END)) EBTotalClients, 
  COUNT(DISTINCT(CASE WHEN fac.Direction = 'EB' THEN fac.FundsTransferId END)) EBTotalFx, 
  SUM(CASE WHEN fac.Direction = 'EB' THEN fac.AmountUsd ELSE 0 END) EBAmountUsd
FROM 
  avenue_finance.fact_funds_transfers fac 
LEFT JOIN 
  avenue_finance.dim_time dmt 
    ON fac.DimTimeSK = dmt.DimTimeSK
WHERE 
  dmt.Date < DATE_TRUNC(CURRENT_DATE(),MONTH)
GROUP BY 
  ALL 
ORDER BY 
  1 DESC,2 DESC, 7
) 
;

CREATE OR REPLACE TEMP TABLE market_share_funds_transfers AS
SELECT DISTINCT
    dmt.Year, 
    dmt.Month, 
    dmt.MonthName,
    dmt.Quarter, 
    dmt.QuarterName, 
    nome_da_instituicao AS Company, 
    transferencia_p_exterior_quant AS TotalFXQntParaExterior, 
    transferencia_do_exterior_quant AS TotalFXQntDoExterior, 
    transferencia_p_exterior_quant + transferencia_do_exterior_quant AS TotalFXQnt, 
    transferencia_p_exterior_valor_us AS TotalFXVolumeParaExterior, 
    transferencia_do_exterior_valor_us AS TotalFXVolumeDoExterior,
    transferencia_p_exterior_valor_us + transferencia_do_exterior_valor_us AS TotalFXVolume 
FROM 
    avenue_finance.avenue_bacen_api_ranking_cambio_instituicao_financeiras fac
LEFT JOIN 
    avenue_finance.dim_time dmt 
        ON CAST(fac.ano as NUMERIC) = dmt.Year 
            AND CAST(fac.mes as NUMERIC) = dmt.Month
WHERE 
  UPPER(nome_da_instituicao) NOT LIKE '%AVENUE%' 
; 

DROP TABLE IF EXISTS avenue_finance.fact_ranking_funds_transfers; 
CREATE TABLE IF NOT EXISTS  avenue_finance.fact_ranking_funds_transfers AS
SELECT 
    Year, 
    Month, 
    MonthName,
    Quarter, 
    QuarterName, 
    Company, 
    TotalFXQntParaExterior, 
    TotalFXQntDoExterior, 
    TotalFXQnt, 
    TotalFXVolumeParaExterior, 
    TotalFXVolumeDoExterior,
    TotalFXVolume
FROM 
    market_share_funds_transfers
WHERE 
    UPPER(Company) NOT LIKE '%BEXS BANCO%'

UNION ALL 

SELECT
    Year, 
    Month, 
    MonthName,
    Quarter, 
    QuarterName, 
    Company, 
    TotalFXQntParaExterior, 
    TotalFXQntDoExterior, 
    TotalFXQnt, 
    TotalFXVolumeParaExterior, 
    TotalFXVolumeDoExterior,
    TotalFXVolume
FROM 
    avenue_funds_transfers

UNION ALL 

SELECT 
    market.Year, 
    market.Month, 
    market.MonthName,
    market.Quarter, 
    market.QuarterName, 
    market.Company, 
    market.TotalFXQntParaExterior - avenue.TotalFXQntParaExterior AS TotalFXQntParaExterior, 
    market.TotalFXQntDoExterior - avenue.TotalFXQntDoExterior AS TotalFXQntDoExterior, 
    market.TotalFXQnt - avenue.TotalFXQnt AS TotalFXQnt, 
    market.TotalFXVolumeParaExterior - avenue.TotalFXVolumeParaExterior AS TotalFXVolumeParaExterior, 
    market.TotalFXVolumeDoExterior - avenue.TotalFXVolumeDoExterior AS TotalFXVolumeDoExterior,
    market.TotalFXVolume - avenue.TotalFXVolume AS TotalFXVolume
FROM 
    market_share_funds_transfers market 
LEFT JOIN 
    avenue_funds_transfers avenue 
        ON market.Year = avenue.Year 
            AND market.Month = avenue.Month 
WHERE 
    UPPER(market.Company) LIKE '%BEXS BANCO%'
ORDER BY 
    Year, Month
; 

CALL `avenue_finance.LoadDimExternalClient`(ChangedDate,inserted,updated);

CALL `avenue_finance.LoadFactRevenueFPNA`(RevenueStartDate);


