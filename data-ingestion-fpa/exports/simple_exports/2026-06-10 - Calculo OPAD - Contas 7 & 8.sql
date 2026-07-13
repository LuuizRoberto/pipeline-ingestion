-- 2026-06-10 - Calculo OPAD - Contas 7 & 8
DROP TABLE IF EXISTS avenue_finance.avenue_matera_lancamentos_conta_contabil_opad; 
CREATE OR REPLACE TABLE avenue_finance.avenue_matera_lancamentos_conta_contabil_opad AS
SELECT 
    accountingdate, 
    ledgeraccountcode, 
    SUM(ValueAdjusted) AS ValorLancTotal
FROM 
(
SELECT *, 
    CASE 
        WHEN acc.releasetype = 'C' THEN acc.value 
        WHEN acc.releasetype = 'D' THEN acc.value * -1
        END AS ValueAdjusted,
FROM 
    avenue_finance.avenue_matera_api_accounting_facts acc
WHERE 
    EXTRACT(MONTH FROM CAST(accountingdate AS DATE)) IN (6, 12)
AND
    Systemcode != 'RES' 
AND 
    (ledgeraccountcode LIKE '8%' OR ledgeraccountcode LIKE '7%')
AND 
    companycode = 1
) 
GROUP BY ALL 
;  


SELECT 
    dtperiodo,
    codcontacontabilpai, 
    codcontacontabil,
    nomecontacontabil, 
    tipolanc, 
    vlrsaldo, 
    vlrsaldo_mesanterior,
    COALESCE(movs.ValorLancTotal,0) as ValorMovTotal, 
    vlrsaldo_mesanterior + COALESCE(movs.ValorLancTotal,0) as ValorLancTotal, 
FROM 
(
SELECT 
    *, 
    CASE 
        WHEN EXTRACT(MONTH FROM CAST(dtperiodo AS DATE)) IN (6, 12) THEN 
                LAG(vlrsaldoajt) OVER (PARTITION BY codcontacontabil ORDER BY dtperiodo)
                ELSE vlrsaldoajt
                END AS vlrsaldo_mesanterior
FROM 
(
SELECT 
    CAST(cnt.codcontacontabilpai AS STRING) AS codcontacontabilpai,
    CAST(sld.codcontacontabil AS STRING) AS codcontacontabil, 
    cnt.nomecontacontabil, 
    CAST(sld.dtperiodo AS DATE) AS dtperiodo, 
    sld.tipolanc,
    sld.vlrsaldo, 
    CASE 
        WHEN sld.tipolanc = 'C' THEN sld.vlrsaldo 
        WHEN sld.tipolanc = 'D' THEN sld.vlrsaldo * -1
        END AS vlrsaldoajt,
FROM    
    avenue_finance.avenue_matera_api_saldo_contabil sld
LEFT JOIN 
    avenue_finance.avenue_matera_api_conta_contabil cnt
        ON sld.codcontacontabil = cnt.codcontacontabil
WHERE 
    (sld.codcontacontabil LIKE '8%' OR sld.codcontacontabil LIKE '7%')
) 
) a 
LEFT JOIN 
    avenue_finance.avenue_matera_lancamentos_conta_contabil_opad movs 
        ON a.codcontacontabil = movs.ledgeraccountcode
            AND CAST(a.dtperiodo AS DATE) = CAST(movs.accountingdate AS DATE) 
ORDER BY 
    codcontacontabilpai,  dtperiodo, codcontacontabil


