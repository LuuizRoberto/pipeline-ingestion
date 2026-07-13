-- 2026-06-09 - Carga Inicial - dim_external_client
CREATE OR REPLACE TABLE `avenue_finance.dim_external_client` AS
SELECT
   CAST(-1 AS INT64)               AS DimExternalClientSK
  ,CAST('Not Applicable'  AS STRING)            AS CustomerExternalId
  ,CAST('Not Applicable'  AS STRING)            AS AvenueAccountId
  ,CAST('Not Applicable'  AS STRING)            AS CustomerEmail
  ,CAST('Not Applicable'  AS STRING)            AS CustomerCPF
  ,CAST(NULL AS DATETIME)          AS ExternalAccountCreatedDate
  ,'Not Applicable'                AS IsAvenueClient
  ,'Not Applicable'                AS ClientSource
  ,DATETIME '1900-01-01 00:00:00'  AS DimStartDate
  ,DATETIME '9999-12-31 00:00:00'  AS DimEndDate
  ,1                               AS DimActiveFlag
;

INSERT INTO `avenue_finance.dim_external_client`
WITH salesforce_last AS (
    SELECT * FROM (
        SELECT
            CPF__c
            ,Id
            ,CreatedDate
            ,ROW_NUMBER() OVER (PARTITION BY CPF__c ORDER BY CreatedDate DESC) AS LastVersion
        FROM
            `avenue_datalake_latest_snapshot.salesforce_avenue_avenuesecurities_salesforce_Account`
        WHERE
            CPF__c IS NOT NULL
    )
    WHERE 
      LastVersion = 1 
),

myprofit_base AS (
    SELECT
         payer_cpf_cnpj
        ,customer_id
        ,customer_name
        ,email
        ,DATETIME(paid_at, 'America/Sao_Paulo')                                       AS paid_at
        ,MIN(DATETIME(paid_at, 'America/Sao_Paulo')) OVER (PARTITION BY customer_id)  AS ExternalAccountCreatedDate
        ,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_at DESC)           AS LastRegs
    FROM
        `avenue_datalake_latest_snapshot.iugu_myprofit_api_reports_invoices`
    WHERE
        paid_at  IS NOT NULL
    AND 
      payer_cpf_cnpj IS NOT NULL
)

SELECT
   ROW_NUMBER() OVER (ORDER BY mp.customer_id)  AS DimExternalClientSK
  ,mp.customer_id                               AS CustomerExternalId
  ,accmap.key_name                              AS AvenueAccountId
  ,mp.email                                     AS CustomerEmail
  ,mp.payer_cpf_cnpj                            AS CustomerCPF
  ,mp.ExternalAccountCreatedDate
  ,CASE
      WHEN sf.Id IS NOT NULL THEN 'Avenue Client'
      ELSE  'Not Avenue Client'
   END                                          AS IsAvenueClient
  ,'MyProfit'                                   AS ClientSource
  ,DATETIME '1900-01-01 00:00:00'               AS DimStartDate
  ,DATETIME '9999-12-31 00:00:00'               AS DimEndDate
  ,1                                            AS DimActiveFlag
FROM myprofit_base mp
LEFT JOIN 
    salesforce_last sf
      ON  sf.CPF__c = mp.payer_cpf_cnpj
LEFT JOIN
    `avenue_datalake_latest_snapshot.datastore_trading_prod_av_default_default_accountmapping_prod` accmap
       ON accmap.SalesForceId = sf.Id
WHERE
    mp.LastRegs = 1
;



