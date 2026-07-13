SELECT
  oms.*,
  acc.Tipo_de_Conta__c,
  acc.Categoria__c,
  fac.TotalNetDol
FROM
  av_datalake_l2.cloudsql_trading_prod_av_alloydb_investments_prd.public_oms_positions oms
    LEFT JOIN av_datalake_l2.salesforce_avenue_avenuesecurities_salesforce.account acc
      ON oms.account_id = acc.Avenue_ID__c
    LEFT JOIN av_datalake_l3.data_warehouse.fact_active_clients fac
      ON oms.account_id = fac.AvenueAccountId
      AND fac.RefDate
        = (
          SELECT
            MAX(RefDate)
          FROM
            av_datalake_l3.data_warehouse.fact_active_clients
        )
WHERE
  oms.instr_category = 7
  AND oms.entry_date
    = (
      SELECT
        MAX(entry_date)
      FROM
        av_datalake_l2.cloudsql_trading_prod_av_alloydb_investments_prd.public_oms_positions
    )
  AND oms.total_qty < 0
