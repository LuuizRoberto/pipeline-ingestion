-- 2026-06-16 - Covered Call Clients
SELECT 
        'apex_982_file' AS PositionType,
        pos.AccountNumber, 
        pos.Cusip, 
        pos.Symbol, 
        pos.Description,
        pos.SecurityTypeCode, 
        pos.UnderlyingSymbol, 
        pos.ISIN, 
        pos.PositionType, 
        pos.OptionStrategy, 
        pos.TradeQuantity,
        pos.ClosingPrice, 
        pos.MarketValue,  
        dol.Sell as DollarPTAXSell,
        dol.Buy as DollarPTAXBuy
    FROM 
        `avenue_data_warehouse.temp_apex_custody_dates` tcd
    INNER JOIN 
      (
        SELECT
            *
        FROM
            `avenue_datalake.gcs_avenue_apex_self_directed_ext_982_avnu` 
        WHERE
            dl_export_date = CURRENT_DATE('America/Sao_Paulo')
        ) pos
           ON tcd.CustodyDate = pos.ProcessDate
    LEFT JOIN
        `avenue_data_warehouse.dim_account` dac
            ON pos.AccountNumber = dac.ApexSelfDirectId 
            AND tcd.Date >= dac.DimStartDate
            AND tcd.Date < dac.DimEndDate
    LEFT JOIN
        `avenue_data_warehouse.dim_product` dpr
            ON  pos.Symbol = dpr.Symbol
            AND tcd.CustodyDate >= dpr.DimStartDate
            AND tcd.CustodyDate < dpr.DimEndDate
    LEFT JOIN  
        `avenue_data_warehouse.fact_dollar_ptax` dol
            ON tcd.CustodyDate = dol.Date          
    LEFT JOIN
        `avenue_data_warehouse.vw_product_bonds` vwb
            ON pos.Symbol = vwb.MarketSymbol
    LEFT JOIN
        `avenue_data_warehouse.asset_prices` asp
            ON  pos.Symbol = asp.Symbol
            AND pos.ProcessDate >= asp.StartDate
            AND pos.ProcessDate < asp.EndDate 
    WHERE
        CAST(TradeQuantity AS FLOAT64) < 0 
    AND 
        OptionStrategy = 'Covered Call'
    ;


