-- 2026-03-02 FactBankings
CREATE OR REPLACE PROCEDURE `data-prod-295413.avenue_data_warehouse_dev.LoadBankingFacts`(IN RefferedDate DATETIME)
BEGIN
    DECLARE BankingStartDate DATE;
    DECLARE BankingEndDate DATE;

    SET BankingEndDate = DATE(RefferedDate);
    SET BankingStartDate = DATE("2025-01-01");

    SELECT RefferedDate;
    SELECT TIMESTAMP(DATE_ADD(BankingEndDate, INTERVAL 1 DAY));

    IF BankingStartDate < DATE('2025-01-01') THEN
        RAISE USING MESSAGE = 'Não é possível reprocessar dados anteriores a 2026-01-01;';
    ELSE
        --Cria tabela temporária com as contas que realizaram movimento nos ultimos 30 dias
        CREATE TEMP TABLE temp_banking_movement AS
        SELECT DISTINCT
            AvenueAccountId,
            1 AS HasTransaction
        FROM 
            avenue_datalake_layer2.banking_movements mov
        WHERE 
            mov.HistoricId IN (4026,4027,4142,4143)
        AND 
            mov.CreatedDate >= DATE_SUB(RefferedDate, INTERVAL 29 DAY)
        AND
            mov.CreatedDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
        ;


        -- Cria tabela temporária com as contas que realizaram movimento nos ultimos 90 dias
        CREATE TEMP TABLE temp_banking_movement_90d AS
        SELECT DISTINCT
            AvenueAccountId,
            1 AS HasTransaction
        FROM 
            avenue_datalake_layer2.banking_movements mov
        WHERE 
            mov.HistoricId IN (4026,4027,4142,4143)
        AND 
            mov.SettlementDate >= DATE_SUB(RefferedDate, INTERVAL 89 DAY)
        AND
            mov.SettlementDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
        ;


        --Cria tabela temporária com data da primeira despesa
        CREATE TEMP TABLE temp_banking_first_spend_date AS
        SELECT DISTINCT
            AvenueAccountId
        ,FIRST_VALUE(CreatedDate) OVER 
            (PARTITION BY AvenueAccountId ORDER BY CreatedDate 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS CreatedDate
        ,FIRST_VALUE(SettlementDate) OVER 
            (PARTITION BY AvenueAccountId ORDER BY CreatedDate 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS FirstSpendDate
        FROM (
            SELECT
                mov.AvenueAccountId,
                mov.CreatedDate,
                mov.SettlementDate
            FROM
                avenue_datalake_layer2.banking_movements mov
            WHERE
                mov.CreatedDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
            AND
                mov.HistoricId IN(4026, 4027,4142,4143)
            )
        ;

        --Cria tabela temporária com as informações da primeira despesa
        CREATE TEMP TABLE temp_banking_first_spend AS
        SELECT * EXCEPT(FirstSpend) FROM
        (
            SELECT
                AvenueAccountId,
                SettlementDate,
                HistoricId,
                Description,
                EntryId,
                DebitCredit,
                AmountUS,
                ROW_NUMBER() OVER (PARTITION BY AvenueAccountId ORDER BY SettlementDate,EntryId) AS FirstSpend
            FROM
            (
                SELECT
                    mov.AvenueAccountId,
                    mov.SettlementDate,
                    mov.HistoricId,
                    mov.Description,
                    mov.EntryId,
                    mov.DebitCredit,
                    SUM(mov.AmountUS) AS AmountUS
                FROM
                    avenue_datalake_layer2.banking_movements mov
                INNER JOIN
                    temp_banking_first_spend_date fsd
                        ON  mov.AvenueAccountId = fsd.AvenueAccountId
                        AND mov.CreatedDate = fsd.CreatedDate
                        AND mov.SettlementDate = fsd.FirstSpendDate
                WHERE
                    mov.CreatedDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
                AND
                    mov.HistoricId IN(4026,4027,4142,4143)
                GROUP BY
                    mov.AvenueAccountId,
                    mov.SettlementDate,
                    mov.HistoricId,
                    mov.Description,
                    mov.EntryId,
                    mov.DebitCredit
            )
        )
        WHERE
            FirstSpend = 1
        ;

        --Cria tabela temporária com data do primeiro depósito
        CREATE TEMP TABLE temp_banking_first_funding_date AS
        SELECT DISTINCT
            AvenueAccountId
            ,FIRST_VALUE(CreatedDate) OVER 
                (PARTITION BY AvenueAccountId ORDER BY CreatedDate 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS CreatedDate
            ,FIRST_VALUE(SettlementDate) OVER 
                (PARTITION BY AvenueAccountId ORDER BY CreatedDate 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS FirstFundingDate
        FROM (
            SELECT
                mov.AvenueAccountId,
                mov.CreatedDate,
                mov.SettlementDate
            FROM
                avenue_datalake_layer2.banking_movements mov
            WHERE
                mov.CreatedDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
            AND
                mov.HistoricId IN(3026, 3027, 3028, 3029, 9003, 3035, 3037, 3045, 3046, 3050,3108)
        )
        ;


        --Cria tabela temporária com as informações do primeiro depósito
        CREATE TEMP TABLE temp_banking_first_funding AS
        SELECT 
            mov.AvenueAccountId,
            mov.SettlementDate,
            mov.HistoricId,
            mov.Description,
            mov.DebitCredit,
            SUM(mov.AmountUS) AS AmountUS
        FROM 
            avenue_datalake_layer2.banking_movements mov
        INNER JOIN 
            temp_banking_first_funding_date ffd    
                ON  mov.AvenueAccountId = ffd.AvenueAccountId
                AND mov.CreatedDate = ffd.CreatedDate
                AND mov.SettlementDate = ffd.FirstFundingDate
        WHERE 
            mov.CreatedDate < DATE_ADD(RefferedDate, INTERVAL 1 DAY)
        AND 
            mov.HistoricId IN(3026, 3027, 3028, 3029, 9003, 3035, 3037,3045, 3046, 3050,3108)
        GROUP BY 
            mov.AvenueAccountId,
            mov.SettlementDate,
            mov.HistoricId,
            mov.Description,
            mov.DebitCredit
        ;


        -- temp clientes que possuem cartão fisico.
        -- Verifica se o cliente possui cartão fisico e se esse cartão fisico está ativo. 
        -- Até Agosto/24 essa regra era criada com base na tabela datastore_trading_prod_av_default_default_banking_card_info_prod. 
        -- A tabela do datastore era do tipo append e por conta disso conseguiamos olhar para o datalake e criar uma visão diária dos clientes que possuiam cartão naquele dia especifico. 
        -- Essa tabela foi descontinuada e por conta disso a partir de Setembro/24. 
        -- Essa nova tabela é do tipo update, por conta disso, é necessário criar uma nova regra com uma data de vigência do registro, para verificar o periodo que o cliente possuiu um cartão fisico e o status do cartão fisico. 
        -- Importante ressaltar que após ter um primeiro registro de cartão fisico o cliente sempre passará a ter o cartão fisico, o que pode alterar é o status desse cartão. 

        CREATE TEMP TABLE temp_has_pyshical_card AS 
        SELECT
        *, 
        CASE
            WHEN LAG(updated_at,1) OVER (PARTITION BY account_id ORDER BY updated_at) IS NULL THEN created_at
            ELSE updated_at END AS StartDate,  
        CASE
            WHEN LEAD(updated_at,1) OVER (PARTITION BY account_id ORDER BY updated_at) IS NULL THEN 99991231
            ELSE LEAD(updated_at,1) OVER (PARTITION BY account_id ORDER BY updated_at) 
            END AS EndDate,
        FROM 
        (
        SELECT DISTINCT 
        created_at,
        updated_at,
        account_id, 
        status, 
        FROM
            `data-prod-295413.avenue_datalake.cloudsql_trading_prod_av_alloydb_db_banking_adm_prd_public_card_information`
        WHERE 
        type = "Physical"
        ) 
        ORDER BY 
        account_id,updated_at
        ;           

    -- Banking Accounts
        DELETE
        FROM
            `data-prod-295413.avenue_data_warehouse.fact_banking_active_accounts`
        WHERE
        `Date` >= BankingStartDate
        AND
        `Date` <= BankingEndDate
        ;
    
        -- Carrega a fato com dados de contas ativas ou não
        INSERT INTO `data-prod-295413.avenue_data_warehouse.fact_banking_active_accounts`
        SELECT
            CAST(FORMAT_DATE('%Y%m%d',t.Date) AS INT64) AS DimTimeSK,
            t.Date,
            COALESCE(dac.DimAccountSK,-1)       AS DimAccountSK,
            COALESCE(fofa.DimOfficeChannelSK,2) AS DimOfficeChannelSK,
            COALESCE(fofa.DimForeignFinderSK,1) AS DimForeignFinderSK,
            COALESCE(fofa.DimBranchSK,1)        AS DimBranchSK,
            COALESCE(auc_r.DimAucRangeSK, 1) AS DimAucRangeSK,
            COALESCE(seg.DimClientSegmentSK, 1) AS DimClientSegmentSK,
            COALESCE(auc_r.DimTotalInvestedRangeBrlSK, 1) AS DimTotalInvestedRangeBrlSK,
            COALESCE(auc_r.DimTotalInvestedRangeUsdSK,1) AS DimTotalInvestedRangeUsdSK,
            mac.account_id                      AS AvenueAccountid,
            cast(null as string)                  AS MoneyBinBankingAccountId,
            COALESCE(fofa.OfficeId,'5258b0b5-aec2-4697-a4eb-03f5f7c7fcbd' ) as OfficeId, -- AVENUE B2C
            COALESCE(fofa.ForeignFinderId,'Self Directed') as ForeignFinderId,
            COALESCE(fofa.BranchId,'9cbe84a6b7091652b29231e3360eeb6d') as BranchId,
            DATE(mac.created,'America/Sao_Paulo') AS AccountCreationDate,
            COALESCE(tr.HasTransaction,0)  AS IsActive,
            COALESCE(tr90.HasTransaction,0)  AS IsActive90d,
            DATE(mac.created) AS AccountCreationDateGMT,
            COALESCE(dac.TotalInvested,0) AS TotalInvestedReal,
            COALESCE(dac.TotalInvested,0) / dol.Sell AS TotalInvestedDolar,
            COALESCE(Net.TotalNetReal,0) AS TotalNetReal,
            COALESCE(Net.TotalNetDol,0) AS TotalNetDol,
            CASE
                WHEN COALESCE(Net.TotalNetDol,0) < 50000 THEN 'Globalizer' 
                WHEN COALESCE(Net.TotalNetDol,0) >= 50000 AND COALESCE(Net.TotalNetDol,0) < 250000 THEN 'Access'
                WHEN COALESCE(Net.TotalNetDol,0) >= 250000 THEN 'Singular'
                END AS AucSegment, 
            CASE
                WHEN card.account_id IS NOT NULL THEN 1
                ELSE 0 END AS HasPhysicalCard, 
            CASE 
                WHEN card.Status = "ACTIVE" THEN 1 
                ELSE 0 END AS IsActiveCard,
            COALESCE(card.Status, "NO PHYSICAL CARD") AS CardStatus 
        FROM 
            `data-prod-295413.avenue_data_warehouse.dim_time` t   
        INNER JOIN 
            (SELECT 
                *,
                COALESCE(account_creation_at,created_at) as created,
                DATE_SUB(dl_export_date, INTERVAL 1 DAY) AS dt_ini,
                CASE WHEN LEAD(dl_export_date,1) OVER (PARTITION BY m.account_id ORDER BY dl_export_date) IS NULL THEN '9999-12-31' 
                ELSE DATE_SUB((LEAD(dl_export_date,1) OVER (PARTITION BY m.account_id ORDER BY dl_export_date)), INTERVAL 1 DAY) END  as dt_fim
            FROM 
                avenue_datalake.cloudsql_trading_prod_av_alloydb_db_banking_adm_prd_public_account_banking m
            WHERE 
                status not in (6,7) 
            ) mac
                ON  t.Date >= DATE(mac.created,'America/Sao_Paulo') 
                AND t.Date >= dt_ini 
                AND t.Date < dt_fim 
        INNER JOIN 
            `data-prod-295413.avenue_data_warehouse.dim_account` dac
                ON  mac.account_id = dac.AvenueAccountId
                AND t.Date >= dac.DimStartDate
                AND t.Date < dac.DimEndDate
        LEFT JOIN  
            `data-prod-295413.avenue_data_warehouse.fact_office_foreign_finder_account` fofa 
                ON  mac.account_id = fofa.AvenueAccountId 
                AND t.dimtimeSK = fofa.DimTimeSK
        LEFT JOIN  
            `data-prod-295413.avenue_data_warehouse.fact_dollar_ptax` dol 
                ON dol.Date = t.Date
        LEFT JOIN 
            (SELECT 
                DimTimeSk, 
                DimAccountSk,
                COALESCE(SUM(f.TotalNetDol),0)  as TotalNetDol, 
                COALESCE(SUM(f.TotalNetReal),0) as TotalNetReal
            FROM  
                `data-prod-295413.avenue_data_warehouse.fact_custody` f
            WHERE 
                f.DimAccountSK != -1
            AND
            f.`Date` >= BankingStartDate
            AND
            f.`Date` <= BankingEndDate
            GROUP BY 
                DimAccountSK, DimTimeSK
            ) AS Net 
                ON  Net.DimAccountSK = dac.DimAccountSK 
                AND Net.DimTimeSk = t.DimTimeSK
        LEFT JOIN
            (   SELECT 
                    t1.Date,
                    ent.AvenueAccountId,
                    4 AS accounttype,
                    1 AS HasTransaction 
                FROM 
                    avenue_data_warehouse.dim_time t1   
                INNER JOIN 
                    avenue_datalake_layer2.banking_movements ent
                        ON  ent.HistoricId IN (4026,4027,4142,4143)
                        AND ent.CreatedDate >= DATE_SUB(t1.Date, INTERVAL 29 DAY)
                        AND ent.CreatedDate < DATE_ADD(t1.Date, INTERVAL 1 DAY)
                WHERE
                    t1.DimTimesk >= 20210310
                AND 
                    t1.`Date` >= BankingStartDate
                AND
                    t1.`Date` <= BankingEndDate
                GROUP BY 
                    ALL
            ) AS tr 
                ON  tr.Date = t.Date 
                AND mac.account_id = tr.AvenueAccountId
        LEFT JOIN
            (   SELECT 
                    t1.Date,
                    ent.AvenueAccountId,
                    4 AS accounttype,
                    1 AS HasTransaction 
                FROM 
                    avenue_data_warehouse.dim_time t1   
                INNER JOIN 
                    avenue_datalake_layer2.banking_movements ent
                        ON  ent.HistoricId IN (4026,4027,4142,4143)
                        AND ent.CreatedDate >= DATE_SUB(t1.Date, INTERVAL 89 DAY)
                        AND ent.CreatedDate < DATE_ADD(t1.Date, INTERVAL 1 DAY)
                WHERE
                    t1.DimTimesk >= 20210310
                AND 
                    t1.`Date` >= BankingStartDate
                AND
                    t1.`Date` <= BankingEndDate
                GROUP BY 
                    ALL
            ) AS tr90 
                ON tr90.Date = t.Date 
                AND mac.account_id = tr90.AvenueAccountId     
        LEFT JOIN 
            temp_has_pyshical_card card 
            ON mac.account_id = card.account_id 
                AND CAST(FORMAT_DATE('%Y%m%d',t.Date) AS INT64) >= card.StartDate 
                AND CAST(FORMAT_DATE('%Y%m%d',t.Date) AS INT64) < card.EndDate 
        LEFT JOIN
        `avenue_datalake_layer2.client_auc_segment` seg
            ON  CAST(FORMAT_DATE('%Y%m%d',t.Date) AS INT64) = seg.DimTimeSK
            AND mac.account_id = seg.AvenueAccountId 
        LEFT JOIN
        `avenue_datalake_layer2.client_auc_range` auc_r
            ON  CAST(FORMAT_DATE('%Y%m%d',t.Date) AS INT64) = auc_r.DimTimeSK
            AND mac.account_id = auc_r.AvenueAccountId 
        WHERE 
            dac.SalesforceAccountCanceled != 'Canceled'
        AND
            t.DimTimesk >= 20210310
        AND 
            dac.AvenueAccountId != '0546a589-7414-44f2-8fc7-29bb579c03f6'
        AND 
        t.`Date` >= BankingStartDate
        AND
        t.`Date` <= BankingEndDate
        ;

        -- Apaga os dados da fact_banking_evasion referentes ao período informado
        DELETE  
        FROM 
            `data-prod-295413.avenue_data_warehouse.fact_banking_evasion` 
        WHERE 
            DimTimeSK >= CAST(FORMAT_DATE('%Y%m%d', BankingStartDate) AS INT64)
        AND 
            DimTimeSK <= CAST(FORMAT_DATE('%Y%m%d', BankingEndDate) AS INT64)
        ;
        
        -- Insere os dados na fact_banking_evasion referentes ao período informado
        INSERT INTO
            `data-prod-295413.avenue_data_warehouse.fact_banking_evasion` 
        SELECT 
            * EXCEPT(Status)
        FROM 
            (SELECT 
                fbl.DimTimeSK,
                fbl.Date,
                fbl.DimAccountSK,
                fbl.DimOfficeChannelSK,
                fbl.DimForeignFinderSK,
                fbl.DimBranchSK,
                COALESCE(auc_r.DimAucRangeSK,1) AS DimAucRangeSK,
                COALESCE(seg.DimClientSegmentSK,1) AS DimClientSegmentSK,
                COALESCE(auc_r.DimTotalInvestedRangeBrlSK,1) AS DimTotalInvestedRangeBrlSK,
                COALESCE(auc_r.DimTotalInvestedRangeUsdSK,1) AS DimTotalInvestedRangeUsdSK,
                fbl.AvenueAccountId,
                fbl.MoneyBinBankingAccountId,
                fbl.OfficeId,
                fbl.ForeignFinderId,
                fbl.BranchId,
                CASE 
                    WHEN fbl.PreviousStatus = 0 AND fbl.CurrentStatus = 0 THEN 'Inactive'
                    WHEN fbl.PreviousStatus = 0 AND fbl.CurrentStatus = 1 THEN 'Activation'
                    WHEN fbl.PreviousStatus = 1 AND fbl.CurrentStatus = 1 THEN 'Active'
                    WHEN fbl.PreviousStatus = 1 AND fbl.CurrentStatus = 0 THEN 'Churn'
                    END AS Status    
            FROM 
                (SELECT
                    FIRST_VALUE(DimTimeSK) OVER (
                        PARTITION BY AvenueAccountId ORDER BY DimTimeSK ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS FirstDimTimeSK,  
                    FIRST_VALUE(fac.IsActive90d) OVER (
                        PARTITION BY AvenueAccountId ORDER BY DimTimeSK ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS FirstStatus, 
                    CASE 
                        WHEN LAG(fac.IsActive90d,1) OVER (PARTITION BY AvenueAccountId ORDER BY DimTimeSK) IS NULL THEN fac.IsActive90d
                        ELSE LAG(fac.IsActive90d,1) OVER (PARTITION BY AvenueAccountId ORDER BY DimTimeSK) 
                        END AS PreviousStatus,
                    fac.IsActive90d AS CurrentStatus, 
                    fac.*
                FROM 
                    `data-prod-295413.avenue_data_warehouse.fact_banking_active_accounts` fac
                ORDER BY 
                    fac.DimTimeSK 
                ) as fbl

            LEFT JOIN
            `avenue_datalake_layer2.client_auc_segment` seg
                ON  fbl.DimTimeSK = seg.DimTimeSK
                    AND fbl.AvenueAccountId = seg.AvenueAccountId 
            LEFT JOIN
            `avenue_datalake_layer2.client_auc_range` auc_r
                ON  fbl.DimTimeSK = auc_r.DimTimeSK
                    AND fbl.AvenueAccountId = auc_r.AvenueAccountId     
            WHERE     
                fbl.CurrentStatus != fbl.PreviousStatus
            )
        WHERE 
            DimTimeSK >= CAST(FORMAT_DATE('%Y%m%d', BankingStartDate) AS INT64)
        AND 
            DimTimeSK <= CAST(FORMAT_DATE('%Y%m%d', BankingEndDate) AS INT64)
        AND 
            Status = 'Churn'
        AND 
            AvenueAccountId != '0546a589-7414-44f2-8fc7-29bb579c03f6'
        ;   
        
        -- Apaga os dados da fact_banking_activation referentes ao período informado
        DELETE
        FROM 
            `data-prod-295413.avenue_data_warehouse.fact_banking_activation` 
        WHERE 
            DimTimeSK >= CAST(FORMAT_DATE('%Y%m%d', BankingStartDate) AS INT64)
        AND 
            DimTimeSK <= CAST(FORMAT_DATE('%Y%m%d', BankingEndDate) AS INT64)
        ;
        
        -- Insere os dados na fact_banking_activation referentes ao período informado
        INSERT INTO
            `data-prod-295413.avenue_data_warehouse.fact_banking_activation` 
        SELECT 
            * EXCEPT(Status)
        FROM 
            (SELECT 
                fbl.DimTimeSK,
                fbl.Date,
                fbl.DimAccountSK,
                fbl.DimOfficeChannelSK,
                fbl.DimForeignFinderSK,
                fbl.DimBranchSK,
                COALESCE(auc_r.DimAucRangeSK,1) AS DimAucRangeSK,
                COALESCE(seg.DimClientSegmentSK,1) AS DimClientSegmentSK,
                COALESCE(auc_r.DimTotalInvestedRangeBrlSK,1) AS DimTotalInvestedRangeBrlSK,
                COALESCE(auc_r.DimTotalInvestedRangeUsdSK,1) AS DimTotalInvestedRangeUsdSK,
                fbl.AvenueAccountId,
                fbl.MoneyBinBankingAccountId,
                fbl.OfficeId,
                fbl.ForeignFinderId,
                fbl.BranchId,
                CASE 
                    WHEN fbl.PreviousStatus = 0 AND fbl.CurrentStatus = 0 THEN 'Inactive'
                    WHEN fbl.PreviousStatus = 0 AND fbl.CurrentStatus = 1 THEN 'Activation'
                    WHEN fbl.PreviousStatus = 1 AND fbl.CurrentStatus = 1 THEN 'Active'
                    WHEN fbl.PreviousStatus = 1 AND fbl.CurrentStatus = 0 THEN 'Churn'
                    END AS Status    
            FROM 
                (SELECT
                    FIRST_VALUE(DimTimeSK) OVER (
                        PARTITION BY AvenueAccountId ORDER BY DimTimeSK ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS FirstDimTimeSK,  
                    FIRST_VALUE(fac.IsActive90d) OVER (
                        PARTITION BY AvenueAccountId ORDER BY DimTimeSK ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS FirstStatus, 
                    CASE 
                        WHEN LAG(fac.IsActive90d,1) OVER (PARTITION BY AvenueAccountId ORDER BY DimTimeSK) IS NULL THEN fac.IsActive90d
                        ELSE LAG(fac.IsActive90d,1) OVER (PARTITION BY AvenueAccountId ORDER BY DimTimeSK) 
                        END AS PreviousStatus,
                    fac.IsActive90d AS CurrentStatus, 
                    fac.*
                FROM 
                    `data-prod-295413.avenue_data_warehouse.fact_banking_active_accounts` fac
                ORDER BY 
                    fac.DimTimeSK 
                ) as fbl
            LEFT JOIN
            `avenue_datalake_layer2.client_auc_segment` seg
                ON  fbl.DimTimeSK = seg.DimTimeSK
                    AND fbl.AvenueAccountId = seg.AvenueAccountId
            LEFT JOIN
            `avenue_datalake_layer2.client_auc_range` auc_r
                ON  fbl.DimTimeSK = auc_r.DimTimeSK
                    AND fbl.AvenueAccountId = auc_r.AvenueAccountId 
            WHERE     
                fbl.CurrentStatus != fbl.PreviousStatus
            )
        WHERE 
            DimTimeSK >= CAST(FORMAT_DATE('%Y%m%d', BankingStartDate) AS INT64)
        AND 
            DimTimeSK <= CAST(FORMAT_DATE('%Y%m%d', BankingEndDate) AS INT64)
        AND 
            Status = 'Activation'
        AND 
            AvenueAccountId != '0546a589-7414-44f2-8fc7-29bb579c03f6'
        ;  

    END IF;

END;