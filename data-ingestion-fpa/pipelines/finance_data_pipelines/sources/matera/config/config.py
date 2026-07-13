MATERA_CONFIG = {
    "project_id": "data-prod-295413",
    "service": "matera",
    "type": "api",
    "instance": "contabil",
    "bq_project": "data-prod-295413",  # projeto GCP do BigQuery — pode diferir do project_id
    "bq_dataset": "avenue_finance",    # dataset do BigQuery — alterar aqui se mudar
    "sec": {
        "sm_user_secret_id": "user-matera-avenue-api",
        "sm_pwd_secret_id": "pwd-matera-avenue-api",
        "host": "https://backoffice-gestao.legados.prd.avenue.maas.link/",
    },
    "tables": {
        "evento_contabil": {
            "endpoint": "ct/api/v1/evento-contabil/{cnpjempresa}",
            "data_path": "data",
            "cnpjempresa": "61384004000105",
            "primary_key": "id",
            "databricks_table_target": "avenue_finance.matera.evento_contabil",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_matera_api_evento_contabil",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/matera/evento_contabil",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/matera/evento_contabil"
        },
        "accounting_facts": {
            "endpoint": "ct/api/v1/accounting-facts",
            "data_path": "data.items",
            "company_code": ["1", "328", "329", "333", "339"],
            "page_size": 1000,
            "primary_key": [
                "companycode",
                "accountingtypeCode",
                "releasenumber",
                "releasetype",
                "ledgeraccountcode",
                "accountingreleasedate",
                "guidenumber",                    # diferencia registros da mesma release em guias distintas
                "counterpartyaccountcode",        # conta de contrapartida
                "checkdigitnumbercounterpart",    # dígito verificador da contrapartida
            ],
            "numeric_id_columns": ["releasenumber", "guidenumber", "checkdigitnumbercounterpart"],
            "value_columns": ["value"],  # colunas financeiras para reconciliação
            "databricks_table_target": "avenue_finance.matera.accounting_facts",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_matera_api_accounting_facts",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/matera/accounting_facts",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/matera/accounting_facts"
        },
        "lancamento_contabil": {
            "endpoint": "ct/api/v1/consulta-lancamento/{id_empresa}/{ano}/{num_guia}/{cod_tipo}",
            "cod_tipo": "O",
            "data_path": "data",
            "primary_key": [
                "dtlanccontabil",
                "dtgeracao",
                "numlanc",
                "codcontacontabil",
                "idcentrocusto",
                "idlanccontacontabil",
                "numguia",
            ],
            "numeric_id_columns": ["num_guia"],
            "value_columns" : ["valor"],
            "guia_lookback": 100,
            "guia_lookahead": 200,
            "databricks_table_target": "avenue_finance.matera.lancamento_contabil",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_matera_api_lancamento_contabil",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/matera/lancamento_contabil",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/matera/lancamento_contabil"
        },
        "conta_contabil": {
            "endpoint": "ct/api/v1/conta-contabil/{id_plano_contas}/{cod_conta_contabil}",
            "data_path": "data",
            "id_plano_contas": "2",
            "ids_file": "sources/matera/config/conta_contabil_ids.txt",
            # Fonte para complementar a lista de IDs: contas que já tiveram lançamento.
            # A lista final é a união (arquivo ∪ distinct desta coluna).
            "codes_source_table":  "data-prod-295413.avenue_finance.avenue_matera_api_accounting_facts",
            "codes_source_column": "ledgeraccountcode",
            "primary_key": "codcontacontabil",
            "databricks_table_target": "avenue_finance.matera.conta_contabil",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_matera_api_conta_contabil",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/matera/conta_contabil",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/matera/conta_contabil"
        },
        "saldo_contabil": {
            "endpoint": "ct/api/v1/consulta-saldo-contabil/{id_empresa}/{id_plano_contas}/{cod_conta}/{dt_periodo}/{tipo_contabil}",
            "data_path": "data",
            "id_plano_contas": "2",
            "tipo_contabil": "O",
            "company_code": ["1"],
            "ids_file_by_empresa": {
                "1":   "sources/matera/config/conta_contabil_ids_1.txt",
                "328": "sources/matera/config/conta_contabil_ids_328.txt",
                "329": "sources/matera/config/conta_contabil_ids_329.txt",
                "333": "sources/matera/config/conta_contabil_ids_333.txt",
                "339": "sources/matera/config/conta_contabil_ids_339.txt",
            },
            "primary_key": ["idempresa", "codcontacontabil", "dtperiodo", "tipocontabil"],
            "value_columns": ["vlrsaldo"],  # colunas financeiras para reconciliação
            "databricks_table_target": "avenue_finance.matera.saldo_contabil",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_matera_api_saldo_contabil",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/matera/saldo_contabil",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/matera/saldo_contabil"
        },
    },
}
