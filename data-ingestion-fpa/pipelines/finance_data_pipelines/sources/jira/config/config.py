JIRA_CONFIG = {
    "project_id": "data-prod-295413",
    "service": "jira",
    "type": "atlassian",
    "instance": "https://avenue-securities.atlassian.net",
    "bq_project": "data-prod-295413",  # projeto GCP do BigQuery — pode diferir do project_id
    "bq_dataset": "avenue_finance",    # dataset do BigQuery — alterar aqui se mudar
    "sec": {
        "sm_user_secret_id": "user-jira-avenue-avenuesec-cloud-platform",
        "sm_token_secret_id": "token-jira-avenue-avenuesec-cloud-platform",
    },
    "tables": {
        "issues": {
            "project": "PROVIDER",
            "endpoint": "rest/api/3/search/jql",
            "extract_mode": "update",
            "primary_key": "issue_id",
            "numeric_id_columns": ["issue_id"],
            "changed_range": "3 days",
            "changed_date": "updated",
            "databricks_table_target": "avenue_finance.default.jira_issues_providers",
            "bigquery_table_target": "data-prod-295413.avenue_finance.avenue_jira_atlassian_issues_providers",
            "volume_path": "/Volumes/avenue_finance/default/data-finance/jira/issues/providers",
            "gcs_snapshot_path": None,  # ex: "gs://finance-data/jira/issues_providers"
            "fields_table": [
                "created",
                "issue_key",
                "contracting_company",
                "type_of_product_or_service",
                "centro_de_custo",
                "cost_center_",
                "center_cost",
                "cost_center",
                "centro_de_custo",
                "novo_centro_de_custos",
                "what_is_the_duration",
                "data_inicio",
                "data_fim",
                "valor",
                "frequency",
                "inform_the_country_to_which_payment_will_be_made_from_a_supplier_an_american_supplier_may_have_a_receiving_company_in_another_payment_location_please_confirm_this_information",
                "responsavel_pela_contratacao",
                "tipo_de_solicitacao",
                "orcado",
                "cnpj",
                "reporter",
                "approvals",
                "nome_do_responsavel_pelo_preenchimento_deste_formulario",
                "razao_social_da_sociedade_contratada",
                "numero_de_registro_da_sociedade_se_brasileira_cnpj_se_nao_brasileira_o_numero_de_registro_da_sociedade_no_pais_de_origem",
                "area_de_atuacao_deste_fornecedor",
                "area_interna_contratante",
                "start_date",
                "end_date",
                "updated",
            ],
        }
    },
}


