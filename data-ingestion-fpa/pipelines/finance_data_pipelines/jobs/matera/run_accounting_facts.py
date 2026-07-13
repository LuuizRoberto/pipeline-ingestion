"""
Job: accounting_facts — ingestão diária com delete por janela + insert.

Estratégia padrão (sem parâmetros):
    - Reprocessa os 2 meses anteriores ao dia atual

Estratégia de reprocessamento (com parâmetros via Databricks widgets):
    - reprocess_start_date: data de início do reprocessamento (YYYY-MM-DD)
    - reprocess_end_date:   data de fim do reprocessamento    (YYYY-MM-DD)

    Exemplo no Databricks:
        reprocess_start_date = 2025-01-01
        reprocess_end_date   = 2025-12-31

Agendado no Databricks como task independente.
Aponta para: jobs/matera/run_accounting_facts.py
"""

import calendar
import datetime as pydt
from datetime import timedelta

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

from pipelines.finance_data_pipelines.core import (
    delete_bq_window,
    get_logger,
    parse_reprocess_args,
    validate_sql_identifier,
)
from pipelines.finance_data_pipelines.sources.matera.config.config import MATERA_CONFIG
from pipelines.finance_data_pipelines.sources.matera.extractor.extractor import ExtractorMatera

logger = get_logger(__name__)


def _get_months_in_range(start_date: pydt.datetime, end_date: pydt.datetime) -> list:
    """Retorna lista com o primeiro dia de cada mês dentro do range."""
    months = []
    current = start_date.replace(day=1)
    while current <= end_date:
        months.append(current)
        current = current + relativedelta(months=1)
    return months


def main() -> None:
    # Resolvido dentro de main() — evita NoneType se o módulo for importado
    # antes da SparkSession existir (ex: testes, imports antecipados).
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "SparkSession não encontrada. "
            "Este job deve ser executado dentro do Databricks."
        )

    table_extract  = "accounting_facts"
    table_config   = MATERA_CONFIG["tables"][table_extract]
    target_table   = table_config["databricks_table_target"]
    bq_table       = table_config["bigquery_table_target"]
    company_code_list = table_config["company_code"]
    page_size      = table_config["page_size"]

    dl_export_date = pydt.datetime.today()

    # ------------------------------------------------------------------
    # Lê parâmetros via task parameters do Databricks
    # Formato no campo Parameters da task:
    # ["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
    # Se não informados, roda no modo diário padrão
    # ------------------------------------------------------------------
    reprocess_start, reprocess_end = parse_reprocess_args()

    if reprocess_start and reprocess_end:
        # Modo reprocessamento — usa datas passadas via widget
        start_date = pydt.datetime.strptime(reprocess_start, "%Y-%m-%d")
        end_date   = pydt.datetime.strptime(reprocess_end, "%Y-%m-%d")
        logger.info("Modo REPROCESSAMENTO | start: %s | end: %s", reprocess_start, reprocess_end)
    else:
        # Modo padrão — 2 meses anteriores
        refdate    = dl_export_date - timedelta(days=1)
        start_date = refdate.replace(day=1) - relativedelta(months=1)
        end_date   = refdate
        logger.info("Modo DIÁRIO | start: %s | end: %s",
                    start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    months_to_process = _get_months_in_range(start_date, end_date)
    window_start = start_date.strftime("%Y-%m-%d")

    extractor = ExtractorMatera(MATERA_CONFIG, export_date=dl_export_date)

    logger.info("Empresas: %s | Meses a processar: %d", company_code_list, len(months_to_process))

    resultado     = "Falha"
    error_message = None
    try:
        for company_code in company_code_list:
            logger.info("Processando empresa: %s", company_code)
            validate_sql_identifier(company_code, "company_code")

            # Delete por janela — apaga a partir do início do range
            delete_sql = f"""
                DELETE FROM {target_table}
                WHERE companycode = '{company_code}'
                  AND DATE(accountingreleasedate) >= '{window_start}'
                  AND DATE(accountingreleasedate) <= '{end_date.strftime("%Y-%m-%d")}'
            """
            logger.info(
                "Limpando janela | Empresa: %s | %s até %s",
                company_code, window_start, end_date.strftime("%Y-%m-%d"),
            )

            logger.info("Query de exclusão: %s", delete_sql)
            spark.sql(delete_sql)
            delete_bq_window(
                bq_client=extractor.bq_client,
                bq_table=bq_table,
                where_clause=(
                    f"companycode = {company_code}"  # INT64 — sem aspas
                    f" AND DATE(accountingreleasedate) >= '{window_start}'"
                    f" AND DATE(accountingreleasedate) <= '{end_date.strftime('%Y-%m-%d')}'"
                ),
            )

            # Insert — percorre mês a mês dentro do range
            for start_of_month in months_to_process:
                last_day_of_month = start_of_month.replace(
                    day=calendar.monthrange(start_of_month.year, start_of_month.month)[1]
                )
                end_of_month = min(last_day_of_month, end_date)
                accounting_date_param = start_of_month.strftime("%Y-%m-%d")
                current_date = max(start_of_month, start_date)

                logger.info(
                    "Empresa: %s | Mês: %s | Janela: %s até %s",
                    company_code,
                    start_of_month.strftime("%Y-%m"),
                    current_date.strftime("%Y-%m-%d"),
                    end_of_month.strftime("%Y-%m-%d"),
                )

                while current_date <= end_of_month:
                    release_date_param = current_date.strftime("%Y-%m-%d")
                    query_params = {
                        "companyCode": company_code,
                        "accountingDate": accounting_date_param,
                        "accountingReleaseDate": release_date_param,
                        "pageSize": page_size,
                    }
                    try:
                        logger.info(
                            "Empresa: %s | AccDate: %s | RelDate: %s",
                            company_code, accounting_date_param, release_date_param,
                        )
                        extractor.run_extraction(
                            table_name=table_extract,
                            params=query_params,
                        )
                    except Exception:
                        logger.exception("Erro no dia %s. Continuando...", release_date_param)

                    current_date += timedelta(days=1)

            logger.info("Empresa %s concluída.", company_code)

        resultado = "Sucesso"  # fora do for — só "Sucesso" se TODAS as empresas passarem
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        row_count = extractor._write_snapshot(table_extract)
        extractor._log_ingestion_result(table_extract, row_count, resultado, error_message)

    logger.info("Ingestão accounting_facts finalizada com sucesso.")


if __name__ == "__main__":
    main()
