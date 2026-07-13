"""
Job: saldo_contabil — ingestão diária com delete por janela + insert.
 
Estratégia padrão (sem parâmetros):
    - Reprocessa os 2 meses anteriores ao dia atual
 
Estratégia de reprocessamento (com parâmetros via task parameters):
    - Formato: ["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
 
Agendado no Databricks como task independente.
Aponta para: jobs/matera/run_saldo_contabil.py
"""
 
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


def _get_periodos(start_date: pydt.datetime, end_date: pydt.datetime) -> list:
    """Retorna lista de períodos (primeiro dia de cada mês) dentro do range."""
    periodos = []
    current  = start_date.replace(day=1)
    while current <= end_date:
        periodos.append(current.strftime("%Y-%m-%d"))
        current = current + relativedelta(months=1)
    return periodos
 
 
def main() -> None:
    # Resolvido dentro de main() — evita NoneType se o módulo for importado
    # antes da SparkSession existir (ex: testes, imports antecipados).
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "SparkSession não encontrada. "
            "Este job deve ser executado dentro do Databricks."
        )

    table_extract  = "saldo_contabil"
    table_config   = MATERA_CONFIG["tables"][table_extract]
    target_table   = table_config["databricks_table_target"]
    bq_table       = table_config["bigquery_table_target"]
    company_codes  = table_config["company_code"]
    dl_export_date = pydt.datetime.today()
 
    # ------------------------------------------------------------------
    # Lê parâmetros via task parameters
    # Formato: ["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
    # Se não informados, roda no modo diário padrão (2 meses)
    # ------------------------------------------------------------------
    reprocess_start, reprocess_end = parse_reprocess_args()
 
    if reprocess_start and reprocess_end:
        start_date = pydt.datetime.strptime(reprocess_start, "%Y-%m-%d")
        end_date   = pydt.datetime.strptime(reprocess_end, "%Y-%m-%d")
        logger.info("Modo REPROCESSAMENTO | start: %s | end: %s", reprocess_start, reprocess_end)
    else:
        refdate    = dl_export_date - timedelta(days=1)
        start_date = refdate.replace(day=1) - relativedelta(months=1)
        end_date   = refdate
        logger.info("Modo DIÁRIO | start: %s | end: %s",
                    start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
 
    periodos     = _get_periodos(start_date, end_date)
    window_start = start_date.strftime("%Y-%m-%d")
    window_end   = end_date.strftime("%Y-%m-%d")
 
    extractor = ExtractorMatera(MATERA_CONFIG, export_date=dl_export_date)

    resultado     = "Falha"
    error_message = None
    try:
        for id_empresa in company_codes:
            logger.info("Processando empresa: %s", id_empresa)
            validate_sql_identifier(id_empresa, "id_empresa")

            # Delete por janela da empresa
            if spark.catalog.tableExists(target_table):
                delete_sql = f"""
                    DELETE FROM {target_table}
                    WHERE idempresa = '{id_empresa}'
                      AND dtperiodo >= '{window_start}'
                      AND dtperiodo <= '{window_end}'
                """
                logger.info(
                    "Limpando janela | empresa=%s | %s até %s", id_empresa, window_start, window_end
                )
                spark.sql(delete_sql)
                delete_bq_window(
                    bq_client=extractor.bq_client,
                    bq_table=bq_table,
                    where_clause=(
                        f"idempresa = '{id_empresa}'"
                        f" AND dtperiodo >= '{window_start}'"
                        f" AND dtperiodo <= '{window_end}'"
                    ),
                )
            else:
                logger.info("Tabela %s ainda não existe — pulando delete.", target_table)

            # Loop de períodos no job — extractor recebe um período por vez
            for dt_periodo in periodos:
                logger.info("Empresa: %s | Período: %s", id_empresa, dt_periodo)
                extractor.run_extraction(
                    table_name="saldo_contabil",
                    params={
                        "id_empresa": id_empresa,
                        "dt_periodo": dt_periodo,
                    },
                )

            logger.info("Empresa %s concluída.", id_empresa)

        resultado = "Sucesso"
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        row_count = extractor._write_snapshot(table_extract)
        extractor._log_ingestion_result(table_extract, row_count, resultado, error_message)

    logger.info("Ingestão saldo_contabil finalizada com sucesso.")
 
 
if __name__ == "__main__":
    main()