"""
Job: lancamento_contabil — ingestão incremental por janela de guias.

Agendado no Databricks como task independente.
Aponta para: jobs/matera/run_lancamento_contabil.py
"""

import datetime as pydt

from pyspark.sql import SparkSession

from pipelines.finance_data_pipelines.core import (
    delete_bq_window,
    get_logger,
    validate_sql_identifier,
)
from pipelines.finance_data_pipelines.sources.matera.config.config import MATERA_CONFIG
from pipelines.finance_data_pipelines.sources.matera.extractor.extractor import ExtractorMatera

logger = get_logger(__name__)


def main() -> None:
    # Resolvido dentro de main() — evita NoneType se o módulo for importado
    # antes da SparkSession existir (ex: testes, imports antecipados).
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "SparkSession não encontrada. "
            "Este job deve ser executado dentro do Databricks."
        )

    table_extract  = "lancamento_contabil"
    table_config   = MATERA_CONFIG["tables"][table_extract]
    target_table   = table_config["databricks_table_target"]
    bq_table       = table_config["bigquery_table_target"]
    guia_lookback  = table_config["guia_lookback"]
    guia_lookahead = table_config["guia_lookahead"]

    dl_export_date = pydt.datetime.today()
    ano_alvo = str(dl_export_date.year)

    extractor = ExtractorMatera(MATERA_CONFIG, dl_export_date)

    logger.info("Calculando janela de processamento para o ano %s...", ano_alvo)

    query_stats = f"""
        SELECT idempresa, MAX(numguia) AS max_guia
        FROM {target_table}
        WHERE ano = '{ano_alvo}'
        GROUP BY idempresa
    """

    try:
        stats_rows = spark.sql(query_stats).collect()
    except Exception:
        logger.exception("Erro ao consultar janela de processamento.")
        raise

    if not stats_rows:
        logger.warning(
            "Nenhum registro encontrado para %s. Verifique se a carga inicial foi realizada.",
            ano_alvo,
        )
        return

    resultado     = "Falha"
    error_message = None
    try:
        for row in stats_rows:
            id_emp = str(row["idempresa"])
            validate_sql_identifier(id_emp, "idempresa")
            max_atual = int(row["max_guia"])

            start_guia = max(1, max_atual - guia_lookback)
            end_guia   = start_guia + guia_lookahead

            logger.info(
                "Empresa: %s | max_guia atual: %d | janela: %d → %d",
                id_emp, max_atual, start_guia, end_guia,
            )

            delete_sql = f"""
                DELETE FROM {target_table}
                WHERE idempresa = '{id_emp}'
                  AND ano = '{ano_alvo}'
                  AND numguia >= {start_guia}
            """
            logger.info("Limpando janela de re-processamento (guia >= %d).", start_guia)
            spark.sql(delete_sql)
            delete_bq_window(
                bq_client=extractor.bq_client,
                bq_table=bq_table,
                where_clause=(
                    f"idempresa = '{id_emp}'"
                    f" AND ano = '{ano_alvo}'"
                    f" AND numguia >= {start_guia}"
                ),
            )

            lista_guias = list(range(start_guia, end_guia + 1))

            extractor.run_extraction(
                table_name=table_extract,
                guide_list=lista_guias,
                year=ano_alvo,
                id_empresa=id_emp,
            )

        resultado = "Sucesso"
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        row_count = extractor._write_snapshot(table_extract)
        extractor._log_ingestion_result(table_extract, row_count, resultado, error_message)

    logger.info("Ingestão de lancamento_contabil finalizada.")


if __name__ == "__main__":
    main()
