"""
Copia tabelas do BigQuery para o Databricks Delta Lake.

Configuração: transfers/config.py — adicione novas tabelas na lista TABLES.
Execução:     Databricks job apontando para este script (sem parâmetros).
"""

import logging
import sys

from pyspark.sql import SparkSession

from pipelines.finance_data_pipelines.transfers.config import TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _sync_table(
    spark: SparkSession,
    bq_table: str,
    databricks_table: str,
    mode: str,
) -> int:
    # bq_table format: "project.dataset.table"
    project, dataset_table = bq_table.split(".", 1)

    logger.info(f"Lendo {bq_table} do BigQuery...")
    df = (
        spark.read.format("bigquery")
        .option("table", dataset_table)
        .option("project", project)
        .load()
    )

    row_count = df.count()
    logger.info(f"Escrevendo {row_count} linhas em {databricks_table} (mode={mode})...")

    schema = ".".join(databricks_table.split(".")[:-1])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    df.write.mode(mode).option("overwriteSchema", "true").saveAsTable(databricks_table)

    logger.info(f"Concluído: {databricks_table} — {row_count} linhas")
    return row_count


def main() -> None:
    spark = SparkSession.getActiveSession()

    results = []
    for table in TABLES:
        bq_table = table["bq_table"]
        databricks_table = table["databricks_table"]
        mode = table.get("mode", "overwrite")
        try:
            row_count = _sync_table(spark, bq_table, databricks_table, mode)
            results.append({"table": databricks_table, "rows": row_count, "status": "OK"})
        except Exception:
            logger.exception(f"Falha ao sincronizar {bq_table}")
            results.append({"table": databricks_table, "rows": 0, "status": "FAILED"})

    logger.info("=" * 60)
    for r in results:
        logger.info(f"  {r['status']:6} | {r['rows']:>8} linhas | {r['table']}")
    logger.info("=" * 60)

    if any(r["status"] == "FAILED" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
