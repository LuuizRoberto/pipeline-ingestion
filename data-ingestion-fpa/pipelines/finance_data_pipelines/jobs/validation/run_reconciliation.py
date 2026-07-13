"""
Job: reconciliação Databricks vs BigQuery.

Roda após todas as ingestões do dia. Gera um SQL UNION ALL para cada destino
(BigQuery e Databricks), executa os dois, junta os resultados e compara.

Resultado final — uma linha por métrica:
    tabela | metrica | databricks | bigquery | resultado | diferenca

Execução no Databricks:
    Adicionar como task após os jobs de ingestão no mesmo pipeline.
    Aponta para: jobs/validation/run_reconciliation.py
"""

import os
from datetime import date

import pyspark.sql.functions as F
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pipelines.finance_data_pipelines.core import create_bq_client, get_logger
from pipelines.finance_data_pipelines.sources.jira.config.config import JIRA_CONFIG
from pipelines.finance_data_pipelines.sources.matera.config.config import MATERA_CONFIG

logger = get_logger(__name__)

SOURCES = [MATERA_CONFIG, JIRA_CONFIG]

BQ_ROW_SCHEMA = StructType([
    StructField("tabela",   StringType(), False),
    StructField("metrica",  StringType(), False),
    StructField("bigquery", DoubleType(),  True),
])


# ------------------------------------------------------------------
# Geradores de SQL
# ------------------------------------------------------------------

def _build_bq_sql(sources: list) -> str:
    """Gera SQL UNION ALL para consultar COUNT e SUM de todas as tabelas no BigQuery.

    Cada tabela contribui com uma linha de 'quantidade' e uma por value_column.
    Usa FLOAT64 para tipos consistentes no UNION.

    Returns:
        String SQL pronta para execução no BigQuery.
    """
    selects = []
    for config in sources:
        for _, tc in config["tables"].items():
            bq = tc.get("bigquery_table_target")
            db = tc.get("databricks_table_target")
            if not bq or not db:
                continue

            selects.append(
                f"SELECT\n"
                f"    '{db}'        AS tabela,\n"
                f"    'quantidade'   AS metrica,\n"
                f"    CAST(COUNT(*) AS FLOAT64) AS total\n"
                f"FROM `{bq}`"
            )
            for col in tc.get("value_columns", []):
                selects.append(
                    f"SELECT\n"
                    f"    '{db}',\n"
                    f"    'valor({col})',\n"
                    f"    CAST(ROUND(COALESCE(SUM({col}), 0), 2) AS FLOAT64)\n"
                    f"FROM `{bq}`"
                )

    return "\nUNION ALL\n".join(selects)


def _build_databricks_sql(sources: list) -> str:
    """Gera SQL UNION ALL para consultar COUNT e SUM de todas as tabelas no Databricks.

    Usa DOUBLE para tipos consistentes no UNION e compatibilidade com o join final.
    ROUND(..., 2) alinha a precisão com o BigQuery — elimina acumulação de ponto
    flutuante em grandes agregações sem esconder divergências reais.

    Returns:
        String SQL pronta para execução via spark.sql().
    """
    selects = []
    for config in sources:
        for _, tc in config["tables"].items():
            db = tc.get("databricks_table_target")
            bq = tc.get("bigquery_table_target")
            if not db or not bq:
                continue

            selects.append(
                f"SELECT\n"
                f"    '{db}'        AS tabela,\n"
                f"    'quantidade'   AS metrica,\n"
                f"    CAST(COUNT(*) AS DOUBLE) AS total\n"
                f"FROM {db}"
            )
            for col in tc.get("value_columns", []):
                selects.append(
                    f"SELECT\n"
                    f"    '{db}',\n"
                    f"    'valor({col})',\n"
                    f"    CAST(ROUND(COALESCE(SUM({col}), 0), 2) AS DOUBLE)\n"
                    f"FROM {db}"
                )

    return "\nUNION ALL\n".join(selects)


# ------------------------------------------------------------------
# Persistência
# ------------------------------------------------------------------

_RECONCILIATION_SCHEMA = [
    bigquery.SchemaField("table",       "STRING"),
    bigquery.SchemaField("metric",      "STRING"),
    bigquery.SchemaField("databricks",  "FLOAT64"),
    bigquery.SchemaField("bigquery",    "FLOAT64"),
    bigquery.SchemaField("result",      "STRING"),
    bigquery.SchemaField("difference",  "FLOAT64"),
    bigquery.SchemaField("export_date", "DATE"),
]


def _save_to_bq(bq_client, result_df, bq_project: str, bq_dataset: str) -> None:
    target_table = f"{bq_project}.{bq_dataset}.avenue_matera_api_reconciliation_results"
    hoje = date.today().strftime("%Y-%m-%d")
    rows = [
        {
            "table":       row.tabela,
            "metric":      row.metrica,
            "databricks":  row.databricks,
            "bigquery":    row.bigquery,
            "result":      row.resultado,
            "difference":  row.diferenca,
            "export_date": hoje,
        }
        for row in result_df.collect()
    ]
    try:
        bq_client.query(
            f"DELETE FROM `{target_table}` WHERE export_date = '{hoje}'"
        ).result()
    except Exception:
        pass  # tabela ainda não existe na primeira execução

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=_RECONCILIATION_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    bq_client.load_table_from_json(rows, target_table, job_config=job_config).result()
    logger.info("Reconciliação salva no BigQuery: %s (%d linhas)", target_table, len(rows))


# ------------------------------------------------------------------
# Execução e join
# ------------------------------------------------------------------

def main():
    spark = SparkSession.getActiveSession()
    if spark is None:
        from databricks.connect import DatabricksSession
        from dotenv import load_dotenv
        load_dotenv()
        spark = (
            DatabricksSession.builder
            .host(os.environ["DATABRICKS_HOST"])
            .token(os.environ["DATABRICKS_TOKEN"])
            .clusterId(os.environ["DATABRICKS_CLUSTER_ID"])
            .getOrCreate()
        )

    bq_client = create_bq_client()

    bq_sql = _build_bq_sql(SOURCES)
    db_sql = _build_databricks_sql(SOURCES)

    logger.info("SQL BigQuery:\n%s", bq_sql)
    logger.info("SQL Databricks:\n%s", db_sql)

    # Executa SQL no BigQuery → coleta linhas → Spark DataFrame
    bq_rows = [
        {
            "tabela":   row.tabela,
            "metrica":  row.metrica,
            "bigquery": float(row.total) if row.total is not None else None,
        }
        for row in bq_client.query(bq_sql).result()
    ]
    bq_df = spark.createDataFrame(bq_rows, schema=BQ_ROW_SCHEMA)

    # Executa SQL no Databricks → Spark DataFrame direto
    db_df = spark.sql(db_sql).withColumnRenamed("total", "databricks")

    # Junta os dois DFs e calcula resultado + diferenca
    # Tolerância relativa: diferenca / max(databricks, bigquery) < 0.0001 (0.01%)
    # Evita falsos WARNING por imprecisão de ponto flutuante em valores grandes
    # Ex: diff=0.03 sobre 2.1 trilhões → 1.4e-14 de diferença relativa → OK
    result_df = (
        db_df
        .join(bq_df, on=["tabela", "metrica"], how="full")
        .withColumn(
            "diferenca",
            F.abs(F.col("databricks") - F.col("bigquery")),
        )
        .withColumn(
            "resultado",
            F.when(
                F.col("databricks").isNull() | F.col("bigquery").isNull(),
                "WARNING",
            ).when(
                # tolerância relativa: 0.01% do maior valor entre os dois
                F.col("diferenca") / F.greatest(
                    F.abs(F.col("databricks")),
                    F.abs(F.col("bigquery")),
                    F.lit(1.0),
                ) < 0.0001,
                "OK",
            ).otherwise("WARNING"),
        )
        .select("tabela", "metrica", "databricks", "bigquery", "resultado", "diferenca")
        .orderBy("tabela", "metrica")
    )

    warnings = result_df.filter(F.col("resultado") == "WARNING").count()
    total    = result_df.count()
    logger.info(
        "Reconciliação concluída: %d métricas verificadas | %d com WARNING.",
        total, warnings,
    )

    _save_to_bq(
        bq_client,
        result_df,
        bq_project=MATERA_CONFIG["bq_project"],
        bq_dataset=MATERA_CONFIG["bq_dataset"],
    )

    return result_df


if __name__ == "__main__":
    df = main()
    df.show(truncate=False)
