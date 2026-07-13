"""
Job: jira_issues_providers — ingestão incremental de issues do projeto PROVIDER.

Agendado no Databricks como task independente.
Aponta para: jobs/jira/run_issues_providers.py
"""

import datetime as pydt

from pipelines.finance_data_pipelines.core import get_logger
from pipelines.finance_data_pipelines.sources.jira.config.config import JIRA_CONFIG
from pipelines.finance_data_pipelines.sources.jira.extractor.extractor import ExtractorJira

logger = get_logger(__name__)


def main() -> None:
    table_name = "issues"

    export_date = pydt.datetime.today()

    # Janela de extração baseada no changed_range do config (ex: "3 days")
    # Para reprocessamento completo: date_start = pydt.datetime(2022, 1, 1)
    changed_range_days = int(JIRA_CONFIG["tables"][table_name]["changed_range"].split(" ")[0])
    date_start = export_date - pydt.timedelta(days=changed_range_days)
    end_date = export_date - pydt.timedelta(days=1)

    logger.info("Iniciando ingestão Jira issues | start: %s | end: %s",
                date_start.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    extractor = ExtractorJira(JIRA_CONFIG, export_date=export_date)

    resultado     = "Falha"
    error_message = None
    try:
        extractor.run_extraction(
            table_name=table_name,
            params={"start_date": date_start, "end_date": end_date},
        )
        resultado = "Sucesso"
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        row_count = extractor._write_snapshot(table_name)
        extractor._log_ingestion_result(table_name, row_count, resultado, error_message)

    logger.info("Ingestão Jira issues finalizada com sucesso.")


if __name__ == "__main__":
    main()


