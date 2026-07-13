"""
Job: conta_contabil — carga diária full com merge por cod_conta_contabil.

Estratégia:
    - Lê a lista de IDs de sources/matera/config/conta_contabil_ids.txt
    - Chama o endpoint para cada ID e salva via merge
    - Para adicionar novos IDs: edite o .txt e faça o deploy

Agendado no Databricks como task independente.
Aponta para: jobs/matera/run_conta_contabil.py
"""

import datetime as pydt

from pipelines.finance_data_pipelines.core import get_logger
from pipelines.finance_data_pipelines.sources.matera.config.config import MATERA_CONFIG
from pipelines.finance_data_pipelines.sources.matera.extractor.extractor import ExtractorMatera

logger = get_logger(__name__)


def main() -> None:
    table_extract = "conta_contabil"
    dl_export_date = pydt.datetime.today()

    logger.info("Iniciando ingestão conta_contabil")

    extractor = ExtractorMatera(MATERA_CONFIG, export_date=dl_export_date)

    resultado     = "Falha"
    error_message = None
    try:
        extractor.run_extraction(table_name=table_extract)
        resultado = "Sucesso"
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        row_count = extractor._write_snapshot(table_extract)
        extractor._log_ingestion_result(table_extract, row_count, resultado, error_message)

    logger.info("Ingestão conta_contabil finalizada com sucesso.")


if __name__ == "__main__":
    main()