import logging
import re
import sys
from typing import Optional

logger = logging.getLogger(__name__)


def parse_reprocess_args(argv: Optional[list] = None) -> tuple[str, str]:
    """Extrai datas de reprocessamento dos argumentos da task do Databricks.

    Lê argumentos no formato ``key=value`` de ``sys.argv`` (ou de ``argv``
    se fornecido). Retorna as datas de início e fim do reprocessamento,
    ou strings vazias se não informadas — indicando modo diário padrão.

    Args:
        argv: Lista de argumentos a parsear. Se None, usa ``sys.argv[1:]``.
              Útil para testes sem precisar mockar sys.argv.

    Returns:
        Tupla ``(reprocess_start_date, reprocess_end_date)``.
        Ambos são strings no formato ``YYYY-MM-DD``, ou ``""`` se ausentes.

    Example:
        Databricks task parameters::

            ["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]

        Uso no job::

            start, end = parse_reprocess_args()
            if start and end:
                # modo reprocessamento
            else:
                # modo diário padrão
    """
    args_list = argv if argv is not None else sys.argv[1:]
    parsed: dict[str, str] = {}
    for arg in args_list:
        if "=" in arg:
            key, value = arg.split("=", 1)
            parsed[key.strip()] = value.strip()

    return parsed.get("reprocess_start_date", ""), parsed.get("reprocess_end_date", "")


def validate_sql_identifier(value: str, field_name: str) -> str:
    """Valida que um valor é seguro para interpolação em queries SQL.

    Aceita apenas strings numéricas — o formato esperado para company_code
    e id_empresa neste projeto. Rejeita qualquer coisa que não seja dígitos.

    Args:
        value:      Valor a validar (ex: "1", "328").
        field_name: Nome do campo, usado na mensagem de erro.

    Returns:
        O próprio ``value`` se válido.

    Raises:
        ValueError: Se o valor contiver caracteres não numéricos.

    Example:
        >>> validate_sql_identifier("328", "company_code")
        '328'
        >>> validate_sql_identifier("1; DROP TABLE", "company_code")
        ValueError: ...
    """
    if not re.fullmatch(r"\d+", value):
        raise ValueError(
            f"Valor inválido para '{field_name}': '{value}'. "
            "Apenas strings numéricas são aceitas em queries SQL."
        )
    return value


def delete_bq_window(bq_client, bq_table: str, where_clause: str) -> None:
    """Executa DELETE no BigQuery para sincronizar exclusões de janela com o Databricks.

    Chamado nos jobs logo após o DELETE do Databricks, com a mesma condição
    WHERE, para que registros removidos na origem também saiam do BigQuery.

    Best-effort: falhas são logadas como WARNING sem interromper o pipeline.

    Args:
        bq_client:    Instância de ``bigquery.Client`` do extractor (extractor.bq_client).
        bq_table:     Nome completo da tabela BQ (project.dataset.table).
        where_clause: Condição WHERE do DELETE (sem a palavra WHERE),
                      ex: "idempresa = '1' AND numguia >= 750".

    Example:
        >>> delete_bq_window(
        ...     extractor.bq_client,
        ...     table_config["bigquery_table_target"],
        ...     f"companycode = '{company_code}' AND DATE(accountingreleasedate) >= '{start}'",
        ... )
    """
    if not bq_client:
        return
    try:
        from google.api_core.exceptions import NotFound
        bq_client.query(f"DELETE FROM `{bq_table}` WHERE {where_clause}").result()
        logger.info("BQ DELETE concluído em '%s'.", bq_table)
    except NotFound:
        # Tabela ainda não existe na primeira execução — comportamento esperado
        logger.info("BQ DELETE ignorado — tabela '%s' ainda não existe.", bq_table)
    except Exception:
        logger.warning(
            "Falha no BQ DELETE em '%s' — pipeline não afetado.",
            bq_table,
            exc_info=True,
        )
