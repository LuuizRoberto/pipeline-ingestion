import logging
import os


def _configure_root_logger() -> None:
    # Guard: só configura uma vez — basicConfig é no-op após o primeiro handler,
    # mas verificar explicitamente evita chamadas repetidas ao SO e ao env.
    if logging.root.handlers:
        return

    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    # Suprime logs verbosos do py4j (Java gateway do PySpark/Databricks).
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Retorna um logger padronizado para uso em qualquer módulo do projeto.

    Args:
        name: Nome do logger, normalmente ``__name__`` do módulo chamador.

    Returns:
        Instância configurada de ``logging.Logger``.
    """
    _configure_root_logger()
    return logging.getLogger(name)
