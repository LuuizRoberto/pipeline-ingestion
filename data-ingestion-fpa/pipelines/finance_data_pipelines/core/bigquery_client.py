import base64
import json
import os
from typing import Optional

import google.auth
from google.cloud import bigquery

from .logger import get_logger

logger = get_logger(__name__)

_DATABRICKS_SCOPE    = "avenue-secrets-dash-finance"
_DATABRICKS_KEY_CREDS    = "gcp-credentials"
_DATABRICKS_KEY_PROJECT  = "gcp-project-id"


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _fetch_credentials_from_databricks() -> tuple[dict, str]:
    """Busca credenciais GCP e project ID no Databricks Secret Scope.

    Espera as variáveis de ambiente DATABRICKS_HOST e DATABRICKS_TOKEN.
    As chaves buscadas são definidas pelas constantes de módulo acima.

    Returns:
        Tupla (gcp_creds_dict, gcp_project).
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    secret_gcp = w.secrets.get_secret(scope=_DATABRICKS_SCOPE, key=_DATABRICKS_KEY_CREDS)
    gcp_creds_dict = json.loads(base64.b64decode(secret_gcp.value).decode("utf-8"))

    secret_project = w.secrets.get_secret(scope=_DATABRICKS_SCOPE, key=_DATABRICKS_KEY_PROJECT)
    gcp_project = base64.b64decode(secret_project.value).decode("utf-8")

    return gcp_creds_dict, gcp_project


def create_bq_client(project: Optional[str] = None, location: str = "US") -> bigquery.Client:
    """Cria um cliente BigQuery escolhendo o modo de autenticação automaticamente.

    Modos:
        Local  — DATABRICKS_HOST e DATABRICKS_TOKEN presentes no ambiente (via .env):
                 busca as credenciais GCP no Databricks Secret Scope e cria o client
                 com elas. O project ID vem do secret; o parâmetro ``project`` tem
                 precedência se fornecido.

        Produção — variáveis ausentes:
                 usa Application Default Credentials (ADC) do cluster.

    Args:
        project:  ID do projeto GCP. Opcional no modo local (vem do secret);
                  recomendado no modo produção.
        location: Localização padrão para jobs e tabelas. Default: "US".

    Returns:
        Instância autenticada de ``bigquery.Client``.

    Raises:
        Exception: Qualquer falha de autenticação ou inicialização.
    """
    try:
        _load_dotenv()

        if os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"):
            logger.info("Modo local — buscando credenciais GCP no Databricks...")
            gcp_creds_dict, gcp_project = _fetch_credentials_from_databricks()
            credentials, _ = google.auth.load_credentials_from_dict(gcp_creds_dict)
            resolved_project = project or gcp_project
            client = bigquery.Client(
                project=resolved_project, credentials=credentials, location=location
            )
            logger.info(
                "BigQuery client criado (Databricks secrets) | project=%s", resolved_project
            )
        else:
            client = bigquery.Client(project=project, location=location)
            logger.info("BigQuery client criado (ADC) | project=%s", project)

        return client
    except Exception:
        logger.exception("Falha ao criar BigQuery client | project=%s", project)
        raise
