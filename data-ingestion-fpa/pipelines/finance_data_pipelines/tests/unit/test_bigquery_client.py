"""
Testes unitários para core/bigquery_client.py.
Não requerem conexão real com Databricks nem com o BigQuery — usam mocks.
"""

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_databricks_secret(value: str) -> MagicMock:
    """Simula o objeto retornado por w.secrets.get_secret()."""
    secret = MagicMock()
    secret.value = base64.b64encode(value.encode()).decode()
    return secret


FAKE_CREDS_DICT = {"type": "service_account", "project_id": "data-prod-295413"}
FAKE_PROJECT    = "data-prod-295413"


# ------------------------------------------------------------------
# Modo local (Databricks secrets)
# ------------------------------------------------------------------

class TestCreateBqClientModoLocal:

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.bigquery.Client")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.google.auth.load_credentials_from_dict")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client._fetch_credentials_from_databricks")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.getenv")
    def test_usa_databricks_quando_env_vars_presentes(
        self, mock_getenv, mock_fetch, mock_load_creds, mock_bq_client
    ):
        mock_getenv.side_effect = lambda key, *_: (
            "https://fake.azuredatabricks.net" if key == "DATABRICKS_HOST"
            else "dapi-fake-token"             if key == "DATABRICKS_TOKEN"
            else None
        )
        fake_credentials = MagicMock()
        mock_fetch.return_value = (FAKE_CREDS_DICT, FAKE_PROJECT)
        mock_load_creds.return_value = (fake_credentials, None)

        from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client
        create_bq_client()

        mock_fetch.assert_called_once()
        mock_load_creds.assert_called_once_with(FAKE_CREDS_DICT)
        mock_bq_client.assert_called_once_with(
            project=FAKE_PROJECT, credentials=fake_credentials, location="US"
        )

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.bigquery.Client")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.google.auth.load_credentials_from_dict")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client._fetch_credentials_from_databricks")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.getenv")
    def test_project_parametro_tem_precedencia_sobre_secret(
        self, mock_getenv, mock_fetch, mock_load_creds, mock_bq_client
    ):
        """project passado explicitamente sobrescreve o project vindo do secret."""
        mock_getenv.side_effect = lambda key, *_: (
            "https://fake.azuredatabricks.net" if key == "DATABRICKS_HOST"
            else "dapi-fake-token"             if key == "DATABRICKS_TOKEN"
            else None
        )
        fake_credentials = MagicMock()
        mock_fetch.return_value = (FAKE_CREDS_DICT, "project-do-secret")
        mock_load_creds.return_value = (fake_credentials, None)

        from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client
        create_bq_client(project="project-explicito")

        mock_bq_client.assert_called_once_with(
            project="project-explicito", credentials=fake_credentials, location="US"
        )


# ------------------------------------------------------------------
# Modo produção (ADC)
# ------------------------------------------------------------------

class TestCreateBqClientModoProducao:

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.bigquery.Client")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.getenv")
    def test_usa_adc_quando_env_vars_ausentes(self, mock_getenv, mock_bq_client):
        mock_getenv.return_value = None

        from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client
        create_bq_client(project=FAKE_PROJECT)

        mock_bq_client.assert_called_once_with(project=FAKE_PROJECT, location="US")

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.bigquery.Client")
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.getenv")
    def test_usa_adc_quando_apenas_host_presente(self, mock_getenv, mock_bq_client):
        """Ambas as vars precisam estar presentes — só HOST não ativa o modo local."""
        mock_getenv.side_effect = lambda key, *_: (
            "https://fake.azuredatabricks.net" if key == "DATABRICKS_HOST" else None
        )

        from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client
        create_bq_client(project=FAKE_PROJECT)

        mock_bq_client.assert_called_once_with(project=FAKE_PROJECT, location="US")


# ------------------------------------------------------------------
# _fetch_credentials_from_databricks
# ------------------------------------------------------------------

class TestFetchCredentialsFromDatabricks:

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.environ", {
        "DATABRICKS_HOST": "https://fake.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi-fake-token",
    })
    def test_retorna_creds_dict_e_project(self):
        mock_workspace = MagicMock()
        mock_workspace.secrets.get_secret.side_effect = [
            _make_databricks_secret(json.dumps(FAKE_CREDS_DICT)),
            _make_databricks_secret(FAKE_PROJECT),
        ]

        with patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace):
            from pipelines.finance_data_pipelines.core.bigquery_client import (
                _fetch_credentials_from_databricks,
            )
            creds_dict, project = _fetch_credentials_from_databricks()

        assert creds_dict == FAKE_CREDS_DICT
        assert project == FAKE_PROJECT

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.environ", {
        "DATABRICKS_HOST": "https://fake.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi-fake-token",
    })
    def test_propaga_excecao_quando_secret_falha(self):
        mock_workspace = MagicMock()
        mock_workspace.secrets.get_secret.side_effect = Exception("secret not found")

        with patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace):
            from pipelines.finance_data_pipelines.core.bigquery_client import (
                _fetch_credentials_from_databricks,
            )
            with pytest.raises(Exception, match="secret not found"):
                _fetch_credentials_from_databricks()


# ------------------------------------------------------------------
# Tratamento de erros
# ------------------------------------------------------------------

class TestCreateBqClientErros:

    @patch("pipelines.finance_data_pipelines.core.bigquery_client.bigquery.Client", side_effect=Exception("auth failed"))
    @patch("pipelines.finance_data_pipelines.core.bigquery_client.os.getenv", return_value=None)
    def test_propaga_excecao_em_falha_de_autenticacao(self, mock_getenv, mock_bq_client):
        from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client
        with pytest.raises(Exception, match="auth failed"):
            create_bq_client(project=FAKE_PROJECT)
