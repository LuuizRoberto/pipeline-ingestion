"""
Testes unitários para ExtractorJira.
Não requerem Spark nem conexão com a API — usam mocks.
"""

import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType

from pipelines.finance_data_pipelines.sources.jira.config.config import JIRA_CONFIG


@pytest.fixture
def mock_jira_extractor():
    """Cria uma instância de ExtractorJira com dependências externas mockadas."""
    with (
        patch("pipelines.finance_data_pipelines.sources.jira.extractor.extractor.secretmanager"),
        patch("pipelines.finance_data_pipelines.sources.jira.extractor.extractor.ExtractorJira._get_secret", return_value="fake"),
        patch("pipelines.finance_data_pipelines.sources.jira.extractor.extractor.ExtractorJira._create_session"),
        patch("pipelines.finance_data_pipelines.sources.jira.extractor.extractor.ExtractorJira._build_auth_header", return_value={}),
        patch("pipelines.finance_data_pipelines.sources.jira.extractor.extractor.ExtractorJira._fetch_field_metadata", return_value=({}, {})),
        patch("pipelines.finance_data_pipelines.core.base_extractor.SparkSession"),
        patch("pipelines.finance_data_pipelines.core.base_extractor.DeltaTable"),
    ):
        from pipelines.finance_data_pipelines.sources.jira.extractor.extractor import ExtractorJira

        extractor = ExtractorJira.__new__(ExtractorJira)
        extractor.config = JIRA_CONFIG
        extractor.jira_instance = JIRA_CONFIG["instance"]
        extractor.project_id = JIRA_CONFIG["project_id"]
        extractor.sm_client = MagicMock()
        extractor.session = MagicMock()
        extractor.auth_header = {"Authorization": "Basic fake"}
        extractor.field_metadata = {
            "customfield_10200": {"id": "customfield_10200", "name": "reporter"},
            "customfield_10300": {"id": "customfield_10300", "name": "created"},
            "customfield_10400": {"id": "customfield_10400", "name": "assignee"},
        }
        extractor._field_name_to_id = {
            "reporter": "customfield_10200",
            "created":  "customfield_10300",
            "assignee": "customfield_10400",
        }
        extractor.export_date = datetime.datetime.today()
        extractor.spark = MagicMock()
        return extractor


# ------------------------------------------------------------------
# _strip_accents
# ------------------------------------------------------------------

class TestStripAccents:
    def test_remove_acento_agudo(self, mock_jira_extractor):
        assert mock_jira_extractor._strip_accents("Razão") == "Razao"

    def test_remove_cedilha(self, mock_jira_extractor):
        assert mock_jira_extractor._strip_accents("Fornecedor") == "Fornecedor"

    def test_string_sem_acento_nao_muda(self, mock_jira_extractor):
        assert mock_jira_extractor._strip_accents("reporter") == "reporter"

    def test_multiplos_acentos(self, mock_jira_extractor):
        assert mock_jira_extractor._strip_accents("Área Interna") == "Area Interna"


# ------------------------------------------------------------------
# _resolve_field_ids — O(1) via índice reverso
# ------------------------------------------------------------------

class TestResolveFieldIds:
    def test_mapeia_nome_para_id(self, mock_jira_extractor):
        result = mock_jira_extractor._resolve_field_ids(["reporter", "created"])
        assert "customfield_10200" in result
        assert "customfield_10300" in result

    def test_campo_desconhecido_ignorado(self, mock_jira_extractor):
        """Campos não mapeados são silenciosamente descartados."""
        result = mock_jira_extractor._resolve_field_ids(["reporter", "campo_inexistente"])
        assert len(result) == 1
        assert "customfield_10200" in result

    def test_lista_vazia_retorna_lista_vazia(self, mock_jira_extractor):
        assert mock_jira_extractor._resolve_field_ids([]) == []


# ------------------------------------------------------------------
# normalize_data
# ------------------------------------------------------------------

class TestNormalizeDataJira:
    def test_aplana_campos_em_fields(self, mock_jira_extractor):
        """Campos dentro de 'fields' são normalizados e movidos para o nível raiz."""
        raw = [{
            "id": "10001",
            "key": "PRV-1",
            "fields": {
                "customfield_10200": "João",
                "customfield_10300": "2024-01-01",
            },
        }]
        result = mock_jira_extractor.normalize_data(raw)
        assert result[0]["issue_id"] == "10001"
        assert result[0]["issue_key"] == "PRV-1"
        assert result[0]["reporter"] == "João"
        assert result[0]["created"] == "2024-01-01"

    def test_expand_ignorado(self, mock_jira_extractor):
        """O campo 'expand' do Jira é descartado — não tem valor analítico."""
        raw = [{
            "id": "10001",
            "expand": "renderedFields,names,schema",
            "key": "PRV-1",
            "fields": {},
        }]
        result = mock_jira_extractor.normalize_data(raw)
        assert "expand" not in result[0]
        assert "issue_expand" not in result[0]

    def test_campo_desconhecido_ignorado_com_aviso(self, mock_jira_extractor):
        """Campos ausentes no metadado são ignorados sem causar KeyError."""
        raw = [{
            "id": "10001",
            "key": "PRV-1",
            "fields": {"campo_sem_metadado": "valor"},
        }]
        result = mock_jira_extractor.normalize_data(raw)
        assert "campo_sem_metadado" not in result[0]

    def test_tipos_complexos_serializados_como_json(self, mock_jira_extractor):
        """Valores dict/list em fields são serializados como JSON string."""
        raw = [{
            "id": "10001",
            "key": "PRV-1",
            "fields": {
                "customfield_10200": {"displayName": "João", "accountId": "abc123"},
            },
        }]
        result = mock_jira_extractor.normalize_data(raw)
        assert isinstance(result[0]["reporter"], str)
        parsed = json.loads(result[0]["reporter"])
        assert parsed["displayName"] == "João"


# ------------------------------------------------------------------
# _build_dataframe — inferência de schema com union de todos os registros
# ------------------------------------------------------------------

class TestBuildDataframeJira:
    def test_dataframe_vazio_para_records_vazio(self, mock_jira_extractor):
        """Sem issues (ex: feriado), retorna DataFrame com schema vazio."""
        mock_jira_extractor._build_dataframe([])
        mock_jira_extractor.spark.createDataFrame.assert_called_once()
        args = mock_jira_extractor.spark.createDataFrame.call_args[0]
        assert args[0] == []  # lista de registros vazia

    def test_schema_usa_uniao_de_todos_os_registros(self, mock_jira_extractor):
        """Campo presente só em records[1] deve entrar no schema final."""
        records = [
            {"issue_id": "PRV-1", "created": "2024-01-01"},
            {"issue_id": "PRV-2", "assignee": "João"},
        ]
        mock_jira_extractor._build_dataframe(records)

        call_args = mock_jira_extractor.spark.createDataFrame.call_args[0]
        schema = call_args[1]
        field_names = {f.name for f in schema.fields}

        assert "issue_id" in field_names
        assert "created" in field_names
        assert "assignee" in field_names  # estava só em records[1]

    def test_tipos_inferidos_corretamente(self, mock_jira_extractor):
        """Tipos Python são mapeados para os tipos Spark corretos."""
        records = [
            {"texto": "abc", "inteiro": 1, "decimal": 1.5, "booleano": True}
        ]
        mock_jira_extractor._build_dataframe(records)

        schema = mock_jira_extractor.spark.createDataFrame.call_args[0][1]
        tipo_por_campo = {f.name: type(f.dataType) for f in schema.fields}

        assert tipo_por_campo["texto"] == StringType
        assert tipo_por_campo["inteiro"] == IntegerType
        assert tipo_por_campo["decimal"] == DoubleType
        assert tipo_por_campo["booleano"] == BooleanType

    def test_registros_normalizados_com_none_para_campos_ausentes(self, mock_jira_extractor):
        """Registros sem um campo recebem None para garantir consistência no DataFrame."""
        records = [
            {"issue_id": "PRV-1", "assignee": "João"},
            {"issue_id": "PRV-2"},
        ]
        mock_jira_extractor._build_dataframe(records)

        normalized = mock_jira_extractor.spark.createDataFrame.call_args[0][0]
        assert "assignee" in normalized[1]
        assert normalized[1]["assignee"] is None
