"""
Testes unitários para ExtractorMatera.
Não requerem Spark nem conexão com a API — usam mocks.
"""

import copy
import datetime
import json
from unittest.mock import MagicMock, mock_open, patch

import pytest

from pipelines.finance_data_pipelines.sources.matera.config.config import MATERA_CONFIG
from pipelines.finance_data_pipelines.tests.fixtures.matera_responses import (
    ACCOUNTING_FACTS_PAGE_1,
    ACCOUNTING_FACTS_PAGE_2,
    LANCAMENTO_CONTABIL_RESPONSE,
)


@pytest.fixture
def mock_extractor():
    """Cria uma instância de ExtractorMatera com dependências externas mockadas."""
    with (
        patch("pipelines.finance_data_pipelines.sources.matera.extractor.extractor.secretmanager"),
        patch("pipelines.finance_data_pipelines.sources.matera.extractor.extractor.ExtractorMatera._get_secret", return_value="fake"),
        patch("pipelines.finance_data_pipelines.sources.matera.extractor.extractor.ExtractorMatera._get_token", return_value="fake-token"),
        patch("pipelines.finance_data_pipelines.core.base_extractor.SparkSession"),
        patch("pipelines.finance_data_pipelines.core.base_extractor.DeltaTable"),
    ):
        from pipelines.finance_data_pipelines.sources.matera.extractor.extractor import (
            ExtractorMatera,
        )

        extractor = ExtractorMatera.__new__(ExtractorMatera)
        extractor.config = MATERA_CONFIG
        extractor.base_host = MATERA_CONFIG["sec"]["host"].rstrip("/")
        extractor.project_id = MATERA_CONFIG["project_id"]
        extractor.headers = {"Authorization": "Bearer fake-token"}
        extractor.session = MagicMock()
        extractor.export_date = datetime.datetime.today()
        extractor.spark = MagicMock()
        return extractor


# ------------------------------------------------------------------
# _navigate_json
# ------------------------------------------------------------------

class TestNavigateJson:
    def test_caminho_simples(self, mock_extractor):
        data = {"data": [{"id": 1}, {"id": 2}]}
        result = mock_extractor._navigate_json(data, "data")
        assert result == [{"id": 1}, {"id": 2}]

    def test_caminho_aninhado(self, mock_extractor):
        data = {"data": {"items": [{"id": 1}]}}
        result = mock_extractor._navigate_json(data, "data.items")
        assert result == [{"id": 1}]

    def test_caminho_invalido_retorna_lista_vazia(self, mock_extractor):
        data = {"data": []}
        result = mock_extractor._navigate_json(data, "data.items.inexistente")
        assert result == []

    def test_valor_escalar_vira_lista(self, mock_extractor):
        data = {"data": "valor_unico"}
        result = mock_extractor._navigate_json(data, "data")
        assert result == ["valor_unico"]


# ------------------------------------------------------------------
# normalize_data
# ------------------------------------------------------------------

class TestNormalizeData:
    def test_colunas_para_minusculo(self, mock_extractor):
        raw = [{"CompanyCode": "1", "Amount": 100.0}]
        result = mock_extractor.normalize_data(raw)
        assert "companycode" in result[0]
        assert "amount" in result[0]

    def test_tipos_complexos_viram_json_string(self, mock_extractor):
        raw = [{"details": {"a": 1}, "tags": [1, 2]}]
        result = mock_extractor.normalize_data(raw)
        assert isinstance(result[0]["details"], str)
        assert json.loads(result[0]["details"]) == {"a": 1}

    def test_valores_simples_preservados(self, mock_extractor):
        raw = [{"codigo": "1001", "valor": 150.0, "ativo": True}]
        result = mock_extractor.normalize_data(raw)
        assert result[0]["codigo"] == "1001"
        assert result[0]["valor"] == 150.0
        assert result[0]["ativo"] is True


# ------------------------------------------------------------------
# _fetch_accounting_facts — usa fixtures de matera_responses.py
# ------------------------------------------------------------------

class TestFetchAccountingFacts:
    def test_paginacao_para_na_ultima_pagina(self, mock_extractor):
        """O while encerra quando last=True na resposta."""
        mock_extractor.session.get.return_value.json.side_effect = [
            ACCOUNTING_FACTS_PAGE_1,
            ACCOUNTING_FACTS_PAGE_2,
        ]
        mock_extractor.session.get.return_value.raise_for_status = MagicMock()

        result = mock_extractor._fetch_accounting_facts(
            table_config=MATERA_CONFIG["tables"]["accounting_facts"],
            params={},
        )

        assert len(result) == 2
        assert mock_extractor.session.get.call_count == 2

    def test_registros_das_duas_paginas_sao_acumulados(self, mock_extractor):
        """Registros de todas as páginas chegam no resultado final."""
        mock_extractor.session.get.return_value.json.side_effect = [
            ACCOUNTING_FACTS_PAGE_1,
            ACCOUNTING_FACTS_PAGE_2,
        ]
        mock_extractor.session.get.return_value.raise_for_status = MagicMock()

        result = mock_extractor._fetch_accounting_facts(
            table_config=MATERA_CONFIG["tables"]["accounting_facts"],
            params={},
        )

        release_numbers = [r["releasenumber"] for r in result]
        assert 1001 in release_numbers
        assert 1002 in release_numbers


# ------------------------------------------------------------------
# _fetch_lancamento_contabil
# ------------------------------------------------------------------

class TestFetchLancamentoContabil:
    def test_acumula_registros_por_guia(self, mock_extractor):
        """Cada guia válida contribui com seus registros para o total."""
        # deepcopy garante objetos independentes por guia — em produção cada
        # chamada HTTP retorna uma resposta nova; o mock reutilizaria o mesmo objeto.
        mock_extractor.session.get.return_value.json.side_effect = [
            copy.deepcopy(LANCAMENTO_CONTABIL_RESPONSE),
            copy.deepcopy(LANCAMENTO_CONTABIL_RESPONSE),
        ]
        mock_extractor.session.get.return_value.raise_for_status = MagicMock()

        result = mock_extractor._fetch_lancamento_contabil(
            table_config=MATERA_CONFIG["tables"]["lancamento_contabil"],
            guide_list=[1, 2],
            year="2024",
            id_empresa="1",
        )

        assert len(result) == 2  # 1 registro por guia × 2 guias
        assert result[0]["ano"] == "2024"
        assert result[0]["idempresa"] == "1"
        assert result[0]["numguia"] == 1
        assert result[1]["numguia"] == 2

    def test_erro_em_guia_nao_interrompe_coleta(self, mock_extractor):
        """Falha em uma guia é ignorada — as demais continuam sendo coletadas."""
        response_error = MagicMock()
        response_error.raise_for_status.side_effect = Exception("connection timeout")

        response_ok = MagicMock()
        response_ok.raise_for_status = MagicMock()
        response_ok.json.return_value = LANCAMENTO_CONTABIL_RESPONSE

        mock_extractor.session.get.side_effect = [response_error, response_ok]

        result = mock_extractor._fetch_lancamento_contabil(
            table_config=MATERA_CONFIG["tables"]["lancamento_contabil"],
            guide_list=[1, 2],
            year="2024",
            id_empresa="1",
        )

        assert len(result) == 1  # apenas a guia 2 foi coletada com sucesso


# ------------------------------------------------------------------
# _fetch_saldo_contabil
# ------------------------------------------------------------------

class TestFetchSaldoContabil:
    def test_raises_sem_id_empresa(self, mock_extractor):
        with pytest.raises(ValueError, match="id_empresa"):
            mock_extractor._fetch_saldo_contabil(
                MATERA_CONFIG["tables"]["saldo_contabil"],
                params={},
            )

    def test_raises_empresa_sem_mapeamento_de_ids(self, mock_extractor):
        with pytest.raises(ValueError, match="não tem arquivo de IDs"):
            mock_extractor._fetch_saldo_contabil(
                MATERA_CONFIG["tables"]["saldo_contabil"],
                params={"id_empresa": "999", "dt_periodo": "2024-01-01"},
            )

    def test_resposta_400_ignorada_sem_erro(self, mock_extractor):
        """Contas pai sem saldo retornam 400 — devem ser puladas silenciosamente."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_extractor.session.get.return_value = mock_response

        with patch("builtins.open", mock_open(read_data="1001\n1002\n")):
            with patch("os.path.exists", return_value=True):
                result = mock_extractor._fetch_saldo_contabil(
                    MATERA_CONFIG["tables"]["saldo_contabil"],
                    params={"id_empresa": "1", "dt_periodo": "2024-01-01"},
                )

        assert result == []

    def test_caminho_feliz_retorna_saldo(self, mock_extractor):
        """Coleta o saldo corretamente para empresa e período válidos."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"vlrSaldo": 500.0, "tipoLanc": "C"}}
        mock_extractor.session.get.return_value = mock_response

        with patch("builtins.open", mock_open(read_data="1001\n")):
            with patch("os.path.exists", return_value=True):
                result = mock_extractor._fetch_saldo_contabil(
                    MATERA_CONFIG["tables"]["saldo_contabil"],
                    params={"id_empresa": "1", "dt_periodo": "2024-01-01"},
                )

        assert len(result) == 1
        assert result[0]["vlrsaldo"] == 500.0
        assert result[0]["codcontacontabil"] == "1001"
        assert result[0]["dtperiodo"] == "2024-01-01"
        assert result[0]["idempresa"] == "1"


# ------------------------------------------------------------------
# _load_conta_contabil_codes — união arquivo + BigQuery
# ------------------------------------------------------------------

class TestLoadContaContabilCodes:
    def test_uniao_arquivo_e_bq_sem_duplicar(self, mock_extractor):
        """A lista final é a união (arquivo ∪ BQ), única e ordenada."""
        mock_extractor._read_codes_from_file = MagicMock(return_value=["100", "200"])
        mock_extractor._read_codes_from_bq = MagicMock(return_value=["200", "300"])

        result = mock_extractor._load_conta_contabil_codes(
            MATERA_CONFIG["tables"]["conta_contabil"]
        )

        assert result == ["100", "200", "300"]  # 200 não duplica, ordenado

    def test_degrada_para_arquivo_quando_bq_falha(self, mock_extractor):
        """Falha no BQ → from_bq=[]; a coleta segue apenas com os códigos do arquivo."""
        mock_extractor._read_codes_from_file = MagicMock(return_value=["100", "200"])

        # create_bq_client lançando exceção simula BQ indisponível; _read_codes_from_bq
        # deve engolir o erro e devolver [] (comportamento real, não mockado aqui).
        with patch(
            "pipelines.finance_data_pipelines.sources.matera.extractor.extractor.create_bq_client",
            side_effect=Exception("bq indisponível"),
        ):
            result = mock_extractor._load_conta_contabil_codes(
                MATERA_CONFIG["tables"]["conta_contabil"]
            )

        assert result == ["100", "200"]  # só o arquivo, sem quebrar


# ------------------------------------------------------------------
# get_api_data — dispatch
# ------------------------------------------------------------------

class TestGetApiDataDispatch:
    def test_dispatch_accounting_facts(self, mock_extractor):
        mock_extractor._fetch_accounting_facts = MagicMock(return_value=[])
        mock_extractor.get_api_data("accounting_facts", params={})
        mock_extractor._fetch_accounting_facts.assert_called_once()

    def test_dispatch_conta_contabil(self, mock_extractor):
        mock_extractor._fetch_conta_contabil = MagicMock(return_value=[])
        mock_extractor.get_api_data("conta_contabil")
        mock_extractor._fetch_conta_contabil.assert_called_once()

    def test_dispatch_saldo_contabil(self, mock_extractor):
        mock_extractor._fetch_saldo_contabil = MagicMock(return_value=[])
        mock_extractor.get_api_data("saldo_contabil", params={})
        mock_extractor._fetch_saldo_contabil.assert_called_once()

    def test_dispatch_lancamento_contabil(self, mock_extractor):
        mock_extractor._fetch_lancamento_contabil = MagicMock(return_value=[])
        mock_extractor.get_api_data("lancamento_contabil", guide_list=[], year="2024", id_empresa="1")
        mock_extractor._fetch_lancamento_contabil.assert_called_once()

    def test_tabela_desconhecida_usa_fetch_single_call(self, mock_extractor):
        """Tabelas não mapeadas no dispatch caem em _fetch_single_call."""
        mock_extractor._fetch_single_call = MagicMock(return_value=[])
        mock_extractor.get_api_data("evento_contabil")
        mock_extractor._fetch_single_call.assert_called_once()
