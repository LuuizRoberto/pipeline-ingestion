"""
Testes unitários para core/job_utils.py.
Não requerem Spark nem conexão com APIs externas.
"""

import pytest

from pipelines.finance_data_pipelines.core.job_utils import (
    parse_reprocess_args,
    validate_sql_identifier,
)

# ------------------------------------------------------------------
# parse_reprocess_args
# ------------------------------------------------------------------

class TestParseReprocessArgs:
    def test_retorna_datas_quando_informadas(self):
        args = ["reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
        start, end = parse_reprocess_args(args)
        assert start == "2025-01-01"
        assert end == "2025-12-31"

    def test_retorna_strings_vazias_quando_ausentes(self):
        start, end = parse_reprocess_args([])
        assert start == ""
        assert end == ""

    def test_ignora_args_sem_sinal_de_igual(self):
        args = ["sem_igual", "reprocess_start_date=2025-01-01", "reprocess_end_date=2025-12-31"]
        start, end = parse_reprocess_args(args)
        assert start == "2025-01-01"
        assert end == "2025-12-31"

    def test_apenas_start_sem_end_retorna_vazios(self):
        """Ambas as datas precisam estar presentes — só uma não ativa o modo reprocessamento."""
        start, end = parse_reprocess_args(["reprocess_start_date=2025-01-01"])
        assert start == "2025-01-01"
        assert end == ""

    def test_espacos_em_torno_de_chave_e_valor_sao_removidos(self):
        args = [" reprocess_start_date = 2025-06-01 ", " reprocess_end_date = 2025-06-30 "]
        start, end = parse_reprocess_args(args)
        assert start == "2025-06-01"
        assert end == "2025-06-30"


# ------------------------------------------------------------------
# validate_sql_identifier
# ------------------------------------------------------------------

class TestValidateSqlIdentifier:
    def test_aceita_string_numerica_simples(self):
        assert validate_sql_identifier("1", "company_code") == "1"

    def test_aceita_numero_com_varios_digitos(self):
        assert validate_sql_identifier("328", "company_code") == "328"

    def test_retorna_o_proprio_valor_quando_valido(self):
        resultado = validate_sql_identifier("339", "id_empresa")
        assert resultado == "339"

    def test_rejeita_string_com_letras(self):
        with pytest.raises(ValueError, match="company_code"):
            validate_sql_identifier("abc", "company_code")

    def test_rejeita_sql_injection(self):
        with pytest.raises(ValueError, match="id_empresa"):
            validate_sql_identifier("1; DROP TABLE users; --", "id_empresa")

    def test_rejeita_string_vazia(self):
        with pytest.raises(ValueError, match="company_code"):
            validate_sql_identifier("", "company_code")

    def test_rejeita_numero_com_ponto(self):
        """Floats como '1.0' não são aceitos — apenas inteiros como string."""
        with pytest.raises(ValueError):
            validate_sql_identifier("1.0", "company_code")

    def test_mensagem_de_erro_inclui_nome_do_campo(self):
        with pytest.raises(ValueError, match="meu_campo"):
            validate_sql_identifier("invalido!", "meu_campo")
