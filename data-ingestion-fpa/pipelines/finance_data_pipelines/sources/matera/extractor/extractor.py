import json
import os
from datetime import datetime

import requests
from google.cloud import secretmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.finance_data_pipelines.core import BaseExtractor, create_bq_client, get_logger

logger = get_logger(__name__)


class ExtractorMatera(BaseExtractor):
    """Extractor para a API contábil Matera.

    Gerencia autenticação OAuth2, coleta paginada e iterativa para
    diferentes tabelas contábeis, e delega persistência à BaseExtractor.
    """

    # Timeout padrão para todas as chamadas HTTP (segundos)
    _HTTP_TIMEOUT = 60

    # Mapeia table_name → método de coleta.
    # Para adicionar uma nova tabela: inclua uma entrada aqui e implemente o método.
    _FETCH_DISPATCH: dict[str, str] = {
        "lancamento_contabil": "_fetch_lancamento_contabil",
        "accounting_facts":    "_fetch_accounting_facts",
        "conta_contabil":      "_fetch_conta_contabil",
        "saldo_contabil":      "_fetch_saldo_contabil",
    }

    def __init__(self, config: dict, export_date: datetime):
        super().__init__(config, export_date)

        # rstrip("/") evita barra dupla ao concatenar endpoints — o host no config
        # pode vir com "/" no final e o OAuth rejeita "...link//ct/..." com HTTP 400.
        self.base_host = config["sec"]["host"].rstrip("/")
        self.project_id = config["project_id"]

        self.sm_client = secretmanager.SecretManagerServiceClient()
        self.session = self._create_session()

        self.client_id = self._get_secret(config["sec"]["sm_user_secret_id"])
        self.client_secret = self._get_secret(config["sec"]["sm_pwd_secret_id"])
        self.access_token = self._get_token()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Autenticação e sessão
    # ------------------------------------------------------------------

    def _get_secret(self, secret_id: str) -> str:
        """Busca credencial no GCP Secret Manager."""
        resource = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
        return (
            self.sm_client.access_secret_version(request={"name": resource})
            .payload.data.decode("UTF-8")
        )

    def _create_session(self) -> requests.Session:
        """Sessão HTTP com retry exponencial para resiliência."""
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_token(self) -> str:
        """Obtém token OAuth2 via client_credentials."""
        logger.info("Solicitando token OAuth2 da Matera...")
        auth_url = f"{self.base_host}/ct/api/oauth/token"
        response = self.session.post(
            auth_url,
            params={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["access_token"]

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _navigate_json(self, data: dict, path: str) -> list:
        """Navega no JSON usando caminho separado por ponto (ex: 'data.items')."""
        for key in path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                return []
        return data if isinstance(data, list) else [data]

    def _resolve_ids_path(self, ids_file: str) -> str:
        """Resolve o caminho absoluto de um arquivo de IDs.

        Tenta primeiro relativo à raiz do repositório (4 níveis acima deste
        arquivo). Se não encontrar, usa o caminho como está — fallback para
        execução a partir do diretório de trabalho atual.

        Args:
            ids_file: Caminho relativo ao arquivo de IDs (ex: 'sources/matera/config/ids.txt').

        Returns:
            Caminho absoluto resolvido ou o caminho original como fallback.
        """
        repo_root = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(os.path.abspath(__file__))
                )
            )
        )
        ids_path = os.path.join(repo_root, ids_file)
        return ids_path if os.path.exists(ids_path) else ids_file

    # ------------------------------------------------------------------
    # Interface obrigatória (BaseExtractor)
    # ------------------------------------------------------------------

    def get_api_data(self, table_name: str, **kwargs) -> list[dict]:
        """Despacha para o método de coleta correto conforme a tabela.

        Usa _FETCH_DISPATCH para localizar o método e repassa **kwargs
        diretamente. Cada _fetch_* extrai o que precisa internamente.
        Tabelas não mapeadas caem em _fetch_single_call.
        """
        table_config  = self.config["tables"][table_name]
        method_name   = self._FETCH_DISPATCH.get(table_name, "_fetch_single_call")
        fetch_method  = getattr(self, method_name)
        raw_records   = fetch_method(table_config, **kwargs)
        return raw_records

    # ------------------------------------------------------------------
    # Métodos de coleta por tabela
    # ------------------------------------------------------------------

    def _read_codes_from_file(self, ids_file: str) -> list[str]:
        """Lê os códigos de conta do arquivo .txt (ignora vazios e comentários)."""
        ids_path = self._resolve_ids_path(ids_file)
        with open(ids_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]

    def _read_codes_from_bq(self, table: str, column: str) -> list[str]:
        """Busca os códigos distintos de conta que já tiveram lançamento no BigQuery.

        Fonte típica: accounting_facts (todas as contas com movimento). Falhas de
        consulta são logadas e resultam em lista vazia — a coleta continua apenas
        com os códigos do arquivo, sem quebrar o job.
        """
        try:
            client = create_bq_client(project=self.config.get("bq_project"))
            query = f"SELECT DISTINCT `{column}` AS code FROM `{table}` WHERE `{column}` IS NOT NULL"
            rows = client.query(query).result()
            return [str(row.code).strip() for row in rows if row.code is not None]
        except Exception:
            logger.exception(
                "Falha ao buscar códigos em %s. Seguindo apenas com o arquivo.", table
            )
            return []

    def _load_conta_contabil_codes(self, table_config: dict) -> list[str]:
        """Monta a lista final de códigos: arquivo ∪ contas com lançamento (BigQuery).

        Retorna os códigos únicos e ordenados. Se não houver fonte BigQuery
        configurada (codes_source_table), usa somente o arquivo.
        """
        from_file = self._read_codes_from_file(table_config["ids_file"])

        source_table  = table_config.get("codes_source_table")
        source_column = table_config.get("codes_source_column")
        from_bq = (
            self._read_codes_from_bq(source_table, source_column)
            if source_table and source_column else []
        )

        merged = sorted(set(from_file) | set(from_bq))
        logger.info(
            "conta_contabil códigos | arquivo=%d | bigquery=%d | união=%d",
            len(from_file), len(from_bq), len(merged),
        )
        return merged

    def _fetch_conta_contabil(self, table_config: dict, **_) -> list[dict]:
        """Coleta contas contábeis iterando sobre a união de códigos (arquivo + BigQuery).

        A lista final combina os IDs de sources/matera/config/conta_contabil_ids.txt
        com as contas que já tiveram lançamento (distinct de codes_source_column na
        codes_source_table). Para adicionar IDs fixos manualmente: edite o .txt.
        """
        id_plano_contas = table_config["id_plano_contas"]
        endpoint        = table_config["endpoint"]
        data_path       = table_config["data_path"]

        codigos = self._load_conta_contabil_codes(table_config)
        total   = len(codigos)

        logger.info(
            "Iniciando coleta conta_contabil | plano=%s | total=%d contas",
            id_plano_contas, total,
        )

        all_items = []
        for i, cod_conta in enumerate(codigos, start=1):
            url = (
                f"{self.base_host}/"
                f"{endpoint.format(id_plano_contas=id_plano_contas, cod_conta_contabil=cod_conta)}"
            )
            logger.info("conta_contabil | (%d/%d) buscando conta %s", i, total, cod_conta)
            try:
                response = self.session.get(url, headers=self.headers, timeout=self._HTTP_TIMEOUT)
                response.raise_for_status()
                data = self._navigate_json(response.json(), data_path)
                if isinstance(data, dict):
                    data = [data]
                for record in data:
                    record["codContaContabil"] = cod_conta
                all_items.extend(data)
                logger.info("conta_contabil | (%d/%d) conta %s | %d registro(s)", i, total, cod_conta, len(data))
            except Exception:
                logger.exception("Erro na conta %s. Continuando...", cod_conta)

        logger.info("conta_contabil: %d registros coletados.", len(all_items))
        return all_items

    def _fetch_saldo_contabil(self, table_config: dict, **kwargs) -> list[dict]:
        """Coleta saldos contábeis para uma empresa e um período.

        Kwargs:
            params (dict): deve conter 'id_empresa' (str) e 'dt_periodo' (str YYYY-MM-DD).
        """
        params          = kwargs.get("params") or {}
        endpoint        = table_config["endpoint"]
        id_plano_contas = table_config["id_plano_contas"]
        tipo_contabil   = table_config["tipo_contabil"]
        ids_by_empresa  = table_config["ids_file_by_empresa"]

        id_empresa = params.get("id_empresa")
        dt_periodo = params.get("dt_periodo")

        if not id_empresa:
            raise ValueError("params['id_empresa'] é obrigatório para saldo_contabil.")

        ids_file = ids_by_empresa.get(id_empresa)
        if not ids_file:
            raise ValueError(
                f"Empresa '{id_empresa}' não tem arquivo de IDs mapeado em "
                f"ids_file_by_empresa. Empresas configuradas: {sorted(ids_by_empresa)}"
            )

        ids_path = self._resolve_ids_path(ids_file)
        with open(ids_path, "r", encoding="utf-8") as f:
            codigos = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        if not codigos:
            logger.info("Empresa %s sem IDs no arquivo — pulando.", id_empresa)
            return []

        logger.info(
            "Coletando saldo_contabil | empresa=%s | período=%s | contas=%d",
            id_empresa, dt_periodo, len(codigos),
        )

        all_items = []
        for cod_conta in codigos:
            endpoint_path = endpoint.format(
                id_empresa=id_empresa,
                id_plano_contas=id_plano_contas,
                cod_conta=cod_conta,
                dt_periodo=dt_periodo,
                tipo_contabil=tipo_contabil,
            )
            url = f"{self.base_host}/{endpoint_path}"
            try:
                response = self.session.get(url, headers=self.headers, timeout=self._HTTP_TIMEOUT)

                # Contas pai sem saldo retornam 400/404 — pula sem erro
                if response.status_code in (400, 404):
                    continue

                response.raise_for_status()
                data = response.json().get("data", {})

                all_items.append({
                    "idempresa":        id_empresa,
                    "idplanocontas":    id_plano_contas,
                    "codcontacontabil": cod_conta,
                    "dtperiodo":        dt_periodo,
                    "tipocontabil":     tipo_contabil,
                    "vlrsaldo":         data.get("vlrSaldo"),
                    "tipolanc":         data.get("tipoLanc"),
                })

            except Exception:
                logger.exception(
                    "Erro | empresa=%s | conta=%s | período=%s",
                    id_empresa, cod_conta, dt_periodo,
                )

        logger.info(
            "saldo_contabil empresa=%s | período=%s: %d registros.",
            id_empresa, dt_periodo, len(all_items),
        )
        return all_items

    def normalize_data(self, raw_data: list[dict]) -> list[dict]:
        """Normaliza colunas para minúsculo e serializa tipos complexos."""
        result = []
        for record in raw_data:
            clean = {
                col.lower(): json.dumps(val) if isinstance(val, (dict, list)) else val
                for col, val in record.items()
            }
            result.append(clean)
        return result

    def _fetch_single_call(self, table_config: dict, **_) -> list[dict]:
        """Coleta simples — uma única chamada GET sem paginação."""
        url = f"{self.base_host}/{table_config['endpoint'].format(**table_config)}"
        logger.info("Coletando (chamada única): %s", url)
        response = self.session.get(url, headers=self.headers, timeout=self._HTTP_TIMEOUT)
        response.raise_for_status()
        data = self._navigate_json(response.json(), table_config["data_path"])
        logger.info("Total de registros coletados: %d", len(data))
        return data

    def _fetch_accounting_facts(self, table_config: dict, **kwargs) -> list[dict]:
        """Coleta paginada com while — accounting_facts.

        Itera páginas enquanto pagination.last for False na resposta.
        Usa uma cópia local de params para controle de paginação —
        o dict original do caller nunca é modificado (sem side effects).

        Args:
            table_config: Subconjunto de MATERA_CONFIG['tables']['accounting_facts'].
            params: Parâmetros de query (companyCode, accountingDate, pageSize, etc.).

        Returns:
            Lista acumulada de todos os registros de todas as páginas.
        """
        params    = kwargs.get("params") or {}
        url       = f"{self.base_host}/{table_config['endpoint'].format(**table_config)}"
        data_path = table_config["data_path"]
        all_items: list[dict] = []
        is_last_page = False

        # Cópia local — garante que o dict do caller não seja mutado
        page_params = {**params, "page": params.get("page", 0)}

        while not is_last_page:
            logger.info("Coletando accounting_facts | Página atual: %d", page_params["page"])

            response = self.session.get(
                url, headers=self.headers, params=page_params, timeout=self._HTTP_TIMEOUT
            )
            response.raise_for_status()

            json_response = response.json()
            data = self._navigate_json(json_response, data_path)

            logger.info("Registros na página %d: %d", page_params["page"], len(data))
            all_items.extend(data)

            is_last_page = json_response.get("data", {}).get("pagination", {}).get("last", True)
            page_params["page"] += 1

        logger.info("accounting_facts: %d registros coletados.", len(all_items))
        return all_items

    def _fetch_lancamento_contabil(self, table_config: dict, **kwargs) -> list[dict]:
        """Coleta lançamentos contábeis iterando sobre uma lista de guias."""
        guide_list = kwargs.get("guide_list")
        year       = kwargs.get("year")
        id_empresa = kwargs.get("id_empresa")
        endpoint   = table_config["endpoint"]
        data_path  = table_config["data_path"]
        all_items: list[dict] = []

        logger.info(
            "Iniciando extração lancamento_contabil | empresa=%s | ano=%s | guias=%d",
            id_empresa, year, len(guide_list),
        )

        for guide_id in guide_list:
            url_ctx = {**table_config, "ano": year, "num_guia": guide_id, "id_empresa": id_empresa}
            url = f"{self.base_host}/{endpoint.format(**url_ctx)}"
            try:
                response = self.session.get(url, headers=self.headers, timeout=self._HTTP_TIMEOUT)
                response.raise_for_status()
                data = self._navigate_json(response.json(), data_path)
                for record in data:
                    record["ano"] = str(year)
                    record["idempresa"] = str(id_empresa)
                    record["numguia"] = int(guide_id)
                all_items.extend(data)
                logger.info("guia=%s | registros=%d", guide_id, len(data))
            except Exception:
                logger.exception("Falha na guia %s. Continuando...", guide_id)

        logger.info("lancamento_contabil: %d registros coletados.", len(all_items))
        return all_items
