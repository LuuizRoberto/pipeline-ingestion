import base64
import json
import re
import unicodedata
from datetime import datetime
from typing import List

import requests
from google.cloud import secretmanager
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.finance_data_pipelines.core import BaseExtractor, get_logger

logger = get_logger(__name__)


class ExtractorJira(BaseExtractor):
    """Extractor para a API de issues do Jira (Atlassian Cloud).

    Herda save_to_delta_table() e run_extraction() da BaseExtractor.
    Sobrescreve _build_dataframe() para usar StructType explícito —
    necessário porque o Jira tem campos dinâmicos que variam por issue.

    Autenticação via Basic Auth (user + token) no GCP Secret Manager.
    Suporta paginação via nextPageToken.
    """

    _HTTP_TIMEOUT = 60

    def __init__(self, config: dict, export_date: datetime):
        super().__init__(config, export_date)
        self.jira_instance = config["instance"]
        self.project_id = config["project_id"]

        # M10: client instanciado uma vez e reutilizado em todas as chamadas
        self.sm_client = secretmanager.SecretManagerServiceClient()

        # C3: session com retry exponencial — mesma estratégia do ExtractorMatera
        self.session = self._create_session()

        self.auth_header = self._build_auth_header()

        # M11: _fetch_field_metadata constrói field_metadata E o índice reverso
        self.field_metadata, self._field_name_to_id = self._fetch_field_metadata()

    # ------------------------------------------------------------------
    # Autenticação e sessão
    # ------------------------------------------------------------------

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

    def _get_secret(self, secret_id: str) -> str:
        """Busca credencial no GCP Secret Manager."""
        resource = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
        return (
            self.sm_client.access_secret_version(request={"name": resource})
            .payload.data.decode("UTF-8")
        )

    def _build_auth_header(self) -> dict:
        """Constrói o header de autenticação Basic Auth."""
        user = self._get_secret(self.config["sec"]["sm_user_secret_id"])
        token = self._get_secret(self.config["sec"]["sm_token_secret_id"])
        auth_string = base64.b64encode(f"{user}:{token}".encode("ascii")).decode("ascii")
        return {
            "Authorization": f"Basic {auth_string}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Metadados dos campos
    # ------------------------------------------------------------------

    @staticmethod
    def _strip_accents(s: str) -> str:
        """Remove acentos de uma string."""
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    def _fetch_field_metadata(self) -> tuple[dict, dict]:
        """Busca metadados de todos os campos do Jira e constrói dois índices.

        Returns:
            Tupla com:
                - field_metadata:     {field_id: {id, name_normalizado, ...}}
                - field_name_to_id:   {nome_normalizado: field_id}  ← índice reverso (M11)
        """
        logger.info("Buscando metadados dos campos do Jira...")
        url = f"{self.jira_instance}/rest/api/3/field"
        response = self.session.get(url, headers=self.auth_header, timeout=self._HTTP_TIMEOUT)
        response.raise_for_status()

        field_metadata: dict = {}
        for item in response.json():
            field_id = item["id"]
            field_metadata[field_id] = item
            field_metadata[field_id]["name"] = re.sub(
                r"^([0-9])", r"_\1",
                re.sub(
                    "[^A-Za-z0-9\\s]+", "",
                    self._strip_accents(item["name"])
                ).replace(" ", "_"),
            )

        # Índice reverso: nome normalizado → field_id
        # Permite lookup O(1) em _resolve_field_ids em vez de O(n×m)
        field_name_to_id = {
            meta["name"].lower(): field_id
            for field_id, meta in field_metadata.items()
        }

        logger.info("Mapeados %d campos do Jira.", len(field_metadata))
        return field_metadata, field_name_to_id

    def _resolve_field_ids(self, fields_table: List[str]) -> List[str]:
        """Mapeia nomes de campos para IDs do Jira via lookup O(1)."""
        return [
            self._field_name_to_id[name]
            for name in fields_table
            if name in self._field_name_to_id
        ]

    # ------------------------------------------------------------------
    # Interface obrigatória (BaseExtractor)
    # ------------------------------------------------------------------

    def get_api_data(self, table_name: str, **kwargs) -> List[dict]:  # type: ignore[override]
        """Busca issues do Jira via JQL com paginação por nextPageToken.

        Kwargs:
            params (dict): deve conter 'start_date' e 'end_date' (datetime).
        """
        # M12: validação defensiva — erro claro antes de tentar usar os valores
        params = kwargs.get("params") or {}
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        if not start_date or not end_date:
            raise ValueError(
                "params deve conter 'start_date' e 'end_date' como objetos datetime. "
                f"Recebido: {list(params.keys())}"
            )

        table_config = self.config["tables"][table_name]
        project      = table_config["project"]
        changed_date = table_config["changed_date"]
        fields_table = table_config["fields_table"]
        field_ids    = self._resolve_field_ids(fields_table)

        jql_filter = "{} >= '{}' AND {} <= '{}' AND project='{}'".format(
            changed_date,
            start_date.strftime("%Y-%m-%d"),
            changed_date,
            end_date.strftime("%Y-%m-%d"),
            project,
        )

        logger.info("JQL: %s", jql_filter)

        query = {
            "jql": jql_filter,
            "maxResults": 5000,
            "fields": field_ids,
        }

        issues = []
        search_url = f"{self.jira_instance}/rest/api/3/search/jql"

        # C3: self.session no lugar de requests.get direto
        response = self.session.get(
            search_url, headers=self.auth_header, params=query, timeout=self._HTTP_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        issues.extend(data["issues"])

        is_last = data.get("isLast", True)
        next_page_token = data.get("nextPageToken")

        logger.info("Recebidos %d issues. isLast: %s", len(data["issues"]), is_last)

        while not is_last:
            paginated_query = query.copy()
            paginated_query["nextPageToken"] = next_page_token

            # C3: self.session no lugar de requests.get direto
            response = self.session.get(
                search_url,
                headers=self.auth_header,
                params=paginated_query,
                timeout=self._HTTP_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            issues.extend(data["issues"])

            is_last = data.get("isLast", True)
            next_page_token = data.get("nextPageToken")

            logger.info(
                "Paginação: +%d issues | nextToken: %s | isLast: %s",
                len(data["issues"]), next_page_token, is_last,
            )

            if not next_page_token:
                logger.info("Sem mais páginas. Encerrando paginação.")
                break

        logger.info("Total de issues coletados: %d", len(issues))
        return issues

    def normalize_data(self, raw_data: List[dict]) -> List[dict]:
        """Achata a estrutura aninhada das issues do Jira para um dict plano.

        Campos em 'fields' são normalizados pelo metadado. Tipos complexos
        (dict/list) são serializados como JSON string. Campos desconhecidos
        são ignorados com WARNING em vez de causar KeyError.
        """
        records = []
        for issue in raw_data:
            record = {}
            for field, value in issue.items():
                if field == "expand":
                    continue  # metadado interno do Jira, sem valor analítico
                if field != "fields":
                    record[f"issue_{field.lower()}"] = value
                else:
                    for field_id, field_value in value.items():
                        meta = self.field_metadata.get(field_id)
                        if meta is None:
                            logger.warning(
                                "Campo desconhecido ignorado (ausente no metadado): %s",
                                field_id,
                            )
                            continue
                        field_name = meta["name"].lower()
                        if isinstance(field_value, (dict, list)):
                            record[field_name] = json.dumps(field_value)
                        else:
                            record[field_name] = field_value
            records.append(record)
        return records

    def _build_dataframe(self, records: List[dict], schema=None) -> DataFrame:
        """Constrói DataFrame com schema inferido da união de todos os registros.

        Sobrescreve o _build_dataframe da BaseExtractor. O Jira tem campos
        dinâmicos que variam por issue — inferir schema só do records[0]
        causaria perda silenciosa de campos presentes nos demais registros.

        Retorna DataFrame vazio se records estiver vazio — cobre períodos
        sem issues (feriados, fins de semana).
        """
        if not records:
            return self.spark.createDataFrame([], StructType([]))

        # C4: coleta a união de todas as chaves de todos os registros
        all_fields: dict[str, type] = {}
        for record in records:
            for field, value in record.items():
                if field not in all_fields and value is not None:
                    all_fields[field] = value

        # Infere o tipo Spark pelo primeiro valor não-None encontrado para cada campo
        schema_fields = []
        for field, sample_value in all_fields.items():
            if isinstance(sample_value, bool):
                schema_fields.append(StructField(field, BooleanType()))
            elif isinstance(sample_value, int):
                schema_fields.append(StructField(field, IntegerType()))
            elif isinstance(sample_value, float):
                schema_fields.append(StructField(field, DoubleType()))
            else:
                schema_fields.append(StructField(field, StringType()))

        inferred_schema = StructType(schema_fields)

        # Garante que todos os registros têm todas as chaves (None para campos ausentes)
        all_keys = list(all_fields.keys())
        normalized_records = [
            {key: record.get(key) for key in all_keys}
            for record in records
        ]

        return self.spark.createDataFrame(normalized_records, inferred_schema)
