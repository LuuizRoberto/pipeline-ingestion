import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional

import pyspark.sql.functions as sqlfunc
from delta.tables import DeltaTable
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .bigquery_client import create_bq_client
from .logger import get_logger

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """
    Contrato base para todos os extractors do projeto.

    Toda nova fonte de dados deve herdar desta classe e implementar:
        - get_api_data()
        - normalize_data()

    Opcionalmente pode sobrescrever:
        - _build_dataframe() — quando a fonte precisa de schema customizado
          (ex: Jira com campos dinâmicos via StructType)

    Os métodos save_to_delta_table() e run_extraction() são herdados
    e funcionam para qualquer fonte sem modificação.

    Parâmetros extras de get_api_data() e run_extraction() são passados
    via **kwargs — cada extractor concreto declara e documenta os seus.
    """

    def __init__(self, config: dict, export_date: datetime):
        self.config = config
        self.export_date = export_date
        self.spark = SparkSession.getActiveSession()
        # Client BQ criado uma vez se a fonte tiver bq_dataset configurado.
        # None caso contrário — _write_bigquery verifica antes de usar.
        # Fix 5: guard verifica ambos os campos necessários para BQ
        bq_ready = config.get("bq_dataset") and config.get("bq_project")
        self.bq_client = (create_bq_client(project=config["bq_project"]) if bq_ready else None)


    @abstractmethod
    def get_api_data(self, table_name: str, **kwargs: Any) -> List[dict]:
        """Consulta a API da fonte e retorna os registros brutos.

        Args:
            table_name: Chave da tabela em ``config['tables']``.
            **kwargs:   Parâmetros adicionais específicos de cada fonte
                        (ex: params, guide_list, year, id_empresa).
        """
        ...

    @abstractmethod
    def normalize_data(self, raw_data: List[dict]) -> List[dict]:
        """
        Normaliza os registros antes de salvar.
        Deve padronizar nomes de colunas e tratar tipos complexos.
        """
        ...

    def _build_dataframe(
        self, records: List[dict], schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Constrói o DataFrame a partir dos registros normalizados.

        Comportamento padrão: usa JSON RDD para inferência automática de tipos.
        Sobrescreva este método quando precisar de schema explícito (ex: Jira).

        Args:
            records: lista de registros normalizados
            schema:  StructType opcional — se fornecido, usado diretamente
        """
        if schema is not None:
            return self.spark.createDataFrame(records, schema)

        # Comportamento padrão: inferência via JSON RDD
        json_rdd = self.spark.sparkContext.parallelize(
            [json.dumps(r) for r in records]
        )
        raw_df = self.spark.read.json(json_rdd)

        for col in raw_df.columns:
            raw_df = raw_df.withColumnRenamed(col, col.lower())

        return raw_df

    def _align_schema(self, df: DataFrame, target_table: str) -> DataFrame:
        """Alinha o DataFrame ao schema da tabela Delta existente.

        Adiciona colunas novas via ALTER TABLE (preserva histórico) e garante
        que tipos e ordem das colunas coincidam com a tabela de destino antes
        do merge.

        Args:
            df:           DataFrame com os dados novos a serem salvos.
            target_table: Nome completo da tabela Delta (catalog.schema.table).

        Returns:
            DataFrame realinhado com o schema atual da tabela de destino.
        """
        self.spark.catalog.refreshTable(target_table)
        target_df = self.spark.table(target_table)
        new_columns = [c for c in df.columns if c not in target_df.columns]

        if new_columns:
            # Novos campos: adiciona via ALTER TABLE, preservando histórico.
            # NUNCA usar overwrite aqui — destruiria todos os dados históricos.
            logger.warning(
                "Novos campos detectados em '%s': %s. "
                "Adicionando via ALTER TABLE (dados preservados).",
                target_table,
                new_columns,
            )
            col_types = dict(df.dtypes)
            for col_name in new_columns:
                col_type = col_types.get(col_name, "string")
                self.spark.sql(
                    f"ALTER TABLE {target_table} ADD COLUMNS ({col_name} {col_type})"
                )
            self.spark.catalog.refreshTable(target_table)
            target_df = self.spark.table(target_table)

        for field in target_df.schema:
            if field.name not in df.columns:
                df = df.withColumn(field.name, sqlfunc.lit(None).cast(field.dataType))
            else:
                df = df.withColumn(
                    field.name, sqlfunc.col(field.name).cast(field.dataType)
                )
        return df.select(*target_df.columns)

    def _coerce_numeric_types(
        self, df: DataFrame, numeric_id_columns: List[str]
    ) -> DataFrame:
        """Força colunas numéricas ambíguas para Double em tabelas novas.

        Evita o conflito Long vs Double que ocorre quando a inferência de tipo
        via JSON RDD escolhe Long para valores inteiros em colunas financeiras.
        Colunas listadas em ``numeric_id_columns`` são excluídas do cast —
        são IDs e contadores que devem permanecer como inteiros.

        Args:
            df:                 DataFrame recém-construído (tabela ainda não existe).
            numeric_id_columns: Nomes de colunas que NÃO devem ser convertidas
                                para Double (ex: IDs, números de sequência).

        Returns:
            DataFrame com colunas numéricas ambíguas convertidas para Double.
        """
        for col_name, col_type in df.dtypes:
            if col_type in ("long", "int", "integer") and col_name not in numeric_id_columns:
                df = df.withColumn(col_name, sqlfunc.col(col_name).cast("double"))
        return df

    def _write_databricks(
        self, df: DataFrame, target_table: str, merge_condition: str, table_exists: bool
    ) -> None:
        """Persiste o DataFrame no Delta Lake — MERGE se a tabela existe, CREATE TABLE se não.

        Args:
            df:              DataFrame a ser persistido.
            target_table:    Nome completo da tabela Delta.
            merge_condition: Condição SQL do merge (ex: "t.id = s.id").
            table_exists:    Flag indicando se a tabela já existe no catálogo.
        """
        if table_exists:
            logger.info("Executando Merge em: %s", target_table)
            dt = DeltaTable.forName(self.spark, target_table)
            (
                dt.alias("t")
                .merge(df.alias("s"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info("Criando nova tabela Delta: %s", target_table)
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target_table)

    def _write_snapshot(self, table_name: str) -> int:
        """Grava snapshot completo da tabela em Parquet, particionado por data.

        Lê a tabela COMPLETA do Databricks após todas as ingestões — não só o batch.
        Deve ser chamado UMA ÚNICA VEZ no job, após todas as iterações concluírem.
        O retorno (row_count) é usado diretamente pelo _log_ingestion_result.

        Args:
            table_name: Chave da tabela em ``config['tables']``.

        Returns:
            Quantidade total de linhas da tabela no momento do snapshot.
        """
        table_config      = self.config["tables"][table_name]
        target_table      = table_config["databricks_table_target"]
        volume_path       = table_config["volume_path"]
        gcs_snapshot_path = table_config.get("gcs_snapshot_path")

        full_df = self.spark.table(target_table)

        # count() antes do write evita double scan — uma só leitura da tabela
        row_count = full_df.count()

        partition_path = f"dl_export_date={self.export_date.strftime('%Y-%m-%d')}"

        # Snapshot no Volume do Databricks
        volume_full_path = f"{volume_path.rstrip('/')}/{partition_path}"
        full_df.write.mode("overwrite").parquet(volume_full_path)
        logger.info("Snapshot Databricks salvo em: %s (%d linhas)", volume_full_path, row_count)

        # Snapshot no GCS — ativo quando gcs_snapshot_path estiver configurado na tabela
        if gcs_snapshot_path:
            gcs_full_path = f"{gcs_snapshot_path.rstrip('/')}/{partition_path}"
            full_df.write.mode("overwrite").parquet(gcs_full_path)
            logger.info("Snapshot GCS salvo em: %s", gcs_full_path)

        return row_count

    def _write_bigquery(
        self, df: DataFrame, bq_table: str, merge_condition: str
    ) -> None:
        """Upsert do DataFrame no BigQuery via staging table + MERGE DML.

        Interface simétrica a _write_databricks: recebe os mesmos parâmetros
        já calculados pelo orquestrador, sem acessar config internamente.

        Fluxo:
            1. Coleta o DataFrame como lista de dicts (export_date → string).
            2. Grava em staging table com WRITE_TRUNCATE + autodetect.
            3. Se tabela alvo não existe → copia staging (primeira carga).
            4. Se tabela alvo existe → alinha schema (ADD COLUMN) + MERGE.
            5. Remove staging table.

        Best-effort: falhas são logadas como WARNING sem interromper o pipeline.

        Args:
            df:              DataFrame com export_date e deduplicado.
            bq_table:        Nome completo BigQuery (project.dataset.table),
                             lido de ``bigquery_table_target`` no config.
            merge_condition: Condição SQL do merge (ex: "t.id = s.id"),
                             a mesma usada em _write_databricks.
        """
        if not self.bq_client:
            return

        # staging_table definido fora do try para estar acessível no finally
        staging_table = f"{bq_table}_staging_{self.export_date.strftime('%Y%m%d%H%M%S')}"

        try:
            # Coleta Spark DF → lista de dicts sem Pandas.
            # export_date é castado para string para serialização JSON correta.
            records_bq = [
                row.asDict()
                for row in df.withColumn(
                    "export_date", sqlfunc.col("export_date").cast("string")
                ).collect()
            ]

            if not records_bq:
                return

            all_cols = list(records_bq[0].keys())

            # 1. Verifica existência da tabela alvo ANTES de carregar staging.
            #    Isso permite usar o schema do alvo para garantir tipos idênticos.
            try:
                target_ref    = self.bq_client.get_table(bq_table)
                target_exists = True
                target_schema = {f.name: f for f in target_ref.schema}
            except NotFound:
                target_exists = False
                target_schema = {}

            # 2. Monta schema explícito para a staging:
            #    - colunas existentes: usa o tipo da tabela alvo (evita type mismatch no MERGE)
            #    - colunas novas: infere do valor Python (bool checado antes de int)
            staging_schema_fields = []
            for col in all_cols:
                if col in target_schema:
                    staging_schema_fields.append(target_schema[col])
                else:
                    sample = next(
                        (r[col] for r in records_bq if r.get(col) is not None), None
                    )
                    if isinstance(sample, bool):
                        bq_type = "BOOL"
                    elif isinstance(sample, int):
                        bq_type = "INT64"
                    elif isinstance(sample, float):
                        bq_type = "FLOAT64"
                    else:
                        bq_type = "STRING"
                    staging_schema_fields.append(bigquery.SchemaField(col, bq_type))

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=staging_schema_fields,
            )
            self.bq_client.load_table_from_json(
                records_bq, staging_table, job_config=job_config
            ).result()
            logger.info("BQ staging gravado: %s (%d registros)", staging_table, len(records_bq))

            # 3. Primeira carga ou MERGE
            if not target_exists:
                logger.info("BQ: primeira carga em '%s'. Copiando staging.", bq_table)
                self.bq_client.copy_table(staging_table, bq_table).result()
            else:
                # Adiciona colunas novas detectadas no staging (não presentes no alvo)
                new_fields = [
                    f for f in staging_schema_fields if f.name not in target_schema
                ]
                if new_fields:
                    add_cols = ", ".join(
                        [f"ADD COLUMN {f.name} {f.field_type}" for f in new_fields]
                    )
                    self.bq_client.query(f"ALTER TABLE `{bq_table}` {add_cols}").result()
                    logger.warning(
                        "BQ: novas colunas adicionadas em '%s': %s",
                        bq_table, [f.name for f in new_fields],
                    )

                # MERGE — usa a mesma merge_condition do Databricks (aliases t/s).
                # PKs incluídas no UPDATE SET são no-op (t.pk = s.pk com mesmo valor).
                update_set  = ", ".join([f"t.{c} = s.{c}" for c in all_cols])
                insert_cols = ", ".join(all_cols)
                insert_vals = ", ".join([f"s.{c}" for c in all_cols])

                merge_sql = f"""
                    MERGE `{bq_table}` t
                    USING `{staging_table}` s
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_cols}) VALUES ({insert_vals})
                """
                self.bq_client.query(merge_sql).result()

            logger.info("BQ upsert concluído: %s", bq_table)

        except Exception:
            logger.warning(
                "Falha na escrita BigQuery para '%s' — pipeline Databricks não afetado.",
                bq_table,
                exc_info=True,
            )
        finally:
            # Garante limpeza da staging mesmo quando MERGE ou qualquer etapa falha
            self.bq_client.delete_table(staging_table, not_found_ok=True)

    def save_to_delta_table(
        self, records: List[dict], table_name: str, schema: Optional[StructType] = None
    ) -> None:
        """Salva registros no Delta Lake com alinhamento de schema e merge por PK.

        Orquestra o fluxo de persistência delegando cada etapa para métodos
        privados especializados. Compartilhado por todas as fontes.

        Args:
            records:    Lista de registros normalizados. Se vazia, encerra sem erro.
            table_name: Chave da tabela em ``config['tables']``.
            schema:     StructType opcional passado para ``_build_dataframe()``.
        """
        if not records:
            logger.warning("Sem dados para salvar em '%s'. Pulando.", table_name)
            return

        table_config       = self.config["tables"][table_name]
        target_table       = table_config["databricks_table_target"]
        bq_table           = table_config["bigquery_table_target"]
        primary_key        = table_config["primary_key"]
        numeric_id_columns = table_config.get("numeric_id_columns", [])

        df = self._build_dataframe(records, schema)
        df = df.withColumn("export_date", sqlfunc.lit(self.export_date))

        # Avalia tableExists uma única vez para evitar chamada dupla ao catálogo
        table_exists = self.spark.catalog.tableExists(target_table)

        if table_exists:
            logger.info("Alinhando schema com tabela existente: %s", target_table)
            df = self._align_schema(df, target_table)
        else:
            df = self._coerce_numeric_types(df, numeric_id_columns)

        pk_list = (
            [pk.lower() for pk in primary_key]
            if isinstance(primary_key, list)
            else [primary_key.lower()]
        )
        df = df.dropDuplicates(pk_list)

        merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in pk_list])

        self._write_databricks(df, target_table, merge_condition, table_exists)
        self._write_bigquery(df, bq_table, merge_condition)

    def _log_ingestion_result(
        self,
        table_name: str,
        record_count: int,
        resultado: str,
        error_message: Optional[str] = None,
    ) -> None:
        """Grava o resultado da extração em data_ingestion_results no BigQuery.

        Chamado via finally em run_extraction() — executa sempre, em sucesso ou falha.
        Best-effort: falhas de logging não afetam o pipeline.

        Args:
            table_name:    Chave da tabela em ``config['tables']``.
            record_count:  Quantidade de registros do batch ingerido.
            resultado:     ``"Sucesso"`` ou ``"Falha"``.
            error_message: Mensagem da exceção em caso de falha; None em caso de sucesso.
        """
        if not self.bq_client:
            return
        try:
            log_table = (
                f"{self.config['bq_project']}"
                f".{self.config['bq_dataset']}"
                f".data_ingestion_results"
            )
            table_config = self.config["tables"][table_name]
            row = {
                "export_date":        self.export_date.strftime("%Y-%m-%d"),
                "table_name":         table_config["databricks_table_target"],
                "record_count":       record_count,
                "resultado_ingestao": resultado,
                "error_message":      error_message,
            }
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self.bq_client.load_table_from_json([row], log_table, job_config=job_config).result()
        except Exception:
            logger.warning("Falha ao gravar log de ingestão — pipeline não afetado.", exc_info=True)

    def run_extraction(self, table_name: str, **kwargs: Any) -> None:
        """Orquestra o fluxo completo: get → normalize → save.

        Args:
            table_name: Chave da tabela em ``config['tables']``.
            **kwargs:   Repassados diretamente para ``get_api_data()``.
                        Cada extractor concreto declara os kwargs que utiliza.

        Raises:
            Exception: Re-levanta qualquer exceção após logging com traceback.
        """
        logger.info("Iniciando extração: %s", table_name)
        try:
            raw = self.get_api_data(table_name, **kwargs)
            if not raw:
                logger.warning("Nenhum dado retornado para '%s'.", table_name)
                return
            normalized = self.normalize_data(raw)
            self.save_to_delta_table(normalized, table_name)
            logger.info("Extração concluída: %s", table_name)
        except Exception:
            logger.exception("Falha na extração de '%s'.", table_name)
            raise

