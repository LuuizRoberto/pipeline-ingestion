"""
Script local — Ranking de Câmbio por Instituição Financeira (Bacen)

Fonte: https://www.bcb.gov.br/estatisticas/rankingcambioinstituicoes
Destino: BigQuery — avenue_finance.avenue_bacen_api_ranking_cambio_instituicao_financeiras

IMPORTANTE: roda apenas localmente. A API do Bacen bloqueia requests vindos
de ambientes cloud (IPs de datacenter). Execute na sua máquina.

Uso:
    python scripts/bacen/scrape_ranking_cambio.py
    python scripts/bacen/scrape_ranking_cambio.py --ano-inicio 2024 --ano-fim 2026

Requisitos (além do pyproject.toml do repo):
    pip install openpyxl

Variáveis de ambiente necessárias (via .env na raiz do repo):
    DATABRICKS_HOST
    DATABRICKS_TOKEN
"""

import argparse
import io
import logging
import re
import sys
import unicodedata
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery

# Permite rodar com `python scripts/bacen/...` a partir de qualquer diretório
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.finance_data_pipelines.core.bigquery_client import create_bq_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GCP_TABLE = "avenue_finance.avenue_bacen_api_ranking_cambio_instituicao_financeiras"

BACEN_API = (
    "https://www.bcb.gov.br/api/servico/sitebcb/cronologicodados"
    "?tronco=estatisticas"
    "&guidLista=61AD465E-A5CC-4646-96F3-CFBC436BEDA0"
    "&ano={ano}"
    "&ordem=DataReferencia%20desc,%20Title%20asc"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":    "https://www.bcb.gov.br/estatisticas/rankingcambioinstituicoes",
    "Accept":     "application/json",
}

COLUNAS_TEXTO = {"ano", "mes", "cnpj_da_instituicao", "nome_da_instituicao", "conglomerado"}

COLUNAS_FINAIS = [
    "rank", "cnpj_da_instituicao", "nome_da_instituicao", "conglomerado",
    "exportacao_quant", "exportacao_valor_us",
    "importacao_quant", "importacao_valor_us",
    "transferencia_do_exterior_quant", "transferencia_do_exterior_valor_us",
    "transferencia_p_exterior_quant", "transferencia_p_exterior_valor_us",
    "total_do_primario_quant", "total_do_primario_valor_us",
    "interbancario_compra_quant", "interbancario_compra_valor_us",
    "interbancario_venda_quant", "interbancario_venda_valor_us",
    "total_do_interbancario_quant", "total_do_interbancario_valor_us",
    "total_quant", "total_valor_us",
]

MESES_PT = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}


def _limpar(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def listar_zips(ano: int) -> list[dict]:
    resp = requests.get(BACEN_API.format(ano=ano), headers=HEADERS, timeout=15)
    resp.raise_for_status()

    result = []
    for item in resp.json()["conteudo"]:
        url = "https://www.bcb.gov.br" + item["Url"]
        result.append({
            "ano": ano,
            "mes": int(item["dataReferencia"][5:7]),
            "url": url,
        })

    log.info("Ano %d: %d ZIP(s) encontrado(s)", ano, len(result))
    return result


def processar(ano: int, mes: int, url: str) -> pd.DataFrame:
    zb = requests.get(url, headers=HEADERS, timeout=90).content
    log.info("Bytes baixados: %d", len(zb))

    with zipfile.ZipFile(io.BytesIO(zb)) as zf:
        todos = zf.namelist()
        log.info("Arquivos no ZIP: %s", todos)

        alvo = next(
            (n for n in todos
             if f"{ano}" in n
             and f"{mes:02d}" in n
             and n.endswith(".xlsx")
             and "acumulado" not in n.lower()),
            None,
        )

        if alvo is None:
            raise FileNotFoundError(
                f"Nenhum XLSX encontrado para {ano}/{mes:02d} no ZIP. Arquivos: {todos}"
            )

        log.info("Arquivo selecionado: %s", alvo)
        conteudo = zf.read(alvo)

    xl = pd.ExcelFile(io.BytesIO(conteudo), engine="openpyxl")
    mes_abrev = MESES_PT[mes]

    aba_alvo = next(
        (s for s in xl.sheet_names
         if "-" not in s
         and str(ano) in s
         and mes_abrev.lower() in s.lower()),
        None,
    )
    if aba_alvo is None:
        aba_alvo = next((s for s in xl.sheet_names if "-" not in s), None)
    if aba_alvo is None:
        raise ValueError(f"Nenhuma aba mensal encontrada. Abas disponíveis: {xl.sheet_names}")

    log.info("Aba selecionada: '%s'", aba_alvo)

    df_raw = pd.read_excel(
        io.BytesIO(conteudo),
        sheet_name=aba_alvo,
        engine="openpyxl",
        header=None,
        dtype=str,
    )

    header_linha = next(
        i for i, row in df_raw.iterrows()
        if any(str(v).strip().lower() == "rank" for v in row)
    )

    has_conglomerado = any(
        "conglomerado" in str(v).lower()
        for v in df_raw.iloc[header_linha].fillna("").tolist()
    )

    colunas = (
        COLUNAS_FINAIS if has_conglomerado
        else [c for c in COLUNAS_FINAIS if c != "conglomerado"]
    )
    df = df_raw.iloc[header_linha + 2:].copy()
    df = df.iloc[:, :len(colunas)]
    df.columns = colunas

    if not has_conglomerado:
        idx = df.columns.tolist().index("nome_da_instituicao") + 1
        df.insert(idx, "conglomerado", None)

    df.insert(0, "mes", str(mes))
    df.insert(0, "ano", str(ano))

    df = df[df["rank"].astype(str).str.strip().str.match(r"^\d+$", na=False)].reset_index(drop=True)
    log.info("Registros com rank numérico: %d", len(df))

    for col in df.columns:
        if col not in COLUNAS_TEXTO:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.replace("-", "", regex=False)
                .replace({"nan": None, "None": None, "": None})
            )

    return df


def _upload(bq_client: bigquery.Client, df: pd.DataFrame, anos: list[int]) -> None:
    anos_str = ", ".join(f"'{a}'" for a in anos)
    bq_client.query(f"DELETE FROM `{GCP_TABLE}` WHERE ano IN ({anos_str})").result()
    log.info("DELETE executado para anos: %s", anos)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_dataframe(df, GCP_TABLE, job_config=job_config)
    job.result()
    log.info("Inseridos %d registros em %s", len(df), GCP_TABLE)


def main() -> None:
    ano_atual = datetime.today().year

    parser = argparse.ArgumentParser(description="Ranking de Câmbio Bacen → BigQuery")
    parser.add_argument("--ano-inicio", type=int, default=ano_atual)
    parser.add_argument("--ano-fim",    type=int, default=ano_atual)
    args = parser.parse_args()

    log.info("Período: %d–%d", args.ano_inicio, args.ano_fim)

    bq_client = create_bq_client()

    frames = []
    for ano in range(args.ano_inicio, args.ano_fim + 1):
        for z in listar_zips(ano):
            try:
                df = processar(ano, z["mes"], z["url"])
                frames.append(df)
                log.info("%d/%02d — %d instituições", ano, z["mes"], len(df))
            except Exception:
                log.exception("Falha em %d/%02d — pulando", ano, z["mes"])

    if not frames:
        log.warning("Nenhum dado coletado. Abortando.")
        return

    df_final = pd.concat(frames, ignore_index=True)
    for col in df_final.columns:
        if col not in COLUNAS_TEXTO:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce")

    log.info("Total: %d registros", len(df_final))
    _upload(bq_client, df_final, list(range(args.ano_inicio, args.ano_fim + 1)))


if __name__ == "__main__":
    main()
