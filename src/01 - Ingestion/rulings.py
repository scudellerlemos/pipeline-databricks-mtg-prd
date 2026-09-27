# Databricks notebook source
# Ingestão de Rulings - Magic: The Gathering (Scryfall)
# Objetivo: Ingerir rulings (decisões oficiais de regras) via Scryfall Bulk Data API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, sem filtro temporal, incremental, idempotente

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import gzip
import json
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# FUNÇÕES COMPARTILHADAS (get_secret/setup_s3_storage/save_to_parquet/
# http_get_with_retry/start_run/finish_run vivem em ingestion_utils.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./ingestion_utils

# COMMAND ----------

# =============================================================================
# VARIÁVEIS DE CONFIGURAÇÃO
# =============================================================================
S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BASE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"
SCRYFALL_API_URL = get_secret("scryfall_api_url")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
MAX_RETRIES = int(get_secret("max_retries", "3"))
# rulings = 1 objeto por ruling, referenciando a carta via oracle_id (não por
# impressão) - mesmo padrão de bulk-data usado por cards.ipynb/card_prices.ipynb.
SCRYFALL_BULK_TYPE = "rulings"

# Sem filtro years_back aqui: diferente de cards/sets/card_prices (onde a
# janela limita o catálogo a impressões/preços recentes), uma ruling antiga
# sobre uma carta antiga continua válida e relevante hoje - não "expira" pelo
# calendário. O catálogo inteiro é pequeno (~79k linhas, ~5MB comprimido), sem
# necessidade de recorte.
print("Sem filtro temporal - captura o catálogo de rulings inteiro")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE RULINGS
# =============================================================================
RULINGS_SCHEMA = StructType([
    StructField("oracle_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("comment", StringType(), True),
])


def _to_ruling_record(ruling):
    # Landing zone captura a ruling como a Scryfall devolve, referenciada por
    # oracle_id - o join com as impressões de cards.ipynb (1 oracle_id -> N
    # impressões) fica pra Bronze/Silver, não pra Stage.
    return {
        "oracle_id": ruling.get("oracle_id"),
        "source": ruling.get("source"),
        "published_at": ruling.get("published_at"),
        "comment": ruling.get("comment"),
    }


def fetch_ruling_records():
    # Mesmo padrão de card_prices.ipynb: 1 request pro índice do Bulk Data +
    # 1 pro catálogo inteiro, sem requisição por carta/ruling.
    resp = http_get_with_retry(f"{SCRYFALL_API_URL}/bulk-data", headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
    entry = next(e for e in resp.json()["data"] if e["type"] == SCRYFALL_BULK_TYPE)

    raw = http_get_with_retry(entry["jsonl_download_uri"], headers=SCRYFALL_HEADERS, timeout=120, retries=MAX_RETRIES).content
    return [
        _to_ruling_record(json.loads(line))
        for line in gzip.decompress(raw).decode("utf-8").splitlines()
        if line.strip()
    ]


def ingest_rulings(table_name="rulings", run=None):
    print("Baixando catálogo de rulings Scryfall...")

    all_data = fetch_ruling_records()
    print(f"Rulings obtidas do catálogo: {len(all_data)}")

    if not all_data:
        print(f"Nenhum dado válido para {table_name}")
        return None

    df = save_to_parquet(
        spark, all_data, table_name, S3_BASE_PATH,
        schema=RULINGS_SCHEMA,
        run=run,
    )

    if df is not None:
        count = df.count()
        print(f"{table_name}: {count} registros processados")
        return df
    return None

# COMMAND ----------

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

# Configurar S3 Storage
setup_success = setup_s3_storage(S3_BASE_PATH)
if not setup_success:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# Controle de execução (run_id, status, contagens) via run_stage_ingestion -
# padroniza o wrapper start_run -> try/ingest -> finish_run - ver ingestion_utils.py
rulings_df, run = run_stage_ingestion(
    "rulings", "bulk-data/rulings",
    lambda run: ingest_rulings(table_name="rulings", run=run),
    S3_BASE_PATH,
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE RULINGS")
print("=" * 50)

if rulings_df is not None:
    print("Arquivos salvos")
else:
    print("Falha na ingestão de rulings")

print("=" * 50)
