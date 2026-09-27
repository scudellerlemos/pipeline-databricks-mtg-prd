# Databricks notebook source
# Ingestão de Preços de Cards - Magic: The Gathering
# Objetivo: Ingerir preços das cartas via Scryfall Bulk Data API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, incremental, idempotente

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import gzip
import json
from datetime import datetime
from pyspark.sql import SparkSession
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
# Scryfall rejeita o User-Agent default do requests (erro "generic_user_agent")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
MAX_RETRIES = int(get_secret("max_retries", "3"))
# default_cards = 1 objeto por IMPRESSÃO, cada um com seu próprio `prices` -
# o mesmo bulk que cards.py usa. O preço de Magic varia por impressão (um
# Lightning Bolt de 1993 e a reimpressão de 2026 não valem o mesmo), então
# oracle_cards (1 objeto por Oracle ID, deduplicado entre impressões) devolvia
# o preço de uma impressão arbitrária como se fosse o preço "da carta".
SCRYFALL_BULK_TYPE = "default_cards"

# Janela temporal: mesma fonte que cards/sets (secret years_back). card_prices
# grava seu próprio snapshot independente e aplica o mesmo filtro por
# releaseDate que cards.py/sets.py usam, sem depender da execução deles.
YEARS_BACK = int(get_secret("years_back", "5"))
current_year = datetime.now().year
cutoff_year = current_year - YEARS_BACK
CUTOFF_DATE_STR = datetime(cutoff_year, 1, 1).strftime("%Y-%m-%d")

print(f"YEARS_BACK: {YEARS_BACK} | CUTOFF_DATE_STR: {CUTOFF_DATE_STR}")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE CARD_PRICES
# =============================================================================
CARD_PRICES_SCHEMA = StructType([
    # id = id da impressão (mesmo `id` de cards.py) - chave de join com
    # TB_FATO_CARTAS.ID_CARTA da Silver em diante.
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("set", StringType(), True),
    StructField("rarity", StringType(), True),
    # A Scryfall cota cada variante fisica da mesma impressao separadamente -
    # foil chega a valer varios multiplos do nao-foil (ex.: Lightning Bolt em
    # msc, usd 0.74 vs usd_foil 3.73). Capturar so `usd` exibiria o preco de
    # uma variante como se fosse o da impressao inteira.
    StructField("usd", StringType(), True),
    StructField("usd_foil", StringType(), True),
    StructField("usd_etched", StringType(), True),
    StructField("eur", StringType(), True),
    StructField("eur_foil", StringType(), True),
    StructField("tix", StringType(), True),
    StructField("scryfall_uri", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("releaseDate", StringType(), True),
])


def _to_price_record(card):
    # Landing zone captura o catálogo de preços como a Scryfall devolve, sem
    # tentar casar com os arquivos de `cards` já gravados no S3 - esse join
    # (1:1 por id da impressão) fica pra Silver/Gold, não pra Stage.
    prices = card.get("prices", {}) or {}
    image_uris = card.get("image_uris")
    return {
        "id": card.get("id"),
        "name": card.get("name"),
        "set": card.get("set"),
        "rarity": card.get("rarity"),
        "usd": prices.get("usd"),
        "usd_foil": prices.get("usd_foil"),
        "usd_etched": prices.get("usd_etched"),
        "eur": prices.get("eur"),
        "eur_foil": prices.get("eur_foil"),
        "tix": prices.get("tix"),
        "scryfall_uri": card.get("scryfall_uri"),
        "image_url": image_uris.get("normal") if image_uris else None,
        "releaseDate": card.get("released_at"),
    }


def fetch_price_records():
    # Mesmo padrão de cards.py/sets.py: 1 request pro índice do Bulk Data +
    # 1 pro catálogo inteiro, sem requisição por carta.
    resp = http_get_with_retry(f"{SCRYFALL_API_URL}/bulk-data", headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
    entry = next(e for e in resp.json()["data"] if e["type"] == SCRYFALL_BULK_TYPE)

    raw = http_get_with_retry(entry["jsonl_download_uri"], headers=SCRYFALL_HEADERS, timeout=120, retries=MAX_RETRIES).content
    return [
        _to_price_record(json.loads(line))
        for line in gzip.decompress(raw).decode("utf-8").splitlines()
        if line.strip()
    ]


def ingest_card_prices(table_name="card_prices", run=None):
    print(f"Baixando catálogo de preços Scryfall ({SCRYFALL_BULK_TYPE})...")

    all_data = fetch_price_records()
    print(f"Preços obtidos do catálogo: {len(all_data)}")

    if not all_data:
        print(f"Nenhum dado válido para {table_name}")
        return None

    df = save_to_parquet(
        spark, all_data, table_name, S3_BASE_PATH,
        schema=CARD_PRICES_SCHEMA,
        partition_source_col="releaseDate", cutoff_date_str=CUTOFF_DATE_STR,
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

# Verificar Spark
try:
    spark
    print("Spark disponível")
except NameError:
    print("Spark não está disponível - tentando obter do contexto")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        print("Spark criado com sucesso")
    except Exception as e:
        print(f"Erro ao criar Spark: {e}")
        raise Exception("Spark não está disponível")

# Configurar S3 Storage
setup_success = setup_s3_storage(S3_BASE_PATH)
if not setup_success:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# Controle de execução (run_id, status, contagens) via run_stage_ingestion -
# padroniza o wrapper start_run -> try/ingest -> finish_run - ver ingestion_utils.py
prices_df, run = run_stage_ingestion(
    "card_prices", "bulk-data/default_cards",
    lambda run: ingest_card_prices(table_name="card_prices", run=run),
    S3_BASE_PATH,
    params={"years_back": YEARS_BACK},
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE PREÇOS")
print("=" * 50)

if prices_df is not None:
    print("✅ Arquivos salvos com sucesso")
    print(f"📊 Total de registros: {prices_df.count()}")
    print(f"🎯 Particionamento: por releaseDate (janela de {YEARS_BACK} anos)")
else:
    print("❌ Falha na ingestão de preços")

print("=" * 50)
