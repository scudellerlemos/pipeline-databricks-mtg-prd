# Databricks notebook source
# Ingestão de Cards - Magic: The Gathering (Bulk Data)
# Objetivo: Ingerir dados de cards via Scryfall Bulk Data API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, incremental, por coleção (set)

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import json
import gzip
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

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
MAX_RETRIES = int(get_secret("max_retries", "3"))

# 1 download do catálogo inteiro da Scryfall, filtrado em memória pelos
# set_codes da janela temporal - bem mais rápido que paginar coleção por
# coleção.
SCRYFALL_API_URL = get_secret("scryfall_api_url")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
# default_cards = 1 objeto por impressão (não por Oracle ID) - grava 1 linha
# por impressão, já que set/artist/number/imageUrl variam por edição. Mesmo
# bulk usado por card_prices.py, pelo mesmo motivo: preço também varia por
# impressão.
SCRYFALL_BULK_TYPE = "default_cards"

# Configurações do S3
S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BASE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"

# Configurações de janela temporal (por coleção: só ingere sets lançados nos últimos YEARS_BACK anos)
YEARS_BACK = int(get_secret("years_back", "5"))
current_year = datetime.now().year
cutoff_year = current_year - YEARS_BACK
CUTOFF_DATE = datetime(cutoff_year, 1, 1)
CUTOFF_DATE_STR = CUTOFF_DATE.strftime("%Y-%m-%d")

print(f"YEARS_BACK: {YEARS_BACK} | CUTOFF_DATE_STR: {CUTOFF_DATE_STR}")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE CARDS
# =============================================================================
CARDS_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("manaCost", StringType(), True),
    StructField("cmc", FloatType(), True),
    StructField("colors", StringType(), True),
    StructField("colorIdentity", StringType(), True),
    StructField("type", StringType(), True),
    StructField("types", StringType(), True),
    StructField("subtypes", StringType(), True),
    StructField("rarity", StringType(), True),
    StructField("set", StringType(), True),
    StructField("setName", StringType(), True),
    StructField("text", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("number", StringType(), True),
    StructField("power", StringType(), True),
    StructField("toughness", StringType(), True),
    StructField("layout", StringType(), True),
    StructField("multiverseid", IntegerType(), True),
    StructField("imageUrl", StringType(), True),
    StructField("variations", StringType(), True),
    StructField("foreignNames", StringType(), True),
    StructField("printings", StringType(), True),
    StructField("originalText", StringType(), True),
    StructField("originalType", StringType(), True),
    StructField("legalities", StringType(), True),
    StructField("id", StringType(), True),
    # oracle_id identifica a carta (Oracle) através de reimpressões - estável
    # onde `id` (por impressão) não é. Necessário na Silver pra cruzar com
    # Bronze migrations e resolver a cadeia de merge/delete de scryfall_id.
    StructField("oracle_id", StringType(), True)
])


def _face_fallback(card, key):
    # Cards de dupla face (DFC) não têm mana_cost/oracle_text/artist/power/
    # toughness/image_uris no nível raiz - só dentro de card_faces[0] (frente).
    # `is not None` em vez de `or`: colors:[] no nível raiz é válido (incolor),
    # não "ausente".
    value = card.get(key)
    if value is not None:
        return value
    faces = card.get("card_faces")
    return faces[0].get(key) if faces else None


def _to_card_record(card):
    # Landing zone só captura o dado bruto e filtra por coleção - sem
    # tratamento/coerção de negócio. json.dumps serializa os campos compostos
    # (list/dict, incluindo o dict `legalities`) pois colunas StringType do
    # Parquet não guardam estrutura aninhada.
    image_uris = _face_fallback(card, "image_uris")
    colors = _face_fallback(card, "colors")
    color_identity = card.get("color_identity")
    legalities = card.get("legalities")
    return {
        "name": card.get("name"),
        "manaCost": _face_fallback(card, "mana_cost"),
        "cmc": as_float(card.get("cmc")),
        "colors": json.dumps(colors) if colors is not None else None,
        "colorIdentity": json.dumps(color_identity) if color_identity is not None else None,
        "type": card.get("type_line"),
        # sem equivalente na Scryfall (campos legados da magicthegathering.io)
        "types": None,
        "subtypes": None,
        "rarity": card.get("rarity"),
        "set": card.get("set"),
        "setName": card.get("set_name"),
        "text": _face_fallback(card, "oracle_text"),
        "artist": _face_fallback(card, "artist"),
        "number": card.get("collector_number"),
        "power": _face_fallback(card, "power"),
        "toughness": _face_fallback(card, "toughness"),
        "layout": card.get("layout"),
        "multiverseid": None,
        "imageUrl": image_uris.get("normal") if image_uris else None,
        "variations": None,
        "foreignNames": None,
        "printings": None,
        "originalText": None,
        "originalType": None,
        "legalities": json.dumps(legalities) if legalities is not None else None,
        "id": card.get("id"),
        # oracle_id é sempre raiz (mesmo em DFC - identifica a carta, não a
        # face), então sem _face_fallback aqui.
        "oracle_id": card.get("oracle_id"),
    }


def fetch_cards_by_sets(valid_set_codes):
    # 1 request pro índice do Bulk Data + 1 pro catálogo inteiro, filtrado em
    # memória. http_get_with_retry (ingestion_utils.py) dá retry/backoff em
    # 429/5xx/timeout nos dois.
    resp = http_get_with_retry(f"{SCRYFALL_API_URL}/bulk-data", headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
    entry = next(e for e in resp.json()["data"] if e["type"] == SCRYFALL_BULK_TYPE)

    raw = http_get_with_retry(entry["jsonl_download_uri"], headers=SCRYFALL_HEADERS, timeout=120, retries=MAX_RETRIES).content
    # get_scryfall_set_codes_since já devolve códigos em minúsculas e o campo
    # `set` das cartas também é minúsculo na Scryfall - comparação direta,
    # sem normalizar.
    valid_codes = set(valid_set_codes)
    records = []
    for line in gzip.decompress(raw).decode("utf-8").splitlines():
        if not line.strip():
            continue
        card = json.loads(line)
        if card.get("set") in valid_codes:
            records.append(_to_card_record(card))
    return records


def ingest_cards_by_collection(set_codes, table_name="cards", run=None):
    print(f"Baixando catálogo Scryfall ({SCRYFALL_BULK_TYPE}) e filtrando por {len(set_codes)} coleções...")

    all_data = fetch_cards_by_sets(set_codes)
    print(f"Cards encontrados nas coleções da janela temporal: {len(all_data)}")

    if not all_data:
        print(f"Nenhum dado válido para {table_name}")
        return None

    df = save_to_parquet(spark, all_data, table_name, S3_BASE_PATH, schema=CARDS_SCHEMA, run=run)

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

# Coleções (sets) lançadas dentro da janela de YEARS_BACK anos - via Scryfall
# /sets (get_scryfall_set_codes_since), não mais a magicthegathering.io.
set_codes = get_scryfall_set_codes_since(SCRYFALL_API_URL, SCRYFALL_HEADERS, CUTOFF_DATE_STR, retries=MAX_RETRIES)

# Controle de execução (run_id, status, contagens) via run_stage_ingestion -
# padroniza o wrapper start_run -> try/ingest -> finish_run - ver ingestion_utils.py
cards_df, run = run_stage_ingestion(
    "cards", "bulk-data/default_cards",
    lambda run: ingest_cards_by_collection(set_codes, table_name="cards", run=run),
    S3_BASE_PATH,
    params={"years_back": YEARS_BACK, "set_count": len(set_codes)},
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE CARDS")
print("=" * 50)

if cards_df is not None:
    print("✅ Arquivos salvos com sucesso")
    print(f"📊 Total de registros: {cards_df.count()}")
    print(f"🗂️ Coleções processadas: {len(set_codes)} (últimos {YEARS_BACK} anos)")
    print("🎯 Particionamento: por ingestion_timestamp (ano/mês/dia da execução)")
else:
    print("❌ Falha na ingestão de cards")

print("=" * 50)
