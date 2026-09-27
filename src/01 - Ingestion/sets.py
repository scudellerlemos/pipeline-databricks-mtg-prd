# Databricks notebook source
# Ingestão de Sets - Magic: The Gathering (Scryfall)
# Objetivo: Ingerir dados de sets via Scryfall API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, incremental, tratamento de campos complexos

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
from datetime import datetime
from pyspark.sql.types import *

# =============================================================================
# FUNÇÕES COMPARTILHADAS (get_secret/setup_s3_storage/http_get_with_retry/
# save_to_parquet/start_run/finish_run vivem em ingestion_utils.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./ingestion_utils

# COMMAND ----------

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

SCRYFALL_API_URL = get_secret("scryfall_api_url")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
MAX_RETRIES = int(get_secret("max_retries", "3"))

# Configurações do S3
S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BASE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"

# Configurações de período
YEARS_BACK = int(get_secret("years_back", "5"))
current_year = datetime.now().year
cutoff_year = current_year - YEARS_BACK
CUTOFF_DATE = datetime(cutoff_year, 1, 1)
CUTOFF_DATE_STR = CUTOFF_DATE.strftime("%Y-%m-%d")

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE SETS")
print("=" * 60)
print("S3_BASE_PATH: [CONFIGURADO]")
print(f"YEARS_BACK: {YEARS_BACK}")
print(f"CUTOFF_DATE_STR: {CUTOFF_DATE_STR}")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE SETS
# =============================================================================

SETS_SCHEMA = StructType(
    [
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("border", StringType(), True),
        StructField("mkm_id", IntegerType(), True),
        StructField("mkm_name", StringType(), True),
        StructField("releaseDate", StringType(), True),
        StructField("gathererCode", StringType(), True),
        StructField("magicCardsInfoCode", StringType(), True),
        StructField("booster", StringType(), True),  # legado, sempre None
        StructField("oldCode", StringType(), True),
        StructField("onlineOnly", BooleanType(), True),
        StructField("source", StringType(), True),
        # Campos nativos da Scryfall
        StructField("card_count", IntegerType(), True),
        StructField("parent_set_code", StringType(), True),
        StructField("block", StringType(), True),
        StructField("icon_svg_uri", StringType(), True),
    ]
    # booster_0..19: legado da magicthegathering.io, sempre nulas.
    + [StructField(f"booster_{i}", StringType(), True) for i in range(20)]
)

# Campos legados da magicthegathering.io sem equivalente na Scryfall: sempre
# None (`source` é preenchido com 'scryfall' pelo save_to_parquet). Ficam no
# schema porque a Silver (TB_DIM_COLECOES) lê essas colunas (ver README -
# "Imutabilidade").
_FIELDS_SEM_EQUIVALENTE_SCRYFALL = (
    'border', 'mkm_id', 'mkm_name', 'gathererCode', 'magicCardsInfoCode',
    'oldCode', 'source', 'booster',
)

def clean_sets_data(data):
    cleaned_data = []
    for item in data:
        if isinstance(item, dict):
            cleaned_item = {}

            # Mapear campos conhecidos com tipos seguros
            field_mappings = {
                'code': str,
                'name': str,
                'type': str,
                'releaseDate': str,
                'onlineOnly': bool,
                'card_count': int,
                'parent_set_code': str,
                'block': str,
                'icon_svg_uri': str,
            }

            # Processar campos conhecidos
            for field, field_type in field_mappings.items():
                if field in item:
                    try:
                        if item[field] is not None:
                            cleaned_item[field] = field_type(item[field])
                        else:
                            cleaned_item[field] = None
                    except (ValueError, TypeError):
                        cleaned_item[field] = str(item[field]) if item[field] is not None else None
                else:
                    cleaned_item[field] = None

            for field in _FIELDS_SEM_EQUIVALENTE_SCRYFALL:
                cleaned_item[field] = None

            cleaned_data.append(cleaned_item)

    return cleaned_data

def _to_set_record(s):
    # Campos legados sem equivalente na Scryfall são preenchidos com None em
    # clean_sets_data.
    return {
        "code": s.get("code"),
        "name": s.get("name"),
        "type": s.get("set_type"),
        "releaseDate": s.get("released_at"),
        "onlineOnly": s.get("digital"),
        "card_count": s.get("card_count"),
        "parent_set_code": s.get("parent_set_code"),
        "block": s.get("block"),
        "icon_svg_uri": s.get("icon_svg_uri"),
    }

def fetch_all_sets():
    # /sets devolve tudo em 1 request hoje; segue next_page caso passe a paginar.
    records = []
    url = f"{SCRYFALL_API_URL}/sets"
    while url:
        resp = http_get_with_retry(url, headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
        body = resp.json()
        records.extend(_to_set_record(s) for s in body["data"])
        url = body.get("next_page") if body.get("has_more") else None
    return records

def ingest_sets(run=None):
    print("Iniciando ingestão simples: sets")

    table_data = fetch_all_sets()
    print(f"Sets obtidos da Scryfall: {len(table_data)}")

    print("Limpando dados de sets...")
    table_data = clean_sets_data(table_data)

    df = save_to_parquet(
        spark, table_data, "sets", S3_BASE_PATH,
        schema=SETS_SCHEMA,
        partition_source_col="releaseDate",
        cutoff_date_str=CUTOFF_DATE_STR,
        run=run,
    )

    if df is not None:
        count = df.count()
        print(f"sets: {count} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
setup_success = setup_s3_storage(S3_BASE_PATH)
if not setup_success:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de sets

# Executa com controle de execução (ver run_stage_ingestion em ingestion_utils.py)
sets_df, run = run_stage_ingestion("sets", "sets", ingest_sets, S3_BASE_PATH)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE SETS")
print("=" * 50)

if sets_df is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de sets")

print("=" * 50)
