# Databricks notebook source
# Ingestão de Symbology - Magic: The Gathering (Scryfall)
# Objetivo: Ingerir o catálogo de símbolos de carta/mana via Scryfall API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, sem filtro temporal (catálogo de referência estático), incremental por idempotência de arquivo

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import json
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

# GET /symbology devolve o catálogo inteiro de símbolos (mana, tap, etc.) em 1
# request só (has_more=false), mesmo padrão de sets.ipynb. É um catálogo de
# referência - a Scryfall não expõe data de alteração por símbolo, então não
# há filtro temporal (years_back/cutoff): idempotência é só por arquivo do dia.
SCRYFALL_API_URL = get_secret("scryfall_api_url")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
MAX_RETRIES = int(get_secret("max_retries", "3"))

# Configurações do S3
S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BASE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE SYMBOLOGY")
print("=" * 60)
print("S3_BASE_PATH: [CONFIGURADO]")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE SYMBOLOGY
# =============================================================================

SYMBOLOGY_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("svg_uri", StringType(), True),
        StructField("loose_variant", StringType(), True),
        StructField("english", StringType(), True),
        StructField("transposable", BooleanType(), True),
        StructField("represents_mana", BooleanType(), True),
        StructField("appears_in_mana_costs", BooleanType(), True),
        StructField("mana_value", DoubleType(), True),
        StructField("hybrid", BooleanType(), True),
        StructField("phyrexian", BooleanType(), True),
        StructField("cmc", DoubleType(), True),
        StructField("funny", BooleanType(), True),
        StructField("colors", StringType(), True),  # lista original como JSON
        StructField("gatherer_alternates", StringType(), True),  # lista original como JSON
    ]
)

def _to_symbol_record(s):
    return {
        "symbol": s.get("symbol"),
        "svg_uri": s.get("svg_uri"),
        "loose_variant": s.get("loose_variant"),
        "english": s.get("english"),
        "transposable": s.get("transposable"),
        "represents_mana": s.get("represents_mana"),
        "appears_in_mana_costs": s.get("appears_in_mana_costs"),
        "mana_value": as_float(s.get("mana_value")),
        "hybrid": s.get("hybrid"),
        "phyrexian": s.get("phyrexian"),
        "cmc": as_float(s.get("cmc")),
        "funny": s.get("funny"),
        # colors/gatherer_alternates são listas (ou null) na Scryfall - mesmo
        # tratamento de booster em sets.ipynb: serializa como JSON pra caber
        # numa coluna StringType sem perder a estrutura original.
        "colors": json.dumps(s.get("colors")) if s.get("colors") is not None else None,
        "gatherer_alternates": json.dumps(s.get("gatherer_alternates")) if s.get("gatherer_alternates") is not None else None,
    }

def fetch_all_symbols():
    # GET /symbology documenta has_more=false (catálogo inteiro em 1 request),
    # mas segue next_page defensivamente - mesmo padrão de fetch_all_migrations()
    # em migrations.ipynb, caso a Scryfall passe a paginar esse endpoint.
    records = []
    url = f"{SCRYFALL_API_URL}/symbology"
    while url:
        resp = http_get_with_retry(url, headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
        body = resp.json()
        records.extend(_to_symbol_record(s) for s in body["data"])
        url = body.get("next_page") if body.get("has_more") else None
    return records

def ingest_symbology(run=None):
    print("Iniciando ingestão simples: symbology")

    table_data = fetch_all_symbols()
    print(f"Símbolos obtidos da Scryfall: {len(table_data)}")

    df = save_to_parquet(
        spark, table_data, "symbology", S3_BASE_PATH,
        schema=SYMBOLOGY_SCHEMA,
        run=run,
    )

    if df is not None:
        count = df.count()
        print(f"symbology: {count} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
setup_success = setup_s3_storage(S3_BASE_PATH)
if not setup_success:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de symbology

# Controle de execução (run_id, status, contagens) via run_stage_ingestion -
# padroniza o wrapper start_run -> try/ingest -> finish_run - ver ingestion_utils.py
symbology_df, run = run_stage_ingestion("symbology", "symbology", ingest_symbology, S3_BASE_PATH)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE SYMBOLOGY")
print("=" * 50)

if symbology_df is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de symbology")

print("=" * 50)
