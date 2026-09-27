# Databricks notebook source
# Ingestão de Migrations - Magic: The Gathering (Scryfall)
# Objetivo: ingerir o histórico de migrações de ID da Scryfall (merge/delete de
# cartas) para staging em Parquet no S3
# Características: dados brutos, formato Parquet, sem filtro temporal, paginado

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
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

# GET /migrations é o único endpoint de catálogo da Stage que pagina de
# verdade (has_more/next_page) - sets e symbology devolvem tudo em 1 request.
# Sem filtro temporal (YEARS_BACK): migrations é o histórico de reconciliação
# de scryfall_id (merge/delete) referenciado por oracle_id/old_scryfall_id -
# cortar por data quebraria a rastreabilidade de IDs antigos que Bronze/Silver
# ainda podem precisar resolver, mesmo tratando de cartas antigas.
SCRYFALL_API_URL = get_secret("scryfall_api_url")
SCRYFALL_HEADERS = {"User-Agent": "MTGPipeline/1.0"}
MAX_RETRIES = int(get_secret("max_retries", "3"))

# Configurações do S3
S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BASE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE MIGRATIONS")
print("=" * 60)
print("S3_BASE_PATH: [CONFIGURADO]")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE MIGRATIONS
# =============================================================================

MIGRATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("performed_at", StringType(), True),
        StructField("migration_strategy", StringType(), True),
        StructField("old_scryfall_id", StringType(), True),
        StructField("new_scryfall_id", StringType(), True),  # só presente em "merge"
        StructField("note", StringType(), True),
        # metadata.* flattenado - objeto fixo (não é lista, não precisa de JSON)
        StructField("metadata_id", StringType(), True),
        StructField("metadata_lang", StringType(), True),
        StructField("metadata_name", StringType(), True),
        StructField("metadata_set_code", StringType(), True),
        StructField("metadata_oracle_id", StringType(), True),
        StructField("metadata_collector_number", StringType(), True),
    ]
)

def _to_migration_record(m):
    metadata = m.get("metadata") or {}
    return {
        "id": m.get("id"),
        "uri": m.get("uri"),
        "performed_at": m.get("performed_at"),
        "migration_strategy": m.get("migration_strategy"),
        "old_scryfall_id": m.get("old_scryfall_id"),
        "new_scryfall_id": m.get("new_scryfall_id"),
        "note": m.get("note"),
        "metadata_id": metadata.get("id"),
        "metadata_lang": metadata.get("lang"),
        "metadata_name": metadata.get("name"),
        "metadata_set_code": metadata.get("set_code"),
        "metadata_oracle_id": metadata.get("oracle_id"),
        "metadata_collector_number": metadata.get("collector_number"),
    }

def fetch_all_migrations():
    # único endpoint da Stage com paginação real (has_more/next_page) - segue
    # next_page até a Scryfall devolver has_more=false.
    records = []
    url = f"{SCRYFALL_API_URL}/migrations"
    while url:
        resp = http_get_with_retry(url, headers=SCRYFALL_HEADERS, retries=MAX_RETRIES)
        body = resp.json()
        records.extend(_to_migration_record(m) for m in body["data"])
        url = body.get("next_page") if body.get("has_more") else None
    return records

def ingest_migrations(run=None):
    print("Iniciando ingestão simples: migrations")

    table_data = fetch_all_migrations()
    print(f"Migrations obtidas da Scryfall: {len(table_data)}")

    df = save_to_parquet(
        spark, table_data, "migrations", S3_BASE_PATH,
        schema=MIGRATIONS_SCHEMA,
        run=run,
    )

    if df is not None:
        count = df.count()
        print(f"migrations: {count} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
setup_success = setup_s3_storage(S3_BASE_PATH)
if not setup_success:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de migrations

# Controle de execução (run_id, status, contagens) via run_stage_ingestion -
# padroniza o wrapper start_run -> try/ingest -> finish_run - ver ingestion_utils.py
migrations_df, run = run_stage_ingestion("migrations", "migrations", ingest_migrations, S3_BASE_PATH)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE MIGRATIONS")
print("=" * 50)

if migrations_df is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de migrations")

print("=" * 50)
