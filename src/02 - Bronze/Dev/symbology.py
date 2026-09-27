# Databricks notebook source
# Camada Bronze - Symbology - Magic: The Gathering
# Objetivo: EL (Extract & Load) da Stage (S3/Parquet) para Bronze (Delta)
# Escopo: sem regra de negocio, sem renomeacao de colunas, sem deduplicacao
# por chave de negocio e sem MERGE/upsert - so APPEND, preservando o dado da
# Stage 1:1 e adicionando apenas metadados tecnicos de rastreabilidade
# (source_file, bronze_run_id, bronze_ingestion_timestamp).
# Catalogo de referencia estatico (simbolos de mana/carta) - mesmo assim,
# sem MERGE: cada execucao e um snapshot append-only do catalogo.

# =============================================================================
# FUNCOES COMPARTILHADAS (ver bronze_utils.py / bronze_column_docs.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run "../../00 - Common/Dev/base_utils"

# COMMAND ----------

# MAGIC %run ./bronze_utils

# COMMAND ----------

# MAGIC %run ./bronze_column_docs

# COMMAND ----------

# =============================================================================
# CONFIGURACAO
# =============================================================================
CATALOG_NAME = get_secret("catalog_name")
SCHEMA_NAME = "bronze"
BRONZE_TABLE_NAME = "symbology"
STAGE_TABLE_NAME = "symbology"

S3_BUCKET = get_secret("s3_bucket")
S3_STAGE_PREFIX = get_secret("s3_stage_prefix", "stage")
S3_BRONZE_PREFIX = get_secret("s3_bronze_prefix", "bronze")
S3_STAGE_PATH = f"s3://{S3_BUCKET}/{S3_STAGE_PREFIX}"
S3_BRONZE_PATH = f"s3://{S3_BUCKET}/{S3_BRONZE_PREFIX}"

setup_unity_catalog(CATALOG_NAME, SCHEMA_NAME)

# COMMAND ----------

# =============================================================================
# EXECUCAO PRINCIPAL
# =============================================================================
symbology_bronze_df, run = run_bronze_ingestion(
    spark, dbutils, CATALOG_NAME, SCHEMA_NAME,
    BRONZE_TABLE_NAME, STAGE_TABLE_NAME,
    S3_STAGE_PATH, S3_BRONZE_PATH,
    table_comment=get_table_comment(BRONZE_TABLE_NAME),
    column_comments=get_column_comments(BRONZE_TABLE_NAME),
)

print("=" * 50)
print(f"RELATORIO DE INGESTAO BRONZE - SYMBOLOGY")
print("=" * 50)
print(
    f"status={run['status']} "
    f"arquivos_processados={run['files_processed']} "
    f"registros_lidos={run['records_read']} "
    f"registros_gravados={run['records_written']}"
)
print("=" * 50)
