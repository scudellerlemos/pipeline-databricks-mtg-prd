# Databricks notebook source
# Camada Bronze - Symbology - Magic: The Gathering
# EL da Stage (S3/Parquet) para Bronze (Delta): so APPEND, dado 1:1 +
# metadados tecnicos. Regras da camada em bronze_utils.py.
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
NOME_CATALOGO = obter_segredo("catalog_name")
NOME_ESQUEMA = "bronze"
NOME_TABELA_BRONZE = "symbology"
NOME_TABELA_STAGE = "symbology"

BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
PREFIXO_S3_BRONZE = obter_segredo("s3_bronze_prefix", "bronze")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"
CAMINHO_S3_BRONZE = f"s3://{BUCKET_S3}/{PREFIXO_S3_BRONZE}"

configurar_unity_catalog(NOME_CATALOGO, NOME_ESQUEMA)

# COMMAND ----------

# =============================================================================
# EXECUCAO PRINCIPAL
# =============================================================================
df_simbolos_bronze, execucao = executar_ingestao_bronze(
    spark, dbutils, NOME_CATALOGO, NOME_ESQUEMA,
    NOME_TABELA_BRONZE, NOME_TABELA_STAGE,
    CAMINHO_S3_STAGE, CAMINHO_S3_BRONZE,
    comentario_tabela=obter_comentario_tabela(NOME_TABELA_BRONZE),
    comentarios_colunas=obter_comentarios_colunas(NOME_TABELA_BRONZE),
)

print("=" * 50)
print(f"RELATORIO DE INGESTAO BRONZE - SYMBOLOGY")
print("=" * 50)
print(
    f"status={execucao['status']} "
    f"arquivos_processados={execucao['files_processed']} "
    f"registros_lidos={execucao['records_read']} "
    f"registros_gravados={execucao['records_written']}"
)
print("=" * 50)
