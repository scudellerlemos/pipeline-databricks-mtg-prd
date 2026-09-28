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
# FUNÇÕES COMPARTILHADAS (obter_segredo/configurar_armazenamento_s3/obter_http_com_retentativa/
# salvar_em_parquet/iniciar_execucao/finalizar_execucao vivem em ingestion_utils.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./ingestion_utils

# COMMAND ----------

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

# Sem filtro temporal: migrations é o histórico de merge/delete de scryfall_id,
# e Bronze/Silver podem precisar resolver IDs antigos.
URL_API_SCRYFALL = obter_segredo("scryfall_api_url")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))

# Configurações do S3
BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE MIGRATIONS")
print("=" * 60)
print("CAMINHO_S3_STAGE: [CONFIGURADO]")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE MIGRATIONS
# =============================================================================

ESQUEMA_MIGRACOES = StructType(
    [
        StructField("id", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("performed_at", StringType(), True),
        StructField("migration_strategy", StringType(), True),
        StructField("old_scryfall_id", StringType(), True),
        StructField("new_scryfall_id", StringType(), True),  # só presente em "merge"
        StructField("note", StringType(), True),
        # metadata.* achatado em colunas
        StructField("metadata_id", StringType(), True),
        StructField("metadata_lang", StringType(), True),
        StructField("metadata_name", StringType(), True),
        StructField("metadata_set_code", StringType(), True),
        StructField("metadata_oracle_id", StringType(), True),
        StructField("metadata_collector_number", StringType(), True),
    ]
)

def _para_registro_migracao(m):
    metadados = m.get("metadata") or {}
    return {
        "id": m.get("id"),
        "uri": m.get("uri"),
        "performed_at": m.get("performed_at"),
        "migration_strategy": m.get("migration_strategy"),
        "old_scryfall_id": m.get("old_scryfall_id"),
        "new_scryfall_id": m.get("new_scryfall_id"),
        "note": m.get("note"),
        "metadata_id": metadados.get("id"),
        "metadata_lang": metadados.get("lang"),
        "metadata_name": metadados.get("name"),
        "metadata_set_code": metadados.get("set_code"),
        "metadata_oracle_id": metadados.get("oracle_id"),
        "metadata_collector_number": metadados.get("collector_number"),
    }

def buscar_todas_migracoes():
    # Endpoint paginado: segue next_page até has_more=false.
    registros = []
    url = f"{URL_API_SCRYFALL}/migrations"
    while url:
        resposta = obter_http_com_retentativa(url, cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
        corpo = resposta.json()
        registros.extend(_para_registro_migracao(m) for m in corpo["data"])
        url = corpo.get("next_page") if corpo.get("has_more") else None
    return registros

def ingerir_migracoes(execucao=None):
    print("Iniciando ingestão simples: migrations")

    dados_tabela = buscar_todas_migracoes()
    print(f"Migrations obtidas da Scryfall: {len(dados_tabela)}")

    df = salvar_em_parquet(
        spark, dados_tabela, "migrations", CAMINHO_S3_STAGE,
        esquema=ESQUEMA_MIGRACOES,
        # new_scryfall_id fica de fora: vem nulo em migração "delete".
        colunas_obrigatorias=["id", "performed_at", "migration_strategy", "old_scryfall_id"],
        execucao=execucao,
    )

    if df is not None:
        total = df.count()
        print(f"migrations: {total} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
sucesso_configuracao = configurar_armazenamento_s3(CAMINHO_S3_STAGE)
if not sucesso_configuracao:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de migrations

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_migracoes, execucao = executar_ingestao_stage("migrations", "migrations", ingerir_migracoes, CAMINHO_S3_STAGE)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE MIGRATIONS")
print("=" * 50)

if df_migracoes is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de migrations")

print("=" * 50)
