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
# FUNÇÕES COMPARTILHADAS (obter_segredo/configurar_armazenamento_s3/salvar_em_parquet/
# obter_http_com_retentativa/iniciar_execucao/finalizar_execucao vivem em ingestion_utils.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./ingestion_utils

# COMMAND ----------

# =============================================================================
# VARIÁVEIS DE CONFIGURAÇÃO
# =============================================================================
BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"
URL_API_SCRYFALL = obter_segredo("scryfall_api_url")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))
# rulings = 1 objeto por ruling, ligado à carta por oracle_id (não por impressão).
TIPO_BULK_SCRYFALL = "rulings"

# Sem filtro years_back: ruling antiga continua válida, e o catálogo é pequeno
# (~79k linhas, ~5MB comprimido).
print("Sem filtro temporal - captura o catálogo de rulings inteiro")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE RULINGS
# =============================================================================
ESQUEMA_ESCLARECIMENTOS = StructType([
    StructField("oracle_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("comment", StringType(), True),
])


def _para_registro_esclarecimento(esclarecimento):
    # Grava como a Scryfall devolve; o join com cards (1 oracle_id -> N
    # impressões) fica na Gold.
    return {
        "oracle_id": esclarecimento.get("oracle_id"),
        "source": esclarecimento.get("source"),
        "published_at": esclarecimento.get("published_at"),
        "comment": esclarecimento.get("comment"),
    }


def buscar_registros_esclarecimentos():
    # 1 request pro índice do Bulk Data + 1 pro catálogo inteiro.
    resposta = obter_http_com_retentativa(f"{URL_API_SCRYFALL}/bulk-data", cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
    entrada = next(e for e in resposta.json()["data"] if e["type"] == TIPO_BULK_SCRYFALL)

    bruto = obter_http_com_retentativa(entrada["jsonl_download_uri"], cabecalhos=CABECALHOS_SCRYFALL, tempo_limite=120, tentativas=MAX_TENTATIVAS).content
    return [
        _para_registro_esclarecimento(json.loads(linha))
        for linha in gzip.decompress(bruto).decode("utf-8").splitlines()
        if linha.strip()
    ]


def ingerir_esclarecimentos(nome_tabela="rulings", execucao=None):
    print("Baixando catálogo de rulings Scryfall...")

    dados_tabela = buscar_registros_esclarecimentos()
    print(f"Rulings obtidas do catálogo: {len(dados_tabela)}")

    if not dados_tabela:
        print(f"Nenhum dado válido para {nome_tabela}")
        return None

    df = salvar_em_parquet(
        spark, dados_tabela, nome_tabela, CAMINHO_S3_STAGE,
        esquema=ESQUEMA_ESCLARECIMENTOS,
        execucao=execucao,
    )

    if df is not None:
        total = df.count()
        print(f"{nome_tabela}: {total} registros processados")
        return df
    return None

# COMMAND ----------

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

# Configurar S3 Storage
sucesso_configuracao = configurar_armazenamento_s3(CAMINHO_S3_STAGE)
if not sucesso_configuracao:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_esclarecimentos, execucao = executar_ingestao_stage(
    "rulings", "bulk-data/rulings",
    lambda execucao: ingerir_esclarecimentos(nome_tabela="rulings", execucao=execucao),
    CAMINHO_S3_STAGE,
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE RULINGS")
print("=" * 50)

if df_esclarecimentos is not None:
    print("Arquivos salvos")
else:
    print("Falha na ingestão de rulings")

print("=" * 50)
