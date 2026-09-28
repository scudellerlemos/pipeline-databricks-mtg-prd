# Databricks notebook source
# Ingestão de Preços de Cards - Magic: The Gathering
# Objetivo: Ingerir preços das cartas via Scryfall Bulk Data API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, snapshot, idempotente

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import gzip
import json
from datetime import datetime
from pyspark.sql import SparkSession
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
# Scryfall rejeita o User-Agent default do requests (erro "generic_user_agent")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))
# default_cards = 1 objeto por impressão, cada um com seu `prices` (o preço
# varia por impressão). Mesmo bulk de cards.py.
TIPO_BULK_SCRYFALL = "default_cards"

# Janela temporal (years_back): filtra pelo released_at da própria impressão,
# sem depender da execução de cards/sets.
ANOS_RETROATIVOS = int(obter_segredo("years_back", "5"))
ano_atual = datetime.now().year
ano_corte = ano_atual - ANOS_RETROATIVOS
DATA_CORTE_TEXTO = datetime(ano_corte, 1, 1).strftime("%Y-%m-%d")

print(f"ANOS_RETROATIVOS: {ANOS_RETROATIVOS} | DATA_CORTE_TEXTO: {DATA_CORTE_TEXTO}")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE CARD_PRICES
# =============================================================================
ESQUEMA_PRECOS_CARTAS = StructType([
    # id da impressão (mesmo `id` de cards.py). Vira ID_CARTA na Silver, chave
    # do join com TB_FATO_CARTAS feito na Gold.
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("set", StringType(), True),
    StructField("rarity", StringType(), True),
    # Um preco por variante (normal/foil/etched) da mesma impressao.
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


def _para_registro_preco(carta):
    # Grava como a Scryfall devolve; o join com `cards` (por id) fica na Gold.
    precos = carta.get("prices", {}) or {}
    # Dupla face (DFC) não tem image_uris na raiz - usa card_faces[0] (frente).
    faces = carta.get("card_faces") or [{}]
    uris_imagem = carta.get("image_uris") or faces[0].get("image_uris")
    return {
        "id": carta.get("id"),
        "name": carta.get("name"),
        "set": carta.get("set"),
        "rarity": carta.get("rarity"),
        "usd": precos.get("usd"),
        "usd_foil": precos.get("usd_foil"),
        "usd_etched": precos.get("usd_etched"),
        "eur": precos.get("eur"),
        "eur_foil": precos.get("eur_foil"),
        "tix": precos.get("tix"),
        "scryfall_uri": carta.get("scryfall_uri"),
        "image_url": uris_imagem.get("normal") if uris_imagem else None,
        "releaseDate": carta.get("released_at"),
    }


def buscar_registros_precos():
    # 1 request pro índice do Bulk Data + 1 pro catálogo inteiro.
    resposta = obter_http_com_retentativa(f"{URL_API_SCRYFALL}/bulk-data", cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
    entrada = next(e for e in resposta.json()["data"] if e["type"] == TIPO_BULK_SCRYFALL)

    bruto = obter_http_com_retentativa(entrada["jsonl_download_uri"], cabecalhos=CABECALHOS_SCRYFALL, tempo_limite=120, tentativas=MAX_TENTATIVAS).content
    return [
        _para_registro_preco(json.loads(linha))
        for linha in gzip.decompress(bruto).decode("utf-8").splitlines()
        if linha.strip()
    ]


def ingerir_precos_cartas(nome_tabela="card_prices", execucao=None):
    print(f"Baixando catálogo de preços Scryfall ({TIPO_BULK_SCRYFALL})...")

    dados_tabela = buscar_registros_precos()
    print(f"Preços obtidos do catálogo: {len(dados_tabela)}")

    if not dados_tabela:
        print(f"Nenhum dado válido para {nome_tabela}")
        return None

    df = salvar_em_parquet(
        spark, dados_tabela, nome_tabela, CAMINHO_S3_STAGE,
        esquema=ESQUEMA_PRECOS_CARTAS,
        colunas_obrigatorias=["id", "set"],
        coluna_origem_particao="releaseDate", data_corte=DATA_CORTE_TEXTO,
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
sucesso_configuracao = configurar_armazenamento_s3(CAMINHO_S3_STAGE)
if not sucesso_configuracao:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_precos, execucao = executar_ingestao_stage(
    "card_prices", "bulk-data/default_cards",
    lambda execucao: ingerir_precos_cartas(nome_tabela="card_prices", execucao=execucao),
    CAMINHO_S3_STAGE,
    parametros={"years_back": ANOS_RETROATIVOS},
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE PREÇOS")
print("=" * 50)

if df_precos is not None:
    print("Arquivos salvos com sucesso")
    print(f"Total de registros: {df_precos.count()}")
    print(f"Particionamento: por releaseDate (janela de {ANOS_RETROATIVOS} anos)")
else:
    print("Falha na ingestão de preços")

print("=" * 50)
