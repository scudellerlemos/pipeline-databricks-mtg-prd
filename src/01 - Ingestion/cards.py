# Databricks notebook source
# Ingestão de Cards - Magic: The Gathering (Bulk Data)
# Objetivo: Ingerir dados de cards via Scryfall Bulk Data API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, snapshot, por coleção (set)

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import json
import gzip
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

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
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))

# Baixa o catálogo inteiro da Scryfall (bulk data) e filtra em memória pelos
# codigos_colecoes da janela temporal.
URL_API_SCRYFALL = obter_segredo("scryfall_api_url")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
# default_cards = 1 objeto por impressão (set/artist/number/imageUrl variam
# por edição). Mesmo bulk de card_prices.py.
TIPO_BULK_SCRYFALL = "default_cards"

# Configurações do S3
BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"

# Configurações de janela temporal (por coleção: só ingere sets lançados nos últimos ANOS_RETROATIVOS anos)
ANOS_RETROATIVOS = int(obter_segredo("years_back", "5"))
ano_atual = datetime.now().year
ano_corte = ano_atual - ANOS_RETROATIVOS
DATA_CORTE = datetime(ano_corte, 1, 1)
DATA_CORTE_TEXTO = DATA_CORTE.strftime("%Y-%m-%d")

print(f"ANOS_RETROATIVOS: {ANOS_RETROATIVOS} | DATA_CORTE_TEXTO: {DATA_CORTE_TEXTO}")

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE CARDS
# =============================================================================
ESQUEMA_CARTAS = StructType([
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
    # oracle_id identifica a carta entre reimpressões (`id` é por impressão).
    # Usado na Gold para ligar a carta às rulings.
    StructField("oracle_id", StringType(), True)
])


def _valor_da_face(carta, chave):
    # Cartas de dupla face (DFC) trazem alguns campos só em card_faces[0] (frente).
    # Usa `is not None` porque colors:[] na raiz é válido (incolor).
    valor = carta.get(chave)
    if valor is not None:
        return valor
    faces = carta.get("card_faces")
    return faces[0].get(chave) if faces else None


def _para_registro_carta(carta):
    # Dado bruto, sem regra de negócio. Listas/dicts viram JSON porque as
    # colunas são StringType.
    uris_imagem = _valor_da_face(carta, "image_uris")
    cores = _valor_da_face(carta, "colors")
    identidade_cor = carta.get("color_identity")
    legalidades = carta.get("legalities")
    return {
        "name": carta.get("name"),
        "manaCost": _valor_da_face(carta, "mana_cost"),
        "cmc": como_float(carta.get("cmc")),
        "colors": json.dumps(cores) if cores is not None else None,
        "colorIdentity": json.dumps(identidade_cor) if identidade_cor is not None else None,
        # reversible_card não tem type_line na raiz, só nas faces
        "type": _valor_da_face(carta, "type_line"),
        # sem equivalente na Scryfall (campos legados da magicthegathering.io)
        "types": None,
        "subtypes": None,
        "rarity": carta.get("rarity"),
        "set": carta.get("set"),
        "setName": carta.get("set_name"),
        "text": _valor_da_face(carta, "oracle_text"),
        "artist": _valor_da_face(carta, "artist"),
        "number": carta.get("collector_number"),
        "power": _valor_da_face(carta, "power"),
        "toughness": _valor_da_face(carta, "toughness"),
        "layout": carta.get("layout"),
        # multiverse_ids (lista) da Scryfall nao e mapeado
        "multiverseid": None,
        "imageUrl": uris_imagem.get("normal") if uris_imagem else None,
        "variations": None,
        "foreignNames": None,
        "printings": None,
        "originalText": None,
        "originalType": None,
        "legalities": json.dumps(legalidades) if legalidades is not None else None,
        "id": carta.get("id"),
        # oracle_id fica na raiz, exceto em reversible_card, que só traz em card_faces.
        "oracle_id": _valor_da_face(carta, "oracle_id"),
    }


def buscar_cartas_por_colecoes(codigos_colecoes_validos):
    # 1 request pro índice do Bulk Data + 1 pro catálogo inteiro, filtrado em memória.
    resposta = obter_http_com_retentativa(f"{URL_API_SCRYFALL}/bulk-data", cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
    entrada = next(e for e in resposta.json()["data"] if e["type"] == TIPO_BULK_SCRYFALL)

    bruto = obter_http_com_retentativa(entrada["jsonl_download_uri"], cabecalhos=CABECALHOS_SCRYFALL, tempo_limite=120, tentativas=MAX_TENTATIVAS).content
    # Os dois lados já vêm em minúsculas (codigos_colecoes e campo `set` da Scryfall).
    codigos_validos = set(codigos_colecoes_validos)
    registros = []
    for linha in gzip.decompress(bruto).decode("utf-8").splitlines():
        if not linha.strip():
            continue
        carta = json.loads(linha)
        if carta.get("set") in codigos_validos:
            registros.append(_para_registro_carta(carta))
    return registros


def ingerir_cartas_por_colecao(codigos_colecoes, nome_tabela="cards", execucao=None):
    print(f"Baixando catálogo Scryfall ({TIPO_BULK_SCRYFALL}) e filtrando por {len(codigos_colecoes)} coleções...")

    dados_tabela = buscar_cartas_por_colecoes(codigos_colecoes)
    print(f"Cards encontrados nas coleções da janela temporal: {len(dados_tabela)}")

    if not dados_tabela:
        print(f"Nenhum dado válido para {nome_tabela}")
        return None

    df = salvar_em_parquet(spark, dados_tabela, nome_tabela, CAMINHO_S3_STAGE, esquema=ESQUEMA_CARTAS, execucao=execucao,
                           colunas_obrigatorias=["id", "oracle_id", "name", "set", "rarity", "type"])

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

# Coleções (sets) lançadas dentro da janela de ANOS_RETROATIVOS anos
codigos_colecoes = obter_codigos_colecoes_scryfall_desde(URL_API_SCRYFALL, CABECALHOS_SCRYFALL, DATA_CORTE_TEXTO, tentativas=MAX_TENTATIVAS)

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_cartas, execucao = executar_ingestao_stage(
    "cards", "bulk-data/default_cards",
    lambda execucao: ingerir_cartas_por_colecao(codigos_colecoes, nome_tabela="cards", execucao=execucao),
    CAMINHO_S3_STAGE,
    parametros={"years_back": ANOS_RETROATIVOS, "set_count": len(codigos_colecoes)},
)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE CARDS")
print("=" * 50)

if df_cartas is not None:
    print("Arquivos salvos com sucesso")
    print(f"Total de registros: {df_cartas.count()}")
    print(f"Coleções processadas: {len(codigos_colecoes)} (últimos {ANOS_RETROATIVOS} anos)")
    print("Particionamento: por ingestion_timestamp (ano/mês/dia da execução)")
else:
    print("Falha na ingestão de cards")

print("=" * 50)
