# Databricks notebook source
# Ingestão de Sets - Magic: The Gathering (Scryfall)
# Objetivo: Ingerir dados de sets via Scryfall API para staging em Parquet no S3
# Características: Dados brutos, formato Parquet, filtro temporal, particionamento, snapshot, tratamento de campos complexos

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
from datetime import datetime
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

URL_API_SCRYFALL = obter_segredo("scryfall_api_url")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))

# Configurações do S3
BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"

# Configurações de período
ANOS_RETROATIVOS = int(obter_segredo("years_back", "5"))
ano_atual = datetime.now().year
ano_corte = ano_atual - ANOS_RETROATIVOS
DATA_CORTE = datetime(ano_corte, 1, 1)
DATA_CORTE_TEXTO = DATA_CORTE.strftime("%Y-%m-%d")

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE SETS")
print("=" * 60)
print("CAMINHO_S3_STAGE: [CONFIGURADO]")
print(f"ANOS_RETROATIVOS: {ANOS_RETROATIVOS}")
print(f"DATA_CORTE_TEXTO: {DATA_CORTE_TEXTO}")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE SETS
# =============================================================================

ESQUEMA_COLECOES = StructType(
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
# None (`source` é preenchido com 'scryfall' pelo salvar_em_parquet). Ficam no
# schema porque a Silver (TB_DIM_COLECOES) lê essas colunas (ver README -
# "Imutabilidade").
_CAMPOS_SEM_EQUIVALENTE_SCRYFALL = (
    'border', 'mkm_id', 'mkm_name', 'gathererCode', 'magicCardsInfoCode',
    'oldCode', 'source', 'booster',
)

def limpar_dados_colecoes(dados):
    dados_limpos = []
    for item in dados:
        if isinstance(item, dict):
            item_limpo = {}

            # Mapear campos conhecidos com tipos seguros
            mapeamento_campos = {
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
            for campo, tipo_campo in mapeamento_campos.items():
                if campo in item:
                    try:
                        if item[campo] is not None:
                            item_limpo[campo] = tipo_campo(item[campo])
                        else:
                            item_limpo[campo] = None
                    except (ValueError, TypeError):
                        item_limpo[campo] = str(item[campo]) if item[campo] is not None else None
                else:
                    item_limpo[campo] = None

            for campo in _CAMPOS_SEM_EQUIVALENTE_SCRYFALL:
                item_limpo[campo] = None

            dados_limpos.append(item_limpo)

    return dados_limpos

def _para_registro_colecao(s):
    # Campos legados sem equivalente na Scryfall são preenchidos com None em
    # limpar_dados_colecoes.
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

def buscar_todas_colecoes():
    # /sets devolve tudo em 1 request hoje; segue next_page caso passe a paginar.
    registros = []
    url = f"{URL_API_SCRYFALL}/sets"
    while url:
        resposta = obter_http_com_retentativa(url, cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
        corpo = resposta.json()
        registros.extend(_para_registro_colecao(s) for s in corpo["data"])
        url = corpo.get("next_page") if corpo.get("has_more") else None
    return registros

def ingerir_colecoes(execucao=None):
    print("Iniciando ingestão simples: sets")

    dados_tabela = buscar_todas_colecoes()
    print(f"Sets obtidos da Scryfall: {len(dados_tabela)}")

    print("Limpando dados de sets...")
    dados_tabela = limpar_dados_colecoes(dados_tabela)

    df = salvar_em_parquet(
        spark, dados_tabela, "sets", CAMINHO_S3_STAGE,
        esquema=ESQUEMA_COLECOES,
        colunas_obrigatorias=["code", "name", "releaseDate"],
        coluna_origem_particao="releaseDate",
        data_corte=DATA_CORTE_TEXTO,
        execucao=execucao,
    )

    if df is not None:
        total = df.count()
        print(f"sets: {total} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
sucesso_configuracao = configurar_armazenamento_s3(CAMINHO_S3_STAGE)
if not sucesso_configuracao:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de sets

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_colecoes, execucao = executar_ingestao_stage("sets", "sets", ingerir_colecoes, CAMINHO_S3_STAGE)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE SETS")
print("=" * 50)

if df_colecoes is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de sets")

print("=" * 50)
