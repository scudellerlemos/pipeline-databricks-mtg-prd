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
# FUNÇÕES COMPARTILHADAS (obter_segredo/configurar_armazenamento_s3/obter_http_com_retentativa/
# salvar_em_parquet/iniciar_execucao/finalizar_execucao vivem em ingestion_utils.py)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./ingestion_utils

# COMMAND ----------

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

# Catálogo de símbolos (mana, tap, etc.) sem data de alteração, então sem
# filtro temporal: 1 arquivo por dia.
URL_API_SCRYFALL = obter_segredo("scryfall_api_url")
CABECALHOS_SCRYFALL = {"User-Agent": "MTGPipeline/1.0"}
MAX_TENTATIVAS = int(obter_segredo("max_retries", "3"))

# Configurações do S3
BUCKET_S3 = obter_segredo("s3_bucket")
PREFIXO_S3_STAGE = obter_segredo("s3_stage_prefix", "stage")
CAMINHO_S3_STAGE = f"s3://{BUCKET_S3}/{PREFIXO_S3_STAGE}"

# Log das configurações
print("=" * 60)
print("CONFIGURAÇÕES PARA INGESTÃO DE SYMBOLOGY")
print("=" * 60)
print("CAMINHO_S3_STAGE: [CONFIGURADO]")
print("=" * 60)

# COMMAND ----------

# =============================================================================
# FUNÇÕES ESPECÍFICAS DE SYMBOLOGY
# =============================================================================

ESQUEMA_SIMBOLOS = StructType(
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

def _para_registro_simbolo(s):
    return {
        "symbol": s.get("symbol"),
        "svg_uri": s.get("svg_uri"),
        "loose_variant": s.get("loose_variant"),
        "english": s.get("english"),
        "transposable": s.get("transposable"),
        "represents_mana": s.get("represents_mana"),
        "appears_in_mana_costs": s.get("appears_in_mana_costs"),
        "mana_value": como_float(s.get("mana_value")),
        "hybrid": s.get("hybrid"),
        "phyrexian": s.get("phyrexian"),
        "cmc": como_float(s.get("cmc")),
        "funny": s.get("funny"),
        # Listas viram JSON (coluna StringType); null continua None.
        "colors": json.dumps(s.get("colors")) if s.get("colors") is not None else None,
        "gatherer_alternates": json.dumps(s.get("gatherer_alternates")) if s.get("gatherer_alternates") is not None else None,
    }

def buscar_todos_simbolos():
    # /symbology devolve tudo em 1 request hoje; segue next_page caso passe a paginar.
    registros = []
    url = f"{URL_API_SCRYFALL}/symbology"
    while url:
        resposta = obter_http_com_retentativa(url, cabecalhos=CABECALHOS_SCRYFALL, tentativas=MAX_TENTATIVAS)
        corpo = resposta.json()
        registros.extend(_para_registro_simbolo(s) for s in corpo["data"])
        url = corpo.get("next_page") if corpo.get("has_more") else None
    return registros

def ingerir_simbolos(execucao=None):
    print("Iniciando ingestão simples: symbology")

    dados_tabela = buscar_todos_simbolos()
    print(f"Símbolos obtidos da Scryfall: {len(dados_tabela)}")

    df = salvar_em_parquet(
        spark, dados_tabela, "symbology", CAMINHO_S3_STAGE,
        esquema=ESQUEMA_SIMBOLOS,
        execucao=execucao,
    )

    if df is not None:
        total = df.count()
        print(f"symbology: {total} registros processados")
        display(df.limit(5))
    return df

# Configurar S3 Storage
sucesso_configuracao = configurar_armazenamento_s3(CAMINHO_S3_STAGE)
if not sucesso_configuracao:
    raise Exception("Falha ao configurar S3 storage")

print("Setup concluído com sucesso")

# COMMAND ----------

# Iniciar ingestão de symbology

# Executa com controle de execução (ver executar_ingestao_stage em ingestion_utils.py)
df_simbolos, execucao = executar_ingestao_stage("symbology", "symbology", ingerir_simbolos, CAMINHO_S3_STAGE)

# Gerar relatório
print("=" * 50)
print("RELATÓRIO DE INGESTÃO DE SYMBOLOGY")
print("=" * 50)

if df_simbolos is not None:
    print("Arquivos salvos")

else:
    print("Falha na ingestão de symbology")

print("=" * 50)
