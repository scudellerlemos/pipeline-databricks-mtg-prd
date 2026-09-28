# Databricks notebook source
# =============================================================================
# CAMADA SILVER - PRECOS DE CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
TB_FATO_PRECOS_CARTAS: historico de precos, Bronze `card_prices` -> Silver.

Fato: uma linha por impressao de carta por coleta. As variantes (foil,
etched, EUR, TIX) sao colunas da mesma linha. Preco varia por impressao, nao
por nome, por isso o grao e ID_CARTA; join com TB_FATO_CARTAS e N:1, na Gold.

Chave unica: ID_CARTA + DT_INGESTAO. A Bronze e append-only, entao cada run
acrescenta a cotacao do dia e o historico se acumula aqui.

Mesma convencao de nome/case de TB_FATO_CARTAS.
"""

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import logging

# =============================================================================
# CARREGAMENTO DO MODULO UTILITARIO
# =============================================================================
# Importar infraestrutura comum e funcoes do silver_utils usando %run (Databricks)

# COMMAND ----------

# MAGIC %run "../../00 - Common/Dev/base_utils"

# COMMAND ----------

# MAGIC %run ./silver_utils

# COMMAND ----------

# MAGIC %run ./silver_column_docs

# COMMAND ----------

# =============================================================================
# CONFIGURACAO INICIAL
# =============================================================================
def configurar_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def transformar_precos_cartas_silver(df):
    """
    Transformacao especifica para tabela Precos de Cartas, via SQL
    (spark.sql sobre temp views).
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Precos de Cartas...")

    df.createOrReplaceTempView("_prices_bronze")

    # Preco chega como string da Bronze. NULL = sem cotacao (nao vira 0).
    # ANO/MES_INGESTAO vem da data da coleta.
    df_final = spark.sql("""
        SELECT
            id AS ID_CARTA,
            name AS NME_CARTA,
            upper(`set`) AS COD_COLECAO,
            rarity AS NME_RARIDADE,
            cast(usd AS float) AS VLR_USD,
            cast(usd_foil AS float) AS VLR_USD_FOIL,
            cast(usd_etched AS float) AS VLR_USD_ETCHED,
            cast(eur AS float) AS VLR_EUR,
            cast(eur_foil AS float) AS VLR_EUR_FOIL,
            cast(tix AS float) AS VLR_TIX,
            scryfall_uri AS URL_SCRYFALL,
            image_url AS URL_IMAGEM,
            to_date(releaseDate) AS DT_LANCAMENTO,
            to_timestamp(ingestion_timestamp) AS DT_INGESTAO,
            coalesce(nullif(trim(source), ''), 'NA') AS NME_FONTE,
            endpoint AS DESC_URL_ORIGEM,
            source_file AS DESC_ARQUIVO_ORIGEM,
            bronze_run_id AS ID_EXECUCAO_BRONZE,
            bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE,
            year(to_timestamp(ingestion_timestamp)) AS ANO_INGESTAO,
            month(to_timestamp(ingestion_timestamp)) AS MES_INGESTAO
        FROM _prices_bronze
    """)

    df_final = normalizar_valores(df_final, ["NME_CARTA", "NME_RARIDADE", "NME_FONTE"])

    return df_final

# =============================================================================
# CONFIGURACAO
# =============================================================================

config = criar_config_manual(obter_segredo("catalog_name"), obter_segredo("s3_bucket"))

configurar_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
processador = SilverTableProcessor("TB_FATO_PRECOS_CARTAS", config)

df_bronze = processador.extrair_da_bronze("card_prices")

df_silver = processador.transformar_dados(df_bronze, transformar_precos_cartas_silver)

# Sem coluna_ordenacao: DT_INGESTAO ja faz parte da chave, nao desempata nada.
processador.salvar_tabela_silver(
    df_silver,
    colunas_particao=["ANO_INGESTAO", "MES_INGESTAO"],
    coluna_chave=["ID_CARTA", "DT_INGESTAO"],
    comentario_tabela=obter_comentario_tabela("TB_FATO_PRECOS_CARTAS"),
    comentarios_colunas=obter_comentarios_colunas("TB_FATO_PRECOS_CARTAS")
)

# =============================================================================
# VALIDACAO E LOGS
# =============================================================================
if df_silver:
    print(f"Processamento concluido com sucesso!")
    print(f"Registros processados: {df_silver.count()}")
    print(f"Colunas finais: {df_silver.columns}")
else:
    print("Falha no processamento - DataFrame vazio")
