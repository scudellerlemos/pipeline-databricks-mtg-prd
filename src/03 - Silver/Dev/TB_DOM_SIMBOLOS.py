# Databricks notebook source
# =============================================================================
# CAMADA SILVER - SIMBOLOS DE MANA - MAGIC: THE GATHERING
# =============================================================================
"""
TB_DOM_SIMBOLOS: simbolos de mana/custo da Scryfall, Bronze `symbology` -> Silver.

Dominio: lista pequena e quase estatica, uma linha por simbolo.
Chave unica: COD_SIMBOLO (nunca nulo na fonte), declarada como PRIMARY KEY.

COD_SIMBOLO troca chaves por colchetes ("{2/U}" -> "[2/U]"), a mesma notacao
de DESC_CUSTO_MANA em TB_FATO_CARTAS, para casar com TB_PONTE_CARTA_SIMBOLOS.
COD_SIMBOLO nao passa por normalizar_valor().

Mesma convencao de nome/case de TB_FATO_CARTAS. Sem particionamento: tabela
de poucas dezenas de linhas.
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
def setup_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def transform_symbology_silver(df):
    """
    Transformacao especifica para tabela Simbolos de Mana, via SQL
    (spark.sql sobre temp views).
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Simbolos de Mana...")

    df.createOrReplaceTempView("_symbology_bronze")

    # Renomeia Bronze -> PT-BR, troca chave por colchete em COD_SIMBOLO e
    # limpa o array serializado em COD_CORES/DESC_GRAFIAS_GATHERER.
    df_final = spark.sql(r"""
        SELECT
            regexp_replace(regexp_replace(symbol, '\\{', '['), '\\}', ']') AS COD_SIMBOLO,
            svg_uri AS URL_ICONE,
            coalesce(nullif(trim(loose_variant), ''), 'NA') AS DESC_VARIANTE_LIVRE,
            coalesce(nullif(trim(english), ''), 'NA') AS DESC_SIMBOLO,
            transposable AS FLG_TRANSPONIVEL,
            represents_mana AS FLG_REPRESENTA_MANA,
            appears_in_mana_costs AS FLG_APARECE_CUSTO_MANA,
            mana_value AS QTD_VALOR_MANA,
            hybrid AS FLG_HIBRIDO,
            phyrexian AS FLG_PHYREXIANO,
            cmc AS QTD_CUSTO_CONVERTIDO,
            funny AS FLG_HUMORISTICO,
            regexp_replace(colors, '\\[|\\]|"', '') AS COD_CORES,
            regexp_replace(gatherer_alternates, '\\[|\\]|"', '') AS DESC_GRAFIAS_GATHERER,
            to_timestamp(ingestion_timestamp) AS DT_INGESTAO,
            coalesce(nullif(trim(source), ''), 'NA') AS NME_FONTE,
            endpoint AS DESC_URL_ORIGEM,
            source_file AS DESC_ARQUIVO_ORIGEM,
            bronze_run_id AS ID_EXECUCAO_BRONZE,
            bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE
        FROM _symbology_bronze
    """)

    # COD_SIMBOLO fica de fora (ver docstring do modulo).
    df_final = normalizar_valores(df_final, [
        "DESC_VARIANTE_LIVRE", "DESC_SIMBOLO", "DESC_GRAFIAS_GATHERER", "NME_FONTE",
    ])

    logger.info(f"Transformacao Simbolos de Mana concluida: {df_final.count()} registros")
    return df_final

# =============================================================================
# CONFIGURACAO
# =============================================================================

config = create_manual_config(get_secret("catalog_name"), get_secret("s3_bucket"))

setup_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
processor = SilverTableProcessor("TB_DOM_SIMBOLOS", config)

df_bronze = processor.extract_from_bronze("symbology")

df_silver = processor.transform_data(df_bronze, transform_symbology_silver)

processor.save_silver_table(
    df_silver,
    key_column="COD_SIMBOLO",
    order_by_col="DT_INGESTAO",
    table_comment=get_table_comment("TB_DOM_SIMBOLOS"),
    column_comments=get_column_comments("TB_DOM_SIMBOLOS")
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
