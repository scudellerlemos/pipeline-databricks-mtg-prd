# Databricks notebook source
# =============================================================================
# CAMADA SILVER - SIMBOLOS DE MANA - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para processamento da tabela TB_DOM_SIMBOLOS.
Transformacao e limpeza de dados da Bronze para Silver.

CLASSIFICACAO DAMA-DMBOK: DOM/REF - lista de referencia pequena e
praticamente estatica (catalogo de simbolos de mana/custo da Scryfall,
raramente ganha item novo), sem grao de evento nem medida de negocio. Daí o
prefixo TB_DOM_ (dominio) e nao TB_DIM_ (que e reservado a entidades que
crescem organicamente, como TB_DIM_COLECOES).

CHAVE UNICA: COD_SIMBOLO (notacao do simbolo - sempre presente e nunca nula
na fonte, ver save_silver_table no fim do notebook) - coluna unica NOT NULL,
Unity Catalog consegue declarar a constraint PRIMARY KEY de verdade.

REGRA "SEM ( ) { } NO DADO SILVER" - APLICADA SEM EXCECAO A COD_SIMBOLO:
- A notacao nativa de simbolo de mana da Scryfall usa chaves (ex.: "{W}",
  "{2/U}") - e notacao legitima do dominio, nao um artefato de serializacao
  como em outras colunas. Mesmo assim, esta tabela segue a MESMA conversao
  pra colchete ([W], [2/U]) que TB_FATO_CARTAS ja aplica aos mesmos simbolos
  quando eles aparecem embutidos em DESC_CUSTO_MANA/DESC_CARTA - sem essa
  consistencia, o mesmo simbolo apareceria com notacao diferente em cada
  tabela, e a Gold nao conseguiria juntar um token extraido do texto da
  carta contra COD_SIMBOLO sem antes reconverter a notacao. COD_SIMBOLO
  NUNCA recebe normalizar_valor()/Title_Case - so a conversao de chave, sem
  excecao (ver transform_symbology_silver abaixo).

CONVENCAO DE NOME/CASE DE COLUNA (pedido do usuario): mesma de TB_FATO_CARTAS
(ver docstring de la) - nome de coluna 100% MAIUSCULO, valor de atributo em
Title_Case por palavra sem acento (normalizar_valor() em silver_utils.py),
exceto COD_/ID_/URL_* (COD_SIMBOLO em particular - ver regra acima) e texto
livre longo.

SEM partition_cols: tabela pequena e estatica (uma linha por simbolo de
mana conhecido, algumas dezenas de linhas) - particionamento fisico nao
traz beneficio aqui.
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
    if not df:
        return None

    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Simbolos de Mana...")

    df.createOrReplaceTempView("_symbology_bronze")

    # Renomeia Bronze -> PT-BR, converte chave pra colchete em COD_SIMBOLO
    # (regra sem excecao - ver docstring do modulo, NUNCA normalizar_valor()
    # aqui) e limpa array serializado em COD_CORES/DESC_GRAFIAS_GATHERER.
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

    # COD_SIMBOLO fica de fora: regra sem excecao (ver docstring do modulo).
    df_final = normalizar_valores(df_final, [
        "DESC_VARIANTE_LIVRE", "DESC_SIMBOLO", "DESC_GRAFIAS_GATHERER", "NME_FONTE",
    ])

    logger.info(f"Transformacao Simbolos de Mana concluida: {df_final.count()} registros")
    return df_final

# =============================================================================
# CONFIGURACAO
# =============================================================================

# Configuracao manual. catalog_name vem do mesmo secret que a Bronze usa
# (get_secret("catalog_name")).
config = create_manual_config(get_secret("catalog_name"), get_secret("s3_bucket"))

# Setup Unity Catalog
setup_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
# Criar processor
processor = SilverTableProcessor("TB_DOM_SIMBOLOS", config)

# Extracao da Bronze (nome real da tabela no catalog, minusculo)
df_bronze = processor.extract_from_bronze("symbology")

# Aplicar transformacao especifica
df_silver = processor.transform_data(df_bronze, transform_symbology_silver)

# Salvar na Silver com merge incremental por COD_SIMBOLO. Sem partition_cols
# (ver docstring da celula anterior - tabela pequena e estatica).
processor.save_silver_table(
    df_silver,
    key_column="COD_SIMBOLO",
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
