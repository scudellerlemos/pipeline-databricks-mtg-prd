# Databricks notebook source
# =============================================================================
# CAMADA SILVER - ESCLARECIMENTOS DE REGRAS - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para processamento da tabela TB_FATO_ESCLARECIMENTOS_CARTAS.
Transformacao e limpeza de dados da Bronze para Silver.

CLASSIFICACAO DAMA-DMBOK: Fato sem medida (factless fact) - uma linha por
esclarecimento oficial de regra (ruling) publicado para uma carta, grao de
evento (publicacao de um esclarecimento), sem medida quantitativa propria.
Ainda assim e Fato e nao DOM/REF: cresce continuamente (a Wizards publica
esclarecimento novo a cada carta lancada) e nao e uma lista de opcoes fixa.

CHAVE UNICA - ID_ESCLARECIMENTO (SURROGATE): a Bronze rulings nao traz um id
proprio de registro (Scryfall so garante oracle_id + source + published_at +
comment) - ID_ESCLARECIMENTO e gerado por hash determinístico
(sha2(concat_ws('|', ...), 256)) sobre essas 4 colunas, garantindo o mesmo id
em reprocessamentos do mesmo dado e permitindo declarar PRIMARY KEY de
verdade (coluna sempre NOT NULL, diferente de derivar a chave de colunas que
podem faltar).

REGRA "SEM ( ) { } NO DADO SILVER": DESC_ESCLARECIMENTO e texto de regras
livre e pode conter parenteses/chaves de notacao de simbolo - mesma
conversao pra colchete ([...]) usada em TB_FATO_CARTAS, por consistencia em
toda a camada Silver.

CONVENCAO DE NOME/CASE DE COLUNA (pedido do usuario): mesma de TB_FATO_CARTAS
(ver docstring de la) - nome de coluna 100% MAIUSCULO, valor de atributo em
Title_Case por palavra sem acento (normalizar_valor() em silver_utils.py),
exceto COD_/ID_/URL_* e texto livre longo.
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

def transform_rulings_silver(df):
    """
    Transformacao especifica para tabela Esclarecimentos de Regras, via SQL
    (spark.sql sobre temp views).
    """
    if not df:
        return None

    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Esclarecimentos de Regras...")

    df.createOrReplaceTempView("_rulings_bronze")

    # CTE _renomeado so traduz Bronze -> PT-BR; o SELECT externo computa o
    # hash e as transformacoes de negocio. ID_ESCLARECIMENTO e lido de
    # _renomeado (antes da traducao de NME_EMISSOR e da limpeza de
    # DESC_ESCLARECIMENTO) para manter o mesmo hash entre reprocessamentos.
    df_final = spark.sql(r"""
        WITH _renomeado AS (
            SELECT
                oracle_id AS ID_ORACLE,
                source AS NME_EMISSOR,
                published_at AS DT_PUBLICACAO,
                comment AS DESC_ESCLARECIMENTO,
                ingestion_timestamp AS DT_INGESTAO,
                source AS NME_FONTE,
                endpoint AS DESC_URL_ORIGEM,
                source_file AS DESC_ARQUIVO_ORIGEM,
                bronze_run_id AS ID_EXECUCAO_BRONZE,
                bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE
            FROM _rulings_bronze
        )
        SELECT
            ID_ORACLE,
            sha2(
                concat_ws('|',
                    coalesce(ID_ORACLE, ''),
                    coalesce(NME_EMISSOR, ''),
                    coalesce(cast(to_date(DT_PUBLICACAO) AS STRING), ''),
                    coalesce(DESC_ESCLARECIMENTO, '')
                ),
                256
            ) AS ID_ESCLARECIMENTO,
            CASE
                WHEN NME_EMISSOR = 'wotc' THEN 'Wizards'
                WHEN NME_EMISSOR = 'scryfall' THEN 'Scryfall'
                ELSE NME_EMISSOR
            END AS NME_EMISSOR,
            to_date(DT_PUBLICACAO) AS DT_PUBLICACAO,
            coalesce(
                nullif(regexp_replace(regexp_replace(trim(DESC_ESCLARECIMENTO), '\\{([^}]*)\\}', '[$1]'), '\\(([^)]*)\\)', '[$1]'), ''),
                'NA'
            ) AS DESC_ESCLARECIMENTO,
            to_timestamp(DT_INGESTAO) AS DT_INGESTAO,
            coalesce(nullif(trim(NME_FONTE), ''), 'NA') AS NME_FONTE,
            DESC_URL_ORIGEM,
            DESC_ARQUIVO_ORIGEM,
            ID_EXECUCAO_BRONZE,
            DT_INGESTAO_BRONZE,
            year(to_date(DT_PUBLICACAO)) AS ANO_PUBLICACAO,
            month(to_date(DT_PUBLICACAO)) AS MES_PUBLICACAO
        FROM _renomeado
    """)

    # NME_EMISSOR ja veio mapeado pra 'Wizards'/'Scryfall' nos casos
    # conhecidos (CASE acima) - normalizar_valor() so afeta o resto (demais
    # emissores), sem tocar nesses dois literais (nao contem espaco/acento).
    df_final = normalizar_valores(df_final, ["NME_EMISSOR", "NME_FONTE"])

    logger.info(f"Transformacao Esclarecimentos de Regras concluida: {df_final.count()} registros")
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
processor = SilverTableProcessor("TB_FATO_ESCLARECIMENTOS_CARTAS", config)

# Extracao da Bronze (nome real da tabela no catalog, minusculo)
df_bronze = processor.extract_from_bronze("rulings")

# Aplicar transformacao especifica
df_silver = processor.transform_data(df_bronze, transform_rulings_silver)

# Salvar na Silver com merge incremental por ID_ESCLARECIMENTO (surrogate
# hash - ver docstring da celula anterior)
processor.save_silver_table(
    df_silver,
    partition_cols=["ANO_PUBLICACAO", "MES_PUBLICACAO"],
    key_column="ID_ESCLARECIMENTO",
    order_by_col="DT_INGESTAO",
    table_comment=get_table_comment("TB_FATO_ESCLARECIMENTOS_CARTAS"),
    column_comments=get_column_comments("TB_FATO_ESCLARECIMENTOS_CARTAS")
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
