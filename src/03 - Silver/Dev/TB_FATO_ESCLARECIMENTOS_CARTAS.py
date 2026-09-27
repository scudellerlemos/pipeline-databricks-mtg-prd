# Databricks notebook source
# =============================================================================
# CAMADA SILVER - ESCLARECIMENTOS DE REGRAS - MAGIC: THE GATHERING
# =============================================================================
"""
TB_FATO_ESCLARECIMENTOS_CARTAS: rulings oficiais, Bronze `rulings` -> Silver.

Fato sem medida: uma linha por esclarecimento publicado para uma carta.

Chave unica: ID_ESCLARECIMENTO, surrogate sha2 de oracle_id + published_at +
comment (a fonte nao tem id proprio). O hash e deterministico, entao
reprocessar gera o mesmo id. `source` (emissor) fica fora do hash: particoes
antigas da Bronze tem source='scryfall' e as novas o emissor real; o merge por
DT_INGESTAO mais recente atualiza o emissor sem duplicar a linha.

DESC_ESCLARECIMENTO troca ( ) { } por colchetes, como em TB_FATO_CARTAS.
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
    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Esclarecimentos de Regras...")

    df.createOrReplaceTempView("_rulings_bronze")

    # _renomeado so traduz Bronze -> PT-BR. O hash usa DESC_ESCLARECIMENTO
    # original (antes da troca de delimitadores).
    df_final = spark.sql(r"""
        WITH _renomeado AS (
            SELECT
                oracle_id AS ID_ORACLE,
                source AS NME_EMISSOR,
                published_at AS DT_PUBLICACAO,
                comment AS DESC_ESCLARECIMENTO,
                ingestion_timestamp AS DT_INGESTAO,
                -- source aqui e o EMISSOR (wotc/scryfall), nao a linhagem;
                -- a fonte dos dados e sempre a Scryfall.
                'scryfall' AS NME_FONTE,
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

    df_final = normalizar_valores(df_final, ["NME_EMISSOR", "NME_FONTE"])

    logger.info(f"Transformacao Esclarecimentos de Regras concluida: {df_final.count()} registros")
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
processor = SilverTableProcessor("TB_FATO_ESCLARECIMENTOS_CARTAS", config)

df_bronze = processor.extract_from_bronze("rulings")

df_silver = processor.transform_data(df_bronze, transform_rulings_silver)

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
