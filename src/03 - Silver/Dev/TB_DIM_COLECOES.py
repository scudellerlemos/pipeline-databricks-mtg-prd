# Databricks notebook source
# =============================================================================
# CAMADA SILVER - COLEÇÕES (SETS) - MAGIC: THE GATHERING
# =============================================================================
"""
TB_DIM_COLECOES: coleções (sets) de Magic, Bronze `sets` -> Silver.

Dimensão: uma linha por coleção, referenciada por COD_COLECAO a partir de
TB_FATO_CARTAS. Cresce a cada lançamento (por isso DIM e não DOM/REF).

Chave única: COD_COLECAO (nunca nulo na fonte), declarada como PRIMARY KEY.

Nomes de coluna em MAIÚSCULO com prefixo semântico. Colunas de nome/categoria
passam por normalizar_valor() (Title_Case, sem acento, "_" no lugar de espaço);
COD_/ID_/URL_ e texto livre longo ficam como vêm.
"""

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import logging

# =============================================================================
# CARREGAMENTO DO MÓDULO UTILITÁRIO
# =============================================================================
# Importar infraestrutura comum e funções do silver_utils usando %run (Databricks)

# COMMAND ----------

# MAGIC %run "../../00 - Common/Dev/base_utils"

# COMMAND ----------

# MAGIC %run ./silver_utils

# COMMAND ----------

# MAGIC %run ./silver_column_docs

# COMMAND ----------

# =============================================================================
# CONFIGURAÇÃO INICIAL
# =============================================================================
def configurar_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def transformar_colecoes_silver(df):
    """
    Transformação específica para tabela Coleções, via SQL (spark.sql sobre temp views)
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformações específicas para Coleções...")

    df.createOrReplaceTempView("_sets_bronze")

    # onlineOnly pode faltar em alguma carga da Bronze: usa NULL boolean.
    expr_somente_online = "onlineOnly AS FLG_SOMENTE_ONLINE" if "onlineOnly" in df.columns \
        else "CAST(NULL AS BOOLEAN) AS FLG_SOMENTE_ONLINE"

    # booster_0..19: legado da magicthegathering.io, sempre nulo (a Scryfall
    # não expõe booster). Só renomeia.
    expr_colunas_booster = ", ".join(f"booster_{i} AS DESC_BOOSTER_SLOT_{i}" for i in range(20))

    # Renomeia Bronze -> PT-BR e deriva ANO/MES_LANCAMENTO. NME_FONTE vazio vira 'NA'.
    df_final = spark.sql(f"""
        SELECT
            upper(code) AS COD_COLECAO,  -- normaliza case: TB_FATO_CARTAS tambem faz upper() em COD_COLECAO, join entre as duas depende do mesmo case
            name AS NME_COLECAO,
            type AS NME_TIPO_COLECAO,
            border AS NME_COR_BORDA,
            mkm_id AS ID_CARDMARKET,
            mkm_name AS NME_CARDMARKET,
            to_date(releaseDate) AS DT_LANCAMENTO,
            gathererCode AS COD_GATHERER,
            magicCardsInfoCode AS COD_MAGICCARDSINFO,
            oldCode AS COD_ANTIGO,
            {expr_somente_online},
            card_count AS QTD_CARTAS,
            upper(parent_set_code) AS COD_COLECAO_PAI,
            block AS NME_BLOCO,
            icon_svg_uri AS URL_ICONE,
            {expr_colunas_booster},
            ingestion_timestamp AS DT_INGESTAO,
            coalesce(nullif(trim(source), ''), 'NA') AS NME_FONTE,
            endpoint AS DESC_URL_ORIGEM,
            source_file AS DESC_ARQUIVO_ORIGEM,
            bronze_run_id AS ID_EXECUCAO_BRONZE,
            bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE,
            year(to_date(releaseDate)) AS ANO_LANCAMENTO,
            month(to_date(releaseDate)) AS MES_LANCAMENTO
        FROM _sets_bronze
    """)

    df_final = normalizar_valores(df_final, [
        "NME_COLECAO", "NME_TIPO_COLECAO", "NME_CARDMARKET",
        "NME_COR_BORDA", "NME_BLOCO", "NME_FONTE",
    ])

    logger.info(f"Transformação Coleções concluída: {df_final.count()} registros")
    return df_final

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

config = criar_config_manual(obter_segredo("catalog_name"), obter_segredo("s3_bucket"))

configurar_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
processador = SilverTableProcessor("TB_DIM_COLECOES", config)

df_bronze = processador.extrair_da_bronze("sets")

df_silver = processador.transformar_dados(df_bronze, transformar_colecoes_silver)

processador.salvar_tabela_silver(
    df_silver,
    colunas_particao=["ANO_LANCAMENTO", "MES_LANCAMENTO"],
    coluna_chave="COD_COLECAO",
    coluna_ordenacao="DT_INGESTAO",
    comentario_tabela=obter_comentario_tabela("TB_DIM_COLECOES"),
    comentarios_colunas=obter_comentarios_colunas("TB_DIM_COLECOES")
)

# =============================================================================
# VALIDAÇÃO E LOGS
# =============================================================================
if df_silver:
    print(f"Processamento concluído com sucesso!")
    print(f"Registros processados: {df_silver.count()}")
    print(f"Colunas finais: {df_silver.columns}")
else:
    print("Falha no processamento - DataFrame vazio")
