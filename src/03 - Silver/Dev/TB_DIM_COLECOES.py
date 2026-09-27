# Databricks notebook source
# =============================================================================
# CAMADA SILVER - COLEÇÕES (SETS) - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para processamento da tabela TB_DIM_COLECOES.
Transformação e limpeza de dados da Bronze para Silver.

CLASSIFICAÇÃO DAMA-DMBOK: Dimensão - descreve a entidade de negócio
"coleção/edição" (nome, tipo, data de lançamento, bloco...), sem medida
quantitativa própria além de contagens descritivas (QTD_CARTAS). É
referenciada por COD_COLECAO a partir de TB_FATO_CARTAS - não é uma lista de
domínio estática pequena (REF), é uma dimensão real que cresce a cada
lançamento. Daí TB_DIM_ e não TB_REF_.

CHAVE ÚNICA: COD_COLECAO (código curto do set - sempre presente e nunca nulo
na fonte, ver save_silver_table no fim do notebook). Diferente de
TB_FATO_CARTAS, aqui a chave é uma única coluna NOT NULL - Unity Catalog
consegue declarar a constraint PRIMARY KEY de verdade (não só o comentário
de tabela), ver silver_utils.save_to_silver.

CONVENÇÃO DE NOME/CASE DE COLUNA: nome de coluna 100% MAIÚSCULO (prefixo
semântico + resto, ex.: COD_COLECAO, NME_COLECAO). Valor de atributo (colunas
de nome/categoria) em Title_Case por palavra, sem acento, espaço virando "_"
(ex.: "Standard Booster" -> "Standard_Booster") - ver normalizar_valor() em
silver_utils.py. Exceção: COD_/ID_/URL_* e texto livre longo mantêm sua
própria convenção de case.

O SELECT abaixo traduz explicitamente toda coluna da Bronze sets (em inglês:
code/name/type/releaseDate/onlineOnly...) pro nome PT-BR final. Extração usa
"sets" (nome real da tabela no catalog, minúsculo).
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
def setup_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def transform_sets_silver(df):
    """
    Transformação específica para tabela Coleções, via SQL (spark.sql sobre temp views)
    """
    if not df:
        return None

    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformações específicas para Coleções...")

    df.createOrReplaceTempView("_sets_bronze")

    # onlineOnly pode não existir em alguma carga da Bronze - cai pra NULL
    # tipado em vez de estourar AnalysisException (coluna é boolean, ver
    # README Bronze).
    online_only_select = "onlineOnly AS FLG_SOMENTE_ONLINE" if "onlineOnly" in df.columns \
        else "CAST(NULL AS BOOLEAN) AS FLG_SOMENTE_ONLINE"

    # booster_0..19: a Stage explode a lista "booster" da fonte em 1 coluna
    # por posição (ver bronze_column_docs.py) - repassa aqui com nome PT-BR,
    # sem mudar o formato.
    booster_cols_select = ", ".join(f"booster_{i} AS DESC_BOOSTER_SLOT_{i}" for i in range(20))

    # Uma única query: renomeia Bronze -> PT-BR, converte DT_LANCAMENTO e já
    # deriva ANO_LANCAMENTO/MES_LANCAMENTO na mesma passada. NME_FONTE cai
    # pra 'NA' se nulo/vazio - normalizar_valores() abaixo cuida do sentinela
    # 'NA' junto com o Title_Case/sem-acento das colunas de nome/categoria.
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
            {online_only_select},
            card_count AS QTD_CARTAS,
            parent_set_code AS COD_COLECAO_PAI,
            block AS NME_BLOCO,
            icon_svg_uri AS URL_ICONE,
            {booster_cols_select},
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

    # Title_Case/"_"/sem-acento (ver normalizar_valor() em silver_utils.py)
    # nas colunas de nome/categoria, de uma vez só, depois que o SQL acima já
    # resolveu rename + tipos + derivação.
    df_final = normalizar_valores(df_final, [
        "NME_COLECAO", "NME_TIPO_COLECAO", "NME_CARDMARKET",
        "NME_COR_BORDA", "NME_BLOCO", "NME_FONTE",
    ])

    logger.info(f"Transformação Coleções concluída: {df_final.count()} registros")
    return df_final

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

# Configuração manual. catalog_name vem do mesmo secret que a Bronze usa
# (get_secret("catalog_name")).
config = create_manual_config(get_secret("catalog_name"), get_secret("s3_bucket"))

# Setup Unity Catalog
setup_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
# Criar processor
processor = SilverTableProcessor("TB_DIM_COLECOES", config)

# Extração da Bronze (nome real da tabela no catalog, minúsculo)
df_bronze = processor.extract_from_bronze("sets")

# Aplicar transformação específica
df_silver = processor.transform_data(df_bronze, transform_sets_silver)

# Salvar na Silver com merge incremental
processor.save_silver_table(
    df_silver,
    partition_cols=["ANO_LANCAMENTO", "MES_LANCAMENTO"],
    key_column="COD_COLECAO",
    table_comment=get_table_comment("TB_DIM_COLECOES"),
    column_comments=get_column_comments("TB_DIM_COLECOES")
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
