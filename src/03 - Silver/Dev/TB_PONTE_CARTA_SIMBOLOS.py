# Databricks notebook source
# =============================================================================
# CAMADA SILVER - PONTE CARTA X SIMBOLOS DE CUSTO - MAGIC: THE GATHERING
# =============================================================================
"""
TB_PONTE_CARTA_SIMBOLOS: carta x símbolo de custo de mana, Silver -> Silver.

Tabela ponte (N:N): explode DESC_CUSTO_MANA de TB_FATO_CARTAS em uma linha
por símbolo (ex.: "[2][U][U]" -> 3 linhas). Lê da Silver porque só lá o custo
já está em notação de colchete; por isso não tem colunas de linhagem Bronze.

Chave única: (ID_CARTA, NUM_ORDEM_SIMBOLO), posição 1-based gerada por
posexplode. Sem particionamento: não há data de evento própria.

COD_SIMBOLO é FK para TB_DOM_SIMBOLOS. Símbolo sem match no domínio ainda
gera linha; a contagem é só logada (DQ informativo, não falha a run).
"""

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import logging

# =============================================================================
# CARREGAMENTO DO MODULO UTILITARIO
# =============================================================================

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


def transform_ponte_carta_simbolos(df_cartas, df_simbolos):
    """
    Explode DESC_CUSTO_MANA (já limpo, notação [X][Y]...) em 1 linha por
    símbolo via regexp_extract_all + posexplode.
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando explosão de custo de mana em símbolos...")

    df_cartas.createOrReplaceTempView("_cartas")
    df_simbolos.createOrReplaceTempView("_simbolos")

    df_final = spark.sql(r"""
        SELECT
            c.ID_CARTA,
            pos + 1 AS NUM_ORDEM_SIMBOLO,
            simbolo AS COD_SIMBOLO
        FROM _cartas c
        LATERAL VIEW posexplode(regexp_extract_all(c.DESC_CUSTO_MANA, '\\[[^\\]]*\\]', 0)) AS pos, simbolo
        WHERE c.DESC_CUSTO_MANA IS NOT NULL AND c.DESC_CUSTO_MANA != 'NA'
    """)

    # DQ informativo: símbolos sem match em TB_DOM_SIMBOLOS.
    # Feito em DataFrame: LATERAL VIEW não aceita JOIN na mesma cláusula FROM.
    qtd_simbolo_nao_catalogado = df_final.join(
        df_simbolos, on="COD_SIMBOLO", how="left_anti"
    ).count()
    nivel = "AVISO" if qtd_simbolo_nao_catalogado > 0 else "OK"
    print(f"{nivel} DQ simbolos_sem_match_em_TB_DOM_SIMBOLOS: {qtd_simbolo_nao_catalogado}")

    logger.info(f"Transformação Ponte Carta x Símbolos concluída: {df_final.count()} registros")
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
processor = SilverTableProcessor("TB_PONTE_CARTA_SIMBOLOS", config)

# Fonte é a própria Silver, então lê via spark.table e não extract_from_bronze.
df_cartas = spark.table(f"{config['catalog_name']}.{config['schema_silver']}.TB_FATO_CARTAS")
df_simbolos = spark.table(f"{config['catalog_name']}.{config['schema_silver']}.TB_DOM_SIMBOLOS")

df_silver = transform_ponte_carta_simbolos(df_cartas, df_simbolos)

processor.save_silver_table(
    df_silver,
    key_column=["ID_CARTA", "NUM_ORDEM_SIMBOLO"],
    table_comment=get_table_comment("TB_PONTE_CARTA_SIMBOLOS"),
    column_comments=get_column_comments("TB_PONTE_CARTA_SIMBOLOS")
)

# =============================================================================
# VALIDACAO E LOGS
# =============================================================================
if df_silver:
    print(f"Processamento concluído com sucesso!")
    print(f"Registros processados: {df_silver.count()}")
    print(f"Colunas finais: {df_silver.columns}")
else:
    print("Falha no processamento - DataFrame vazio")
