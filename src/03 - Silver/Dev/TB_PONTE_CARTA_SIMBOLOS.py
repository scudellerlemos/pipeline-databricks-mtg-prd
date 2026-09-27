# Databricks notebook source
# =============================================================================
# CAMADA SILVER - PONTE CARTA X SIMBOLOS DE CUSTO - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para construção da tabela TB_PONTE_CARTA_SIMBOLOS.
Silver -> Silver (não Bronze -> Silver): explode o custo de mana já limpo de
TB_FATO_CARTAS em uma linha por símbolo, resolvendo a relação N:N entre carta
e símbolo de mana que hoje só existe escondida dentro do texto de
DESC_CUSTO_MANA (ex.: "[2][U][U]" -> 3 linhas).

CLASSIFICAÇÃO DAMA-DMBOK: Ponte/associativa (bridge table) - não é Fato (não
tem grão de evento, não "aconteceu" em uma data; é a decomposição de um
atributo estático já existente em TB_FATO_CARTAS) nem Dimensão nem DOM/REF
(quem define o que cada símbolo significa é TB_DOM_SIMBOLOS. Esta tabela só
resolve QUAL carta tem QUAL símbolo). Daí o prefixo TB_PONTE_.

FONTE: lê TB_FATO_CARTAS da própria Silver (não da Bronze) - só lá
DESC_CUSTO_MANA já está limpo e convertido pra notação de colchete
([X] em vez de {X}, ver regra "sem ( ) { } no dado Silver" na docstring de
TB_FATO_CARTAS.py). Por isso não tem colunas de linhagem Bronze
(DT_INGESTAO/NME_FONTE/etc. de COMMON_COLUMNS) - esta tabela não é
extraída direto da Bronze, é derivada de outra Silver.

CHAVE ÚNICA: (ID_CARTA, NUM_ORDEM_SIMBOLO) - NUM_ORDEM_SIMBOLO é a posição do
símbolo dentro do custo de mana (1-based), sempre gerada por posexplode,
nunca nula por natureza.

SEM partition_cols: esta tabela não tem data de evento própria (é derivada de
um atributo estático de TB_FATO_CARTAS) - inventar ANO_X/MES_X sem uma data
real de origem seria particionamento artificial.

COD_SIMBOLO NÃO É MASCARADO: é a FK pra TB_DOM_SIMBOLOS.COD_SIMBOLO. Símbolo
sem match no domínio ainda vira uma linha aqui (o token existe no custo de
mana, é fato) - só não tem descrição/cor decodificada em TB_DOM_SIMBOLOS.
Contagem de símbolo sem match é logada (DQ informativo), nunca falha a run.
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

    # DQ informativo: símbolo extraído do custo de mana sem match no domínio
    # (não bloqueia a run - só sinaliza símbolo novo/não catalogado).
    # ponytail: reusa df_final (ja explodido) em vez de repetir o posexplode.
    # LATERAL VIEW nao aceita JOIN depois dele na mesma clausula FROM, e o ON
    # precisaria da coluna que o proprio explode gera - por isso a versao SQL
    # quebrava com PARSE_SYNTAX_ERROR antes de rodar o DQ.
    qtd_simbolo_nao_catalogado = df_final.join(
        df_simbolos, on="COD_SIMBOLO", how="left_anti"
    ).count()
    nivel = "⚠️" if qtd_simbolo_nao_catalogado > 0 else "✅"
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

# Silver -> Silver: lê direto via spark.table (não extract_from_bronze - esta
# tabela não tem fonte na Bronze, é derivada de outra Silver).
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
