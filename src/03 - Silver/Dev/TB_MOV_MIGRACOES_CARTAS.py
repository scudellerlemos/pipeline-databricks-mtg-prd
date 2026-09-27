# Databricks notebook source
# =============================================================================
# CAMADA SILVER - MIGRACOES DE ID DE CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
TB_MOV_MIGRACOES_CARTAS: migracoes de id da Scryfall, Bronze `migrations` -> Silver.

Movimento: cada linha e um scryfall_id que mudou (carta unificada em outra ou
removida). A Gold usa ID_CARTA_ANTIGO -> ID_CARTA_CANONICO para resolver ids
antigos.

Migracoes podem encadear (A -> B -> C); _resolver_cadeia_ids segue a cadeia em
Python (o Spark desta versao nao tem CTE recursiva). Ver test_migration_chain.py.

Chave unica: ID_MIGRACAO (nunca nulo na fonte), declarada como PRIMARY KEY.

DESC_NOTA troca ( ) { } por colchetes, como em TB_FATO_CARTAS.
Mesma convencao de nome/case de TB_FATO_CARTAS.
"""

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import logging
from pyspark.sql.functions import col

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

def _resolver_cadeia_ids(mapa_direto):
    """
    Segue a cadeia ID_CARTA_ANTIGO -> ID_CARTA_NOVO ate o id final
    (A -> B -> C resolve A para C). Limite de 10 saltos; ciclos param no
    ultimo id antes de repetir.
    """
    resolvidos = {}
    for inicio in mapa_direto:
        atual = inicio
        vistos = {inicio}
        saltos = 0
        while atual in mapa_direto and saltos < 10:
            proximo = mapa_direto[atual]
            if proximo in vistos:
                break
            atual = proximo
            vistos.add(atual)
            saltos += 1
        resolvidos[inicio] = atual
    return resolvidos

def transformar_migracoes_silver(df):
    """Transformacao da tabela Migracoes de Id de Cartas (SQL sobre temp view)."""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Migracoes de Id de Cartas...")

    df.createOrReplaceTempView("_migrations_bronze")

    # Renomeia Bronze -> PT-BR, traduz a estrategia (merge/delete), limpa
    # DESC_NOTA e deriva ANO/MES_EXECUCAO para particionamento.
    df_final = spark.sql(r"""
        SELECT
            id AS ID_MIGRACAO,
            uri AS URL_SCRYFALL,
            to_date(performed_at) AS DT_EXECUCAO,
            CASE
                WHEN migration_strategy = 'merge' THEN 'Unificacao'
                WHEN migration_strategy = 'delete' THEN 'Remocao'
                ELSE migration_strategy
            END AS NME_ESTRATEGIA_MIGRACAO,
            old_scryfall_id AS ID_CARTA_ANTIGO,
            new_scryfall_id AS ID_CARTA_NOVO,
            coalesce(
                nullif(regexp_replace(regexp_replace(trim(note), '\\(([^)]*)\\)', '[$1]'), '\\{([^}]*)\\}', '[$1]'), ''),
                'NA'
            ) AS DESC_NOTA,
            metadata_id AS ID_CARTA_ASSOCIADA,
            metadata_lang AS COD_IDIOMA,
            metadata_name AS NME_CARTA_ASSOCIADA,
            upper(metadata_set_code) AS COD_COLECAO_ASSOCIADA,
            metadata_oracle_id AS ID_ORACLE_ASSOCIADO,
            metadata_collector_number AS NUM_COLECIONADOR_ASSOCIADO,
            to_timestamp(ingestion_timestamp) AS DT_INGESTAO,
            coalesce(nullif(trim(source), ''), 'NA') AS NME_FONTE,
            endpoint AS DESC_URL_ORIGEM,
            source_file AS DESC_ARQUIVO_ORIGEM,
            bronze_run_id AS ID_EXECUCAO_BRONZE,
            bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE,
            year(to_date(performed_at)) AS ANO_EXECUCAO,
            month(to_date(performed_at)) AS MES_EXECUCAO
        FROM _migrations_bronze
    """)

    df_final = normalizar_valores(df_final, ["NME_CARTA_ASSOCIADA", "NME_FONTE"])

    logger.info(f"Transformacao Migracoes de Id de Cartas concluida: {df_final.count()} registros")
    return df_final

def anexar_id_canonico(df_migracoes):
    """
    Anexa ID_CARTA_CANONICO: o id final apos seguir as unificacoes.
    Linhas de 'Remocao' (sem ID_CARTA_NOVO) ficam com
    ID_CARTA_CANONICO = ID_CARTA_ANTIGO.
    """
    logger = logging.getLogger(__name__)

    # Ordenado por DT_EXECUCAO (desempate ID_MIGRACAO): se a carta migrou mais
    # de uma vez, a ultima escrita no dict (a mais recente) vence.
    linhas_merge = (
        df_migracoes
        .filter("NME_ESTRATEGIA_MIGRACAO = 'Unificacao' AND ID_CARTA_NOVO IS NOT NULL")
        .select("ID_CARTA_ANTIGO", "ID_CARTA_NOVO", "DT_EXECUCAO", "ID_MIGRACAO")
        .distinct()
        .orderBy("ID_CARTA_ANTIGO", "DT_EXECUCAO", "ID_MIGRACAO")
        .collect()
    )
    mapa_direto = {}
    for r in linhas_merge:
        mapa_direto[r["ID_CARTA_ANTIGO"]] = r["ID_CARTA_NOVO"]
    mapa_resolvido = _resolver_cadeia_ids(mapa_direto)

    if not mapa_resolvido:
        return df_migracoes.withColumn("ID_CARTA_CANONICO", col("ID_CARTA_ANTIGO"))

    df_mapa = spark.createDataFrame(
        list(mapa_resolvido.items()), ["_old_id", "_canonical_id"]
    )
    df_mapa.createOrReplaceTempView("_migration_resolved_map")
    df_migracoes.createOrReplaceTempView("_migrations_pre_canonical")

    df_resultado = spark.sql("""
        SELECT
            mig.*,
            coalesce(map._canonical_id, mig.ID_CARTA_ANTIGO) AS ID_CARTA_CANONICO
        FROM _migrations_pre_canonical mig
        LEFT JOIN _migration_resolved_map map
            ON mig.ID_CARTA_ANTIGO = map._old_id
    """)
    logger.info(f"ID_CARTA_CANONICO resolvido para {len(mapa_resolvido)} ids migrados.")
    return df_resultado

# =============================================================================
# CONFIGURACAO
# =============================================================================

config = criar_config_manual(obter_segredo("catalog_name"), obter_segredo("s3_bucket"))

configurar_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
processador = SilverTableProcessor("TB_MOV_MIGRACOES_CARTAS", config)

df_bronze = processador.extrair_da_bronze("migrations")

df_silver_parcial = processador.transformar_dados(df_bronze, transformar_migracoes_silver)
df_silver = anexar_id_canonico(df_silver_parcial)

processador.salvar_tabela_silver(
    df_silver,
    colunas_particao=["ANO_EXECUCAO", "MES_EXECUCAO"],
    coluna_chave="ID_MIGRACAO",
    coluna_ordenacao="DT_INGESTAO",
    comentario_tabela=obter_comentario_tabela("TB_MOV_MIGRACOES_CARTAS"),
    comentarios_colunas=obter_comentarios_colunas("TB_MOV_MIGRACOES_CARTAS")
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
