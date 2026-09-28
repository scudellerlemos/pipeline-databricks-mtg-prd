# Databricks notebook source
# =============================================================================
# CAMADA GOLD - MERCADO DE CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
Constrói TB_FATO_MERCADO_CARTAS, visão única de consumo (analista/BI/Genie)
sobre mercado de cartas, montada a partir da Silver. Duas tabelas:

- gold.TB_DIM_CARTAS: 1 linha por ID_CARTA com os atributos atuais
  (catálogo, coleção, rulings, migração). Overwrite a cada run (~tamanho do
  catálogo, não cresce com o histórico).
- gold.TB_FATO_MERCADO_CARTAS: silver.TB_FATO_PRECOS_CARTAS x TB_DIM_CARTAS.
  Incremental: MERGE só com (a) cotações depois da última da Gold e (b) todo
  o histórico das cartas cujo atributo mudou na dimensão (EXCEPT da
  dimensão recalculada contra a gravada). Assim migração de id e ruling
  novo chegam às cotações antigas sem recalcular tudo.

A fato é gravada antes da dimensão: se a fato falhar, a dimensão gravada
continua a antiga e a próxima run acha as mesmas cartas mudadas. Sem time
travel de propósito: o log Delta guarda 30 dias e a run é mensal.

Carga completa (overwrite das duas) quando: fato ou dimensão ainda não
existem, a dimensão mudou de colunas, ou widget rebuild=true. O incremental
não apaga: cotação removida da Silver (correção manual) só sai da Gold com
rebuild=true.

GRÃO da fato: uma linha por cotação de preço de uma impressão de carta.
Chave: (ID_CARTA, DT_COTACAO), única por construção: PK da Silver de preços
(ID_CARTA, DT_INGESTAO) x PK de TB_DIM_CARTAS (ID_CARTA). Esclarecimentos e
migrações são agregados antes do join para não haver fan-out; chave
duplicada na dimensão faz o salvar_na_gold abortar antes de gravar.

VLR_USD/EUR/TIX são o preço daquela impressão (reimpressão e original são
linhas distintas). _FOIL/_ETCHED são outra cotação da mesma impressão, por
isso são colunas: SUM(VLR_USD) não inclui foil.

TABELAS SILVER USADAS:
- TB_FATO_CARTAS (driver da dimensão): 1 linha por impressão.
- TB_FATO_PRECOS_CARTAS (INNER JOIN por ID_CARTA, na fato): N cotações por impressão.
  INNER porque DT_COTACAO é parte da chave; cartas sem cotação ficam de fora
  (contadas no DQ pré-join).
- TB_DIM_COLECOES (LEFT JOIN por COD_COLECAO): nome, bloco e data de
  lançamento da coleção.
- TB_FATO_ESCLARECIMENTOS_CARTAS (agregada por ID_ORACLE, LEFT JOIN):
  quantidade e data do ruling mais recente.
- TB_MOV_MIGRACOES_CARTAS (agregada por ID_CARTA_ANTIGO, LEFT JOIN): indica se
  a Scryfall fundiu/removeu o ID_CARTA e qual é o id vigente. Uma carta pode
  ter vários eventos; vence o mais recente (DT_EXECUCAO, ID_MIGRACAO).

REGRA DE NULO:
- NME_COLECAO/NME_BLOCO nulos (sem coleção ou sem bloco) -> 'Nao_Identificado'.
- Demais categóricos vêm da Silver como estão.
- Preços nulos continuam nulos (0 não significa "sem cotação").
- QTD_ESCLARECIMENTOS nulo -> 0 (carta nunca teve ruling).
- Datas nulas -> sentinela 1001-01-01.
- PK nunca é mascarada: nulo faz a run falhar em _declarar_chave_primaria.
- ID_CARTA_CANONICO sem migração -> o próprio ID_CARTA.
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

# MAGIC %run ./gold_utils

# COMMAND ----------

# MAGIC %run ./gold_column_docs

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


def transformar_dim_cartas(df_cartas, df_colecoes, df_precos, df_esclarecimentos, df_migracoes):
    """TB_DIM_CARTAS: cartas + coleção + rulings + migração, via spark.sql sobre
    temp views. df_precos só entra no DQ pré-join."""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando join Gold - TB_DIM_CARTAS...")

    df_cartas.createOrReplaceTempView("_cartas")
    df_colecoes.createOrReplaceTempView("_colecoes")
    df_precos.createOrReplaceTempView("_precos")
    df_esclarecimentos.createOrReplaceTempView("_esclarecimentos")
    df_migracoes.createOrReplaceTempView("_migracoes")

    # DATA QUALITY (pré-join): conta o que o INNER JOIN da fato descarta, antes de gravar nada.
    executar_checagens_dq(spark, "pré-join", {
        # Carta sem cotação. Sempre > 0: carta nova chega antes do preço dela.
        "cartas_excluidas_sem_cotacao_de_preco": ("""
            SELECT COUNT(DISTINCT c.ID_CARTA)
            FROM _cartas c
            LEFT JOIN _precos p ON c.ID_CARTA = p.ID_CARTA
            WHERE p.ID_CARTA IS NULL
        """, None),

        # Preço sem carta em TB_FATO_CARTAS (token, promo, art series: os
        # filtros de preço e de carta são diferentes). Mantidos na Silver:
        # cotação passada não dá pra recoletar.
        # Limite 12000 sobre baseline de ~7476: pega filtro de coleção quebrado.
        # Se estourar por motivo legítimo (coleção nova grande), medir de novo e subir.
        "precos_excluidos_sem_carta": ("""
            SELECT COUNT(DISTINCT p.ID_CARTA)
            FROM _precos p
            LEFT JOIN _cartas c ON p.ID_CARTA = c.ID_CARTA
            WHERE c.ID_CARTA IS NULL
        """, 12000),
    })

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _esclarecimentos_agg AS
        SELECT
            ID_ORACLE,
            COUNT(*) AS QTD_ESCLARECIMENTOS,
            MAX(DT_PUBLICACAO) AS DT_ULTIMO_ESCLARECIMENTO
        FROM _esclarecimentos
        GROUP BY ID_ORACLE
    """)

    # 1 linha por ID_CARTA_ANTIGO (a mais recente), para o LEFT JOIN nao duplicar linhas.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _migracoes_resolvidas AS
        SELECT ID_CARTA_ANTIGO, ID_CARTA_CANONICO
        FROM (
            SELECT
                ID_CARTA_ANTIGO,
                ID_CARTA_CANONICO,
                ROW_NUMBER() OVER (
                    PARTITION BY ID_CARTA_ANTIGO
                    ORDER BY DT_EXECUCAO DESC, ID_MIGRACAO DESC
                ) AS rn
            FROM _migracoes
        )
        WHERE rn = 1
    """)

    return spark.sql("""
        SELECT
            c.ID_CARTA,
            c.ID_ORACLE,
            c.NME_CARTA,
            c.NME_TIPO_CARTA,
            c.NME_RARIDADE,
            c.NME_CATEGORIA_COR,
            c.COD_CORES,
            c.QTD_CUSTO_MANA,
            c.COD_COLECAO,
            COALESCE(col.NME_COLECAO, 'Nao_Identificado') AS NME_COLECAO,
            COALESCE(col.NME_BLOCO, 'Nao_Identificado') AS NME_BLOCO,
            COALESCE(col.DT_LANCAMENTO, DATE'1001-01-01') AS DT_LANCAMENTO_COLECAO,
            COALESCE(e.QTD_ESCLARECIMENTOS, 0) AS QTD_ESCLARECIMENTOS,
            COALESCE(e.DT_ULTIMO_ESCLARECIMENTO, DATE'1001-01-01') AS DT_ULTIMO_ESCLARECIMENTO,
            COALESCE(mig.ID_CARTA_CANONICO, c.ID_CARTA) AS ID_CARTA_CANONICO,
            CASE WHEN mig.ID_CARTA_ANTIGO IS NOT NULL THEN 'Sim' ELSE 'Nao' END AS FLG_ID_CARTA_MIGRADO
        FROM _cartas c
        LEFT JOIN _colecoes col ON c.COD_COLECAO = col.COD_COLECAO
        LEFT JOIN _esclarecimentos_agg e ON c.ID_ORACLE = e.ID_ORACLE
        LEFT JOIN _migracoes_resolvidas mig ON c.ID_CARTA = mig.ID_CARTA_ANTIGO
    """)


def consulta_fato_mercado(catalogo, filtro=""):
    """SELECT de TB_FATO_MERCADO_CARTAS: preços da Silver x dimensão recalculada (_dim_cartas)."""
    return f"""
        SELECT
            d.ID_CARTA,
            d.ID_ORACLE,
            d.NME_CARTA,
            d.NME_TIPO_CARTA,
            d.NME_RARIDADE,
            d.NME_CATEGORIA_COR,
            d.COD_CORES,
            d.QTD_CUSTO_MANA,
            d.COD_COLECAO,
            d.NME_COLECAO,
            d.NME_BLOCO,
            d.DT_LANCAMENTO_COLECAO,
            p.DT_INGESTAO AS DT_COTACAO,
            p.VLR_USD,
            p.VLR_USD_FOIL,
            p.VLR_USD_ETCHED,
            p.VLR_EUR,
            p.VLR_EUR_FOIL,
            p.VLR_TIX,
            d.QTD_ESCLARECIMENTOS,
            d.DT_ULTIMO_ESCLARECIMENTO,
            d.ID_CARTA_CANONICO,
            d.FLG_ID_CARTA_MIGRADO,
            p.ANO_INGESTAO AS ANO_COTACAO,
            p.MES_INGESTAO AS MES_COTACAO
        FROM {catalogo}.silver.TB_FATO_PRECOS_CARTAS p
        INNER JOIN _dim_cartas d ON p.ID_CARTA = d.ID_CARTA
        {filtro}
    """


# =============================================================================
# CONFIGURACAO
# =============================================================================
config = criar_config_manual(obter_segredo("catalog_name"), obter_segredo("s3_bucket"))
configurar_unity_catalog(config['catalog_name'], config['schema_gold'])

# rebuild=true: recalcula a fato inteira (ex.: depois de apagar dado da Silver).
dbutils.widgets.text("rebuild", "false")

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO, DATA QUALITY E AUDITORIA
# =============================================================================
# try/finally: toda run grava 1 linha de auditoria, inclusive as que abortam
# (DQ pré-join, validação de PK ou erro inesperado).
execucao_auditoria = iniciar_execucao_auditoria()
status_auditoria = "SUCESSO"
dq_resultados = {}
qtd_lidos = 0
qtd_processados = 0
nome_completo_tabela = f"{config['catalog_name']}.{config['schema_gold']}.TB_FATO_MERCADO_CARTAS"
nome_dim = f"{config['catalog_name']}.{config['schema_gold']}.TB_DIM_CARTAS"
processador = GoldTableProcessor("TB_DIM_CARTAS", config)
processador_fato = GoldTableProcessor("TB_FATO_MERCADO_CARTAS", config)

try:
    df_cartas = processador.extrair_da_silver("TB_FATO_CARTAS")
    df_colecoes = processador.extrair_da_silver("TB_DIM_COLECOES")
    df_precos = processador.extrair_da_silver("TB_FATO_PRECOS_CARTAS")
    df_esclarecimentos = processador.extrair_da_silver("TB_FATO_ESCLARECIMENTOS_CARTAS")
    df_migracoes = processador.extrair_da_silver("TB_MOV_MIGRACOES_CARTAS")

    qtd_lidos = (
        df_cartas.count() + df_colecoes.count() + df_precos.count()
        + df_esclarecimentos.count() + df_migracoes.count()
    )

    # cache: checagem de duplicata e escrita reusam o join.
    df_dim = transformar_dim_cartas(df_cartas, df_colecoes, df_precos, df_esclarecimentos, df_migracoes).cache()

    incremental = (
        dbutils.widgets.get("rebuild").lower() != "true"
        and spark.catalog.tableExists(nome_completo_tabela)
        and spark.catalog.tableExists(nome_dim)
        and spark.table(nome_dim).columns == df_dim.columns
    )
    corte = spark.table(nome_completo_tabela).agg({"DT_COTACAO": "max"}).first()[0] if incremental else None
    incremental = corte is not None
    print(f"Incremental: cotações depois de {corte} + cartas que mudaram na dimensão" if incremental
          else "Carga completa de TB_FATO_MERCADO_CARTAS")
    df_dim.createOrReplaceTempView("_dim_cartas")

    try:
        if incremental:
            # Materializado: a dimensão gravada é sobrescrita logo depois da fato.
            df_mudaram = df_dim.exceptAll(spark.table(nome_dim)).select("ID_CARTA").distinct().cache()
            df_mudaram.createOrReplaceTempView("_cartas_mudaram")
            print(f"Cartas novas ou com atributo alterado: {df_mudaram.count()}")
            consulta = consulta_fato_mercado(
                config['catalog_name'],
                "WHERE p.DT_INGESTAO > :corte OR p.ID_CARTA IN (SELECT ID_CARTA FROM _cartas_mudaram)",
            )
            df_fato = spark.sql(consulta, args={"corte": corte})
        else:
            df_fato = spark.sql(consulta_fato_mercado(config['catalog_name']))

        # cache: count, checagem de duplicata e escrita reusam o join.
        df_fato = df_fato.cache()
        qtd_processados = df_fato.count()
        processador_fato.salvar_tabela_gold(
            df_fato,
            colunas_particao=["ANO_COTACAO", "MES_COTACAO"],
            coluna_chave=["ID_CARTA", "DT_COTACAO"],
            comentario_tabela=obter_comentario_tabela("TB_FATO_MERCADO_CARTAS"),
            comentarios_colunas=obter_comentarios_colunas("TB_FATO_MERCADO_CARTAS"),
            incremental=incremental
        )

        # Depois da fato (ver docstring): falha na fato não adianta a dimensão.
        processador.salvar_tabela_gold(
            df_dim,
            coluna_chave="ID_CARTA",
            comentario_tabela=obter_comentario_tabela("TB_DIM_CARTAS"),
            comentarios_colunas=obter_comentarios_colunas("TB_DIM_CARTAS")
        )
    except RuntimeError:
        status_auditoria = "FALHA_DQ_PK"
        raise

    # DATA QUALITY (pós-carga)
    dq_resultados = executar_checagens_dq(spark, nome_completo_tabela, {
        # ID_ORACLE vem da Silver sem COALESCE; nenhum nulo é tolerado.
        "fk_null_id_oracle": f"SELECT COUNT(*) FROM {nome_completo_tabela} WHERE ID_ORACLE IS NULL",

        # Categóricos nunca são nulos (COALESCE aqui ou 'NA' na Silver).
        "null_residual_categorico": f"""SELECT COUNT(*) FROM {nome_completo_tabela}
            WHERE NME_CARTA IS NULL OR NME_TIPO_CARTA IS NULL OR NME_RARIDADE IS NULL
               OR NME_CATEGORIA_COR IS NULL OR COD_CORES IS NULL
               OR NME_COLECAO IS NULL OR NME_BLOCO IS NULL""",

        # A Scryfall não tem preço negativo: se aparecer, é erro de transformação.
        "valor_negativo_preco": f"""SELECT COUNT(*) FROM {nome_completo_tabela}
            WHERE VLR_USD < 0 OR VLR_EUR < 0 OR VLR_TIX < 0
               OR VLR_USD_FOIL < 0 OR VLR_USD_ETCHED < 0 OR VLR_EUR_FOIL < 0""",

        # Informativo: carta cuja COD_COLECAO não veio em /sets. Sem limite
        # até haver baseline medida.
        "fk_colecao_nao_encontrada": (
            f"SELECT COUNT(*) FROM {nome_completo_tabela} WHERE NME_COLECAO = 'Nao_Identificado'",
            None,
        ),

        # Informativo: ids migrados pela Scryfall só crescem com o tempo.
        "cartas_com_id_migrado": (
            f"SELECT COUNT(*) FROM {nome_completo_tabela} WHERE FLG_ID_CARTA_MIGRADO = 'Sim'",
            None,
        ),
    })
except DataQualityError as erro_dq:
    # Pré-join ou pós-carga: as contagens do DQ vão para a auditoria.
    status_auditoria = "FALHA_DQ"
    dq_resultados = erro_dq.resultados
    raise
except Exception:
    if status_auditoria == "SUCESSO":
        status_auditoria = "FALHA"
    raise
finally:
    # Grava sempre: o log do cluster não sobrevive ao fim do job.
    registrar_auditoria_gold(
        spark, config['catalog_name'], config['schema_gold'], "TB_FATO_MERCADO_CARTAS",
        execucao_auditoria,
        qtd_lidos=qtd_lidos,
        qtd_processados=qtd_processados,
        qtd_inseridos_atualizados=qtd_processados if status_auditoria in ("SUCESSO", "FALHA_DQ") else 0,
        dq_resultados=dq_resultados,
        status=status_auditoria
    )

# =============================================================================
# VALIDACAO E LOGS
# =============================================================================
print(f"Processamento concluído com sucesso!")
print(f"Registros processados: {qtd_processados}")
print(f"Colunas finais: {df_fato.columns}")
