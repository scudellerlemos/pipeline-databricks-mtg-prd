# Databricks notebook source
# =============================================================================
# CAMADA GOLD - MERCADO DE CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para construção da tabela Gold TB_FATO_MERCADO_CARTAS.
Junta Silver -> Gold: uma única tabela de consumo (analista/BI/Genie) sobre
mercado de cartas, sem precisar conhecer Bronze/Silver.

GRAO: uma linha por cotação de preço de uma impressão de carta
(ID_CARTA, DT_COTACAO). Chave: (ID_CARTA, DT_COTACAO).

TAMANHO ESPERADO: ~1 linha por impressão por data de coleta, ou seja
TB_FATO_CARTAS x número de coletas já acumuladas - a tabela cresce ~1x
TB_FATO_CARTAS por run. Não há fan-out: carta e preço estão no mesmo grão de
impressão e o join é 1:1 por ID_CARTA. As três tabelas LEFT restantes são
agregadas ou deduplicadas antes do join pelo mesmo motivo. A premissa é
verificada de verdade em _declare_primary_key (gold_utils.py), que aborta a
run se (ID_CARTA, DT_COTACAO) repetir ou vier NULA.

VLR_USD/EUR/TIX são o preço DAQUELA impressão, não do nome da carta - uma
reimpressão barata e um original caro são linhas distintas com valores
distintos, que é o que torna SUM/AVG por coleção ou raridade legítimo aqui.
As colunas _FOIL/_ETCHED são outra cotação da MESMA impressão (foil vale
múltiplos do não-foil), por isso são colunas e não linhas - um SUM(VLR_USD)
ignora o valor foil da coleção - some VLR_USD_FOIL à parte se quiser ele.

TABELAS SILVER USADAS (5 de 6):
- TB_FATO_CARTAS (driver): 1 linha por impressão de carta.
- TB_FATO_PRECOS_CARTAS (INNER JOIN por ID_CARTA): histórico de cotação de
  preço no mesmo grão de impressão desta tabela, juntável 1:1 (ver docstring
  de TB_FATO_PRECOS_CARTAS.py). INNER porque DT_COTACAO é parte da chave
  desta tabela Gold - carta sem nenhuma cotação de preço não tem linha
  possível aqui (não há valor artificial pra DT_COTACAO sem mascarar a
  chave). Ver seção de Data Quality abaixo para a contagem de cartas
  excluídas por este motivo.
- TB_DIM_COLECOES (LEFT JOIN por COD_COLECAO): nome/bloco/data de lançamento
  da coleção, denormalizados pro consumidor não precisar de um 2º join.
- TB_FATO_ESCLARECIMENTOS_CARTAS (agregada por ID_ORACLE, LEFT JOIN):
  quantidade e data do esclarecimento de regras mais recente por carta -
  proxy de o quanto uma carta é discutida/tem regra complexa.
- TB_MOV_MIGRACOES_CARTAS (agregada por ID_CARTA_ANTIGO, LEFT JOIN em
  ID_CARTA = ID_CARTA_ANTIGO): resolve se o ID_CARTA desta linha foi
  substituído pela Scryfall (fusão/remoção) e, se sim, aponta o id vigente -
  uso documentado na própria origem (docstring de TB_MOV_MIGRACOES_CARTAS.py:
  "Gold junta por ID_CARTA_ANTIGO/ID_CARTA_CANONICO quando precisar resolver
  uma migracao"). Agregada ANTES do join (1 linha por ID_CARTA_ANTIGO, mais
  recente vence por DT_EXECUCAO/ID_MIGRACAO) porque a mesma carta pode ter
  mais de um evento de migração na Silver - sem agregar, o LEFT JOIN direto
  faria fan-out e duplicaria linhas da Gold, quebrando a PK.

TABELA SILVER *NÃO* USADA (1 de 6) - desvio deliberado, documentado:
- TB_DOM_SIMBOLOS: lista de referência de símbolos de mana individuais
  (COD_SIMBOLO = 1 símbolo, ex. '[U]'), pra decodificar texto de carta
  (DESC_CUSTO_MANA/DESC_CARTA, que concatena vários símbolos numa string só,
  ex. '[2][U][U]'). Juntar aqui exigiria explodir DESC_CUSTO_MANA em tokens
  individuais - muda o grão desta tabela (carta x cotação) para carta x
  símbolo, o que não serve ao propósito de mercado desta Gold. Tem uso real
  (decodificar/exibir símbolo de mana), só não neste grão - ver conversa da
  auditoria pra decisão sobre expor como tabela Gold separada.

REGRA DE NULO (GOLD): categórico/descritivo NULO -> literal 'Nao_Identificado'
(nunca 'NA'/vazio/hífen). Medida (VLR_USD/EUR/TIX) NULA continua NULA - 0
não é válido pra "sem cotação" (mesma semântica já documentada na Silver).
QTD_ESCLARECIMENTOS NULO -> 0 (zero é valor real: carta nunca teve ruling).
Data NULA -> sentinela 1001-01-01. PK (ID_CARTA, DT_COTACAO) nunca é
mascarada - se vier NULA, a run falha em _declare_primary_key (gold_utils.py).
ID_CARTA_CANONICO nunca é NULO: quando não há migração pro ID_CARTA desta
linha, o próprio ID_CARTA já é o canônico (COALESCE de fallback, não FK
mascarada - é a regra de negócio documentada em attach_canonical_id).
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
def setup_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def transform_mercado_cartas_gold(df_cartas, df_colecoes, df_precos, df_esclarecimentos, df_migracoes):
    """
    Transformação Gold via SQL (spark.sql sobre temp views) - join das 5
    tabelas Silver descritas na docstring do notebook.
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando join Gold - TB_FATO_MERCADO_CARTAS...")

    df_cartas.createOrReplaceTempView("_cartas")
    df_colecoes.createOrReplaceTempView("_colecoes")
    df_precos.createOrReplaceTempView("_precos")
    df_esclarecimentos.createOrReplaceTempView("_esclarecimentos")
    df_migracoes.createOrReplaceTempView("_migracoes")

    # DATA QUALITY (pré-join) - as duas exclusões que o INNER JOIN abaixo
    # causa, contadas antes dele pra não dependerem da tabela final gravada.
    # Aborta aqui é abortar barato: nada foi escrito ainda.
    run_data_quality_checks(spark, "pré-join", {
        # Carta sem NENHUMA cotação (o grão exige DT_COTACAO não nula). Sempre
        # > 0: toda run traz carta nova antes do preço dela existir.
        "cartas_excluidas_sem_cotacao_de_preco": ("""
            SELECT COUNT(DISTINCT c.ID_CARTA)
            FROM _cartas c
            LEFT JOIN _precos p ON c.ID_CARTA = p.ID_CARTA
            WHERE p.ID_CARTA IS NULL
        """, None),

        # Lado espelho: preço de impressão que não está em TB_FATO_CARTAS.
        # card_prices filtra por releaseDate e cards filtra por código de
        # coleção (/sets), então token, promo e art series entram no preço e
        # não na carta. São mantidos de propósito na Silver - cotação é o único
        # dado não reproduzível do pipeline.
        #
        # ponytail: 12000 é tripwire, não especificação. A baseline medida é
        # 7476 (~12,5% dos preços); o limite existe pra pegar a mudança de
        # regime - um filtro de coleção que quebrou faria isso saltar pra
        # dezenas de milhares e a task continuaria verde. Quando estourar por
        # motivo legítimo (coleção nova grande), mede de novo e sobe o número.
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

    # 1 linha por ID_CARTA_ANTIGO (a carta pode ter mais de 1 evento de
    # migracao na Silver) - sem isso o LEFT JOIN abaixo faria fan-out e
    # duplicaria linha da Gold, quebrando a PK (ID_CARTA, DT_COTACAO).
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

    df_final = spark.sql("""
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
            p.DT_INGESTAO AS DT_COTACAO,
            p.VLR_USD,
            p.VLR_USD_FOIL,
            p.VLR_USD_ETCHED,
            p.VLR_EUR,
            p.VLR_EUR_FOIL,
            p.VLR_TIX,
            COALESCE(e.QTD_ESCLARECIMENTOS, 0) AS QTD_ESCLARECIMENTOS,
            COALESCE(e.DT_ULTIMO_ESCLARECIMENTO, DATE'1001-01-01') AS DT_ULTIMO_ESCLARECIMENTO,
            COALESCE(mig.ID_CARTA_CANONICO, c.ID_CARTA) AS ID_CARTA_CANONICO,
            CASE WHEN mig.ID_CARTA_ANTIGO IS NOT NULL THEN 'Sim' ELSE 'Nao' END AS FLG_ID_CARTA_MIGRADO,
            YEAR(p.DT_INGESTAO) AS ANO_COTACAO,
            MONTH(p.DT_INGESTAO) AS MES_COTACAO
        FROM _cartas c
        INNER JOIN _precos p ON c.ID_CARTA = p.ID_CARTA
        LEFT JOIN _colecoes col ON c.COD_COLECAO = col.COD_COLECAO
        LEFT JOIN _esclarecimentos_agg e ON c.ID_ORACLE = e.ID_ORACLE
        LEFT JOIN _migracoes_resolvidas mig ON c.ID_CARTA = mig.ID_CARTA_ANTIGO
    """)

    logger.info(f"Transformação Gold concluída: {df_final.count()} registros")
    return df_final


# =============================================================================
# CONFIGURACAO
# =============================================================================
config = create_manual_config(get_secret("catalog_name"), get_secret("s3_bucket"))
setup_unity_catalog(config['catalog_name'], config['schema_gold'])

# COMMAND ----------

# =============================================================================
# AUDITORIA - INICIO DO RUN
# =============================================================================
audit_run = start_audit_run()
audit_status = "SUCESSO"

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO GOLD_UTILS
# =============================================================================
processor = GoldTableProcessor("TB_FATO_MERCADO_CARTAS", config)

df_cartas = processor.extract_from_silver("TB_FATO_CARTAS")
df_colecoes = processor.extract_from_silver("TB_DIM_COLECOES")
df_precos = processor.extract_from_silver("TB_FATO_PRECOS_CARTAS")
df_esclarecimentos = processor.extract_from_silver("TB_FATO_ESCLARECIMENTOS_CARTAS")
df_migracoes = processor.extract_from_silver("TB_MOV_MIGRACOES_CARTAS")

qtd_lidos = (
    df_cartas.count() + df_colecoes.count() + df_precos.count()
    + df_esclarecimentos.count() + df_migracoes.count()
)

df_gold = transform_mercado_cartas_gold(df_cartas, df_colecoes, df_precos, df_esclarecimentos, df_migracoes)
qtd_processados = df_gold.count()

try:
    processor.save_gold_table(
        df_gold,
        partition_cols=["ANO_COTACAO", "MES_COTACAO"],
        key_column=["ID_CARTA", "DT_COTACAO"],
        table_comment=get_table_comment("TB_FATO_MERCADO_CARTAS"),
        column_comments=get_column_comments("TB_FATO_MERCADO_CARTAS")
    )
except RuntimeError:
    audit_status = "FALHA_DQ_PK"
    raise

# COMMAND ----------

# =============================================================================
# DATA QUALITY (pós-carga) E AUDITORIA - FIM DO RUN
# =============================================================================
full_table_name = f"{config['catalog_name']}.{config['schema_gold']}.TB_FATO_MERCADO_CARTAS"

dq_resultados = {}
try:
    dq_resultados = run_data_quality_checks(spark, full_table_name, {
        # Chave do fato vinda direto da Silver, sem COALESCE. Linha de preço
        # sem oracle_id é fato sem chave - não existe valor tolerável.
        "fk_null_id_oracle": f"SELECT COUNT(*) FROM {full_table_name} WHERE ID_ORACLE IS NULL",

        # Todos esses passam por COALESCE ou pela regra de nulo da Silver, então
        # NULL aqui é a regra tendo falhado, não dado faltando.
        "null_residual_categorico": f"""SELECT COUNT(*) FROM {full_table_name}
            WHERE NME_CARTA IS NULL OR NME_TIPO_CARTA IS NULL OR NME_RARIDADE IS NULL
               OR NME_CATEGORIA_COR IS NULL OR COD_CORES IS NULL
               OR NME_COLECAO IS NULL OR NME_BLOCO IS NULL""",

        # Preço negativo não existe no Scryfall: se apareceu, foi transformação.
        "valor_negativo_preco": f"""SELECT COUNT(*) FROM {full_table_name}
            WHERE VLR_USD < 0 OR VLR_EUR < 0 OR VLR_TIX < 0
               OR VLR_USD_FOIL < 0 OR VLR_USD_ETCHED < 0 OR VLR_EUR_FOIL < 0""",

        # Informativo: é o COALESCE de coleção entrando em ação (carta cuja
        # COD_COLECAO não veio em /sets). Sem baseline medida ainda - vira
        # limite quando a primeira run com esse código disser quanto é hoje.
        "fk_colecao_nao_encontrada": (
            f"SELECT COUNT(*) FROM {full_table_name} WHERE NME_COLECAO = 'Nao_Identificado'",
            None,
        ),

        # Informativo por natureza: carta que a Scryfall renumerou. Cresce com
        # o tempo e nunca volta a zero.
        "cartas_com_id_migrado": (
            f"SELECT COUNT(*) FROM {full_table_name} WHERE FLG_ID_CARTA_MIGRADO = 'Sim'",
            None,
        ),
    })
except DataQualityError as erro_dq:
    audit_status = "FALHA_DQ"
    dq_resultados = erro_dq.resultados
    raise
finally:
    # finally e nao depois do try: sem isso a run que aborta no DQ nao deixa
    # linha de auditoria nenhuma, e a unica prova do que aconteceu vira o log
    # do cluster - que nao sobrevive ao fim do job.
    record_gold_audit(
        spark, config['catalog_name'], config['schema_gold'], "TB_FATO_MERCADO_CARTAS",
        audit_run,
        qtd_lidos=qtd_lidos,
        qtd_processados=qtd_processados,
        qtd_inseridos_atualizados=qtd_processados,
        dq_resultados=dq_resultados,
        status=audit_status
    )

# =============================================================================
# VALIDACAO E LOGS
# =============================================================================
print(f"Processamento concluído com sucesso!")
print(f"Registros processados: {qtd_processados}")
print(f"Colunas finais: {df_gold.columns}")
