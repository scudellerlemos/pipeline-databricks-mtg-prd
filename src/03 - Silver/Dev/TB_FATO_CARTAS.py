# Databricks notebook source
# =============================================================================
# CAMADA SILVER - CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para processamento da tabela TB_FATO_CARTAS.
Transformação e limpeza de dados da Bronze para Silver.

CLASSIFICAÇÃO DAMA-DMBOK: Fato - uma linha por impressão de carta (grão), com
medidas quantitativas (QTD_CUSTO_MANA, QTD_CORES) e chaves estrangeiras
implícitas pra dimensões (COD_COLECAO -> TB_DIM_COLECOES). Daí o prefixo
TB_FATO_ e o nome sem o segmento redundante "SILVER" (já implícito no schema
silver.* do Unity Catalog).

CHAVE ÚNICA: ID_CARTA (ver save_silver_table no fim do notebook) - NOT NULL
por natureza, então a constraint PRIMARY KEY no Unity Catalog é aplicada com
sucesso (além do COMMENT ON TABLE sempre gravado).

CONVENÇÃO DE NOME/CASE DE COLUNA: nome de coluna 100% MAIÚSCULO (prefixo
semântico ID_/NME_/DESC_/COD_/DT_/QTD_/NUM_/URL_ + resto do nome, ex.:
NME_CARTA). Valor de atributo (colunas de nome/categoria) em Title_Case por
palavra, sem acento, espaço virando "_" (ex.: "mana vermelha" ->
"Mana_Vermelha") - ver normalizar_valor() em silver_utils.py. Exceção:
ID_/COD_/URL_* e texto livre longo mantêm sua própria convenção de case.
Bronze/Ingestion continuam passthrough 1:1 da fonte.

TRANSFORMAÇÃO DE NEGÓCIO EM SQL: toda a lógica de limpeza/derivação roda em
uma única spark.sql() com CTEs (_renomeado -> _limpo -> _sem_delimitador ->
SELECT final). A SELECT final precisa de uma CTE própria porque lê
COD_CORES/DESC_CUSTO_MANA já limpos - uma coluna não pode referenciar, no
mesmo SELECT, outra coluna calculada ali do lado.

FALLBACK DE ID_ORACLE: partições da Bronze gravadas antes da coluna
oracle_id existir não a têm - nesse caso ID_ORACLE fica NULL em vez de
quebrar o pipeline.

PREÇO E MIGRAÇÃO NÃO ESTÃO AQUI: esta tabela tem grão só de "impressão de
carta". Histórico de preço e id canônico pós-migração vivem em tabelas
Silver próprias (TB_FATO_PRECOS_CARTAS, TB_MOV_MIGRACOES_CARTAS) - junte por
ID_CARTA nas duas (preço é por impressão, mesmo grão desta tabela).

REGRA "SEM ( ) { } NO DADO SILVER": texto de carta/custo de mana/legalidades
vem da Scryfall com notação de símbolo entre chaves (ex.: "{2}{U}{U}") e
texto de lembrete entre parênteses, e legalities é um dict serializado. Tudo
convertido pra notação com colchetes ([...]) na CTE _sem_delimitador -
símbolos comuns viram um rótulo legível (ex.: "[White]"), o resto usa um
catch-all genérico que preserva o conteúdo trocando só o delimitador.
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

def transform_cards_silver(df):
    """
    Transformação específica para tabela Cartas, via uma única spark.sql()
    com CTEs (WITH ... AS (...)).
    """
    if not df:
        return None

    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformações específicas para Cartas...")

    df.createOrReplaceTempView("_cards_bronze")

    # oracle_id pode não existir em partições antigas da Bronze - fallback
    # NULL tipado evita quebrar o pipeline; as demais colunas sempre existem.
    if "oracle_id" in df.columns:
        oracle_id_select = "oracle_id AS ID_ORACLE"
    else:
        logger.warning("Coluna oracle_id ausente na Bronze cards - ID_ORACLE ficará NULL.")
        oracle_id_select = "CAST(NULL AS STRING) AS ID_ORACLE"

    # WITH em CTEs (não temp views): cada CTE materializa a etapa anterior,
    # então a SELECT final lê COD_CORES/DESC_CUSTO_MANA já limpos, não os
    # valores crus da Bronze.
    # Backslash duplicado no regex (\\{ etc.): Spark desfaz um \\ simples antes
    # de resolver a string; dobrar garante que sobra um só pro regex.
    # String raw, não f-string: a query tem chaves literais (regex de mana)
    # que um f-string tentaria interpretar como placeholder - troca
    # ID_ORACLE via .replace() depois de montar a string.
    query_cartas = r"""
        WITH _renomeado AS (
            -- Bronze -> nome PT-BR, com filtro de 5 anos no WHERE (avalia
            -- contra ingestion_timestamp antes do SELECT aplicar o alias).
            SELECT
                id AS ID_CARTA,
                __ORACLE_ID_SELECT__,
                name AS NME_CARTA,
                manaCost AS DESC_CUSTO_MANA,
                cmc AS QTD_CUSTO_MANA,
                colors AS COD_CORES,
                colorIdentity AS COD_IDENTIDADE_COR,
                type AS NME_TIPO_CARTA,
                types AS DESC_TIPOS,
                subtypes AS DESC_SUBTIPOS,
                rarity AS NME_RARIDADE,
                `set` AS COD_COLECAO,
                setName AS NME_COLECAO,
                text AS DESC_CARTA,
                artist AS NME_ARTISTA,
                number AS NUM_COLECIONADOR,
                power AS NME_FORCA,
                toughness AS NME_RESISTENCIA,
                layout AS NME_DISPOSICAO_CARTA,
                multiverseid AS ID_MULTIVERSO,
                imageUrl AS URL_IMAGEM,
                variations AS COD_VARIACOES,
                foreignNames AS DESC_NOMES_ESTRANGEIROS,
                printings AS DESC_IMPRESSOES,
                originalText AS DESC_CARTA_ORIGINAL,
                originalType AS NME_TIPO_ORIGINAL,
                legalities AS DESC_LEGALIDADES,
                ingestion_timestamp AS DT_INGESTAO,
                source AS NME_FONTE,
                endpoint AS DESC_URL_ORIGEM,
                source_file AS DESC_ARQUIVO_ORIGEM,
                bronze_run_id AS ID_EXECUCAO_BRONZE,
                bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE
            FROM _cards_bronze
            WHERE ingestion_timestamp >= add_months(current_date(), -60)
        ),

        -- limpeza/derivação de negócio (regex/CASE). Title_Case/sem-acento
        -- fica pro normalizar_valores() em Python, depois desta query.
        _limpo AS (
            SELECT
                * EXCEPT (DESC_CARTA, DESC_CUSTO_MANA, QTD_CUSTO_MANA, NME_FORCA,
                          NME_RESISTENCIA, COD_COLECAO, DESC_IMPRESSOES, COD_VARIACOES,
                          COD_CORES, COD_IDENTIDADE_COR, DESC_SUBTIPOS, DESC_TIPOS,
                          NME_TIPO_CARTA, DT_INGESTAO),

                CASE WHEN DESC_CARTA IS NULL OR DESC_CARTA = '' THEN 'NA' ELSE trim(DESC_CARTA) END AS DESC_CARTA,
                CASE WHEN DESC_CUSTO_MANA IS NULL OR DESC_CUSTO_MANA = '' THEN 'NA' ELSE trim(DESC_CUSTO_MANA) END AS DESC_CUSTO_MANA,
                coalesce(QTD_CUSTO_MANA, 0) AS QTD_CUSTO_MANA,
                -- NME_FORCA/NME_RESISTENCIA podem valer "*"/"1+*" (poder variável,
                -- ex.: Tarmogoyf) - fallback '0' como string, não int (coalesce
                -- com int forçaria cast e quebraria em valor não-numérico).
                coalesce(NME_FORCA, '0') AS NME_FORCA,
                coalesce(NME_RESISTENCIA, '0') AS NME_RESISTENCIA,
                upper(COD_COLECAO) AS COD_COLECAO,  -- normaliza case: TB_DIM_COLECOES tambem faz upper() em COD_COLECAO, join entre as duas depende do mesmo case
                regexp_replace(DESC_IMPRESSOES, '\\[|\\]|"', '') AS DESC_IMPRESSOES,
                regexp_replace(COD_VARIACOES, '\\[|\\]|"', '') AS COD_VARIACOES,
                regexp_replace(COD_CORES, '\\[|\\]|"', '') AS COD_CORES,
                regexp_replace(COD_IDENTIDADE_COR, '\\[|\\]|"', '') AS COD_IDENTIDADE_COR,
                regexp_replace(DESC_SUBTIPOS, '\\[|\\]|"', '') AS DESC_SUBTIPOS,
                CASE WHEN DESC_TIPOS IS NULL OR DESC_TIPOS = '' THEN 'NA' ELSE DESC_TIPOS END AS DESC_TIPOS,

                -- NME_TIPO_CARTA / DESC_DETALHE_TIPO_CARTA: Planeswalker é tipo
                -- isolado; "—" (em dash) separa tipo principal de subtipo
                -- descritivo. As duas colunas saem da mesma origem.
                CASE
                    WHEN NME_TIPO_CARTA IS NULL THEN NULL
                    WHEN lower(NME_TIPO_CARTA) LIKE '%planeswalker%' THEN 'Planeswalker'
                    WHEN instr(NME_TIPO_CARTA, '—') > 0 THEN trim(split(NME_TIPO_CARTA, '—', 2)[0])
                    ELSE trim(NME_TIPO_CARTA)
                END AS NME_TIPO_CARTA,
                CASE
                    WHEN NME_TIPO_CARTA IS NULL THEN NULL
                    WHEN lower(NME_TIPO_CARTA) LIKE '%planeswalker%' THEN NME_TIPO_CARTA
                    WHEN instr(NME_TIPO_CARTA, '—') > 0 THEN trim(split(NME_TIPO_CARTA, '—', 2)[1])
                    ELSE 'NA'
                END AS DESC_DETALHE_TIPO_CARTA,

                to_timestamp(DT_INGESTAO) AS DT_INGESTAO
            FROM _renomeado
        ),

        -- COD_CORES/DESC_SUBTIPOS colorless-default e eliminação de
        -- "(" ")" "{" "}" do dado Silver (sinalizam dado não transformado).
        _sem_delimitador AS (
            SELECT
                * EXCEPT (COD_CORES, DESC_SUBTIPOS, DESC_CARTA, DESC_CUSTO_MANA,
                          DESC_CARTA_ORIGINAL, DESC_LEGALIDADES, DESC_NOMES_ESTRANGEIROS),

                CASE WHEN COD_CORES IS NULL OR COD_CORES = '' THEN 'Colorless' ELSE COD_CORES END AS COD_CORES,
                CASE WHEN DESC_SUBTIPOS IS NULL OR DESC_SUBTIPOS = '' THEN 'NA' ELSE DESC_SUBTIPOS END AS DESC_SUBTIPOS,

                -- DESC_CARTA: símbolos comuns viram rótulo legível ([White] etc.),
                -- catch-all genérico cobre o resto ({...} e (...) restantes).
                -- ponytail: não trata "{"/"(" aninhados (não ocorre em carta real).
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(
                regexp_replace(DESC_CARTA, '\\{W\\}', '[White]'),
                                    '\\{U\\}', '[Blue]'),
                                    '\\{B\\}', '[Black]'),
                                    '\\{R\\}', '[Red]'),
                                    '\\{G\\}', '[Green]'),
                                    '\\{C\\}', '[Colorless]'),
                                    '\\{X\\}', '[X]'),
                                    '\\{T\\}', '[Tap]'),
                                    '\\{Q\\}', '[Untap]'),
                                    '\\{S\\}', '[Snow]'),
                                    '\\{E\\}', '[Energy]'),
                                    '\\{([^}]*)\\}', '[$1]'),
                                    '\\(([^)]*)\\)', '[$1]') AS DESC_CARTA,

                -- DESC_CUSTO_MANA: notação puramente simbólica (ex.: "{2}{U}{U}") -
                -- só o catch-all genérico já resolve, sem precisar da lista nomeada.
                regexp_replace(
                regexp_replace(DESC_CUSTO_MANA, '\\{([^}]*)\\}', '[$1]'),
                                                 '\\(([^)]*)\\)', '[$1]') AS DESC_CUSTO_MANA,

                -- DESC_CARTA_ORIGINAL: texto pré-errata, mesma notação de DESC_CARTA.
                regexp_replace(
                regexp_replace(DESC_CARTA_ORIGINAL, '\\{([^}]*)\\}', '[$1]'),
                                                      '\\(([^)]*)\\)', '[$1]') AS DESC_CARTA_ORIGINAL,

                -- DESC_LEGALIDADES: dict serializado (json.dumps) vindo direto da
                -- Bronze - chaves de dict viram colchete pela mesma regra.
                regexp_replace(
                regexp_replace(DESC_LEGALIDADES, '\\{([^}]*)\\}', '[$1]'),
                                                  '\\(([^)]*)\\)', '[$1]') AS DESC_LEGALIDADES,

                -- DESC_NOMES_ESTRANGEIROS: lista de dicts serializada (um dict por
                -- idioma, sem aninhamento) - mesma regra.
                regexp_replace(
                regexp_replace(DESC_NOMES_ESTRANGEIROS, '\\{([^}]*)\\}', '[$1]'),
                                                          '\\(([^)]*)\\)', '[$1]') AS DESC_NOMES_ESTRANGEIROS
            FROM _limpo
        )

        -- NME_CATEGORIA_COR/QTD_CORES vêm de COD_CORES/DESC_CUSTO_MANA já
        -- resolvidos por _sem_delimitador. ANO_INGESTAO/MES_INGESTAO são a
        -- partição física, derivados de DT_INGESTAO.
        SELECT
            *,
            CASE
                WHEN COD_CORES = 'Colorless' THEN 'Colorless'
                WHEN size(split(COD_CORES, ',')) = 1 THEN 'Mono'
                WHEN size(split(COD_CORES, ',')) = 2 THEN 'Dual Color'
                WHEN size(split(COD_CORES, ',')) >= 3 THEN 'Multicolor'
                ELSE 'Mono'
            END AS NME_CATEGORIA_COR,
            CASE
                WHEN DESC_CUSTO_MANA IS NULL OR DESC_CUSTO_MANA = 'NA' THEN 0
                ELSE length(regexp_replace(upper(DESC_CUSTO_MANA), '[^WUBRG]', ''))
            END AS QTD_CORES,
            year(DT_INGESTAO) AS ANO_INGESTAO,
            month(DT_INGESTAO) AS MES_INGESTAO
        FROM _sem_delimitador
    """
    df_silver = spark.sql(query_cartas.replace("__ORACLE_ID_SELECT__", oracle_id_select))

    # Title_Case/"_"/sem-acento (ver normalizar_valor() em silver_utils.py),
    # de uma vez só, depois que a query acima já resolveu todo o resto.
    df_silver = normalizar_valores(df_silver, [
        "NME_CARTA", "NME_ARTISTA", "NME_RARIDADE", "NME_COLECAO",
        "NME_FORCA", "NME_RESISTENCIA", "DESC_SUBTIPOS", "DESC_TIPOS",
        "NME_TIPO_CARTA", "DESC_DETALHE_TIPO_CARTA", "NME_TIPO_ORIGINAL",
        "NME_CATEGORIA_COR",
    ])

    logger.info(f"Transformação Cartas concluída: {df_silver.count()} registros")
    return df_silver

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
processor = SilverTableProcessor("TB_FATO_CARTAS", config)

# Extração da Bronze (cards) e transformação específica.
df_cards_bronze = processor.extract_from_bronze("cards")
df_silver = processor.transform_data(df_cards_bronze, transform_cards_silver)

# Merge incremental por ID_CARTA, particionado por ANO_INGESTAO/MES_INGESTAO.
# order_by_col=DT_INGESTAO: em reprocessamento com linha duplicada na mesma
# chave, mantém a ingestão mais recente em vez de uma linha arbitrária.
processor.save_silver_table(
    df_silver,
    partition_cols=["ANO_INGESTAO", "MES_INGESTAO"],
    key_column="ID_CARTA",
    order_by_col="DT_INGESTAO",
    table_comment=get_table_comment("TB_FATO_CARTAS"),
    column_comments=get_column_comments("TB_FATO_CARTAS")
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
