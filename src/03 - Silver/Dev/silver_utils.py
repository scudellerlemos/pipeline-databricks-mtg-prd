# Databricks notebook source
# ============================================================================
# SILVER UTILS - Módulo de Funções Utilitárias para Camada Silver
# ============================================================================
"""
Funções utilitárias (config, extract, load) da camada Silver. A transformação
de negócio fica em SQL dentro de cada notebook; aqui é só orquestração.

Requer base_utils carregado antes no notebook:
  %run "../../00 - Common/Dev/base_utils"
  %run ./silver_utils

EXEMPLO DE USO NO NOTEBOOK:

%run "../../00 - Common/Dev/base_utils"
%run ./silver_utils

config = criar_config_manual("meu_catalog", "s3://meu-bucket")
processador = SilverTableProcessor("TB_FATO_CARTAS", config)

df_bronze = processador.extrair_da_bronze("cards")
df_silver = processador.transformar_dados(df_bronze, funcao_transformacao)
processador.salvar_tabela_silver(df_silver, colunas_particao=["ANO_INGESTAO", "MES_INGESTAO"],
                                 coluna_chave="ID_CARTA", coluna_ordenacao="DT_INGESTAO")
"""

import unicodedata

from pyspark.sql.functions import col, hash, row_number, trim, initcap, regexp_replace, udf, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================================
# INFRAESTRUTURA COMUM (Spark session, Unity Catalog, secrets)
# obter_sessao_spark / configurar_unity_catalog / obter_segredo vêm de base_utils.py,
# carregado pelo notebook chamador. Sem %run aninhado aqui: o lint de
# notebooks só resolve %run de um nível.
#
# Funções definidas num arquivo %run'd não enxergam nomes de outro arquivo
# %run'd, então buscamos no namespace do IPython. Fora do Databricks (pytest
# local) get_ipython() é None e o bloco é ignorado.
try:
    obter_sessao_spark, obter_segredo, configurar_unity_catalog
except NameError:
    try:
        import IPython
        _namespace_usuario = IPython.get_ipython().user_ns
        obter_sessao_spark = _namespace_usuario["obter_sessao_spark"]
        obter_segredo = _namespace_usuario["obter_segredo"]
        configurar_unity_catalog = _namespace_usuario["configurar_unity_catalog"]
    except Exception:
        pass
# ============================================================================

# ============================================================================
# FUNÇÕES DE CONFIGURAÇÃO
# ============================================================================
def criar_config_manual(catalogo, bucket_s3, prefixo_s3_silver=None):
    """
    Monta a config da Silver a partir de catalog e bucket informados.

    Example:
        config = criar_config_manual("meu_catalog", "s3://meu-bucket")
        processador = SilverTableProcessor("TB_FATO_CARTAS", config)
    """
    return {
        'catalog_name': catalogo,
        'schema_bronze': "bronze",
        'schema_silver': "silver",
        's3_bucket': bucket_s3,
        # Argumento explicito vence; senao usa MTG_S3_SILVER_PREFIX via obter_segredo.
        's3_silver_prefix': prefixo_s3_silver or obter_segredo("s3_silver_prefix", "silver")
    }

# ============================================================================
# NORMALIZAÇÃO DE VALOR DE ATRIBUTO
# Title_Case por palavra, "_" no lugar de espaço e sem acento (ex.:
# "mana vermelha" -> "Mana_Vermelha"). Usada só em colunas de texto
# categórico/nome, não em Id_/Cod_/Url_ nem em texto livre longo.
# NULL, string vazia e o sentinela 'NA' passam sem alteração.
# ============================================================================
def _remover_acentos(texto):
    if texto is None:
        return None
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')

remover_acentos = udf(_remover_acentos, StringType())

def normalizar_valor(coluna):
    titulo = regexp_replace(initcap(remover_acentos(trim(coluna))), ' ', '_')
    passa_direto = coluna.isNull() | (trim(coluna) == '') | (coluna == 'NA')
    return when(passa_direto, coluna).otherwise(titulo)


def normalizar_valores(df, colunas):
    """Aplica normalizar_valor() numa lista de colunas do DataFrame, uma de cada vez."""
    for c in colunas:
        df = df.withColumn(c, normalizar_valor(col(c)))
    return df

# ============================================================================
# FUNÇÕES DE EXTRAÇÃO DA BRONZE
# ============================================================================
def extrair_da_bronze(catalogo, nome_tabela_bronze):
    """EXTRACT: lê dados da camada Bronze"""
    sessao_spark = obter_sessao_spark()
    tabela_bronze = f"{catalogo}.bronze.{nome_tabela_bronze}"
    # Sem try/except de propósito: erro de leitura (ex.: TABLE_OR_VIEW_NOT_FOUND)
    # deve derrubar a task Silver com a mensagem original.
    df = sessao_spark.table(tabela_bronze)
    print(f"Extraídos {df.count()} registros da Bronze: {tabela_bronze}")
    return df

# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG (mesmo padrão de bronze_utils.py)
#
# Duplicada de propósito: função de um arquivo %run'd não é visível dentro de
# outro arquivo %run'd (ver nota no topo), então não dá pra mover pra base_utils.
# ============================================================================
def _escapar_string_sql(valor):
    # Spark SQL não aceita '' como aspas literal; o escape é com backslash.
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def aplicar_documentacao_tabela(spark, nome_completo_tabela, comentario_tabela=None, comentarios_colunas=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Só metadado (não reescreve dado), então roda em toda execução. Colunas em
    comentarios_colunas que não existem na tabela são ignoradas.
    """
    if comentario_tabela:
        spark.sql(f"COMMENT ON TABLE {nome_completo_tabela} IS '{_escapar_string_sql(comentario_tabela)}'")

    if comentarios_colunas:
        colunas_existentes = {f.name for f in spark.table(nome_completo_tabela).schema.fields}
        for nome_coluna, comentario in comentarios_colunas.items():
            if nome_coluna in colunas_existentes:
                spark.sql(
                    f"ALTER TABLE {nome_completo_tabela} "
                    f"ALTER COLUMN `{nome_coluna}` COMMENT '{_escapar_string_sql(comentario)}'"
                )


def _declarar_chave_primaria(sessao_spark, nome_completo_tabela, nome_tabela, colunas_chave):
    """Declara a PRIMARY KEY de colunas_chave em nome_completo_tabela no Unity Catalog.

    Unity Catalog exige NOT NULL na PK mas não garante unicidade, então um
    SELECT valida nulos e duplicatas antes e levanta RuntimeError com a contagem.
    DROP + ADD da constraint para ser idempotente entre execuções.
    """
    nome_pk = f"pk_{nome_tabela.lower()}"

    somas_nulos = ", ".join(f"sum(case when `{k}` is null then 1 else 0 end) as `{k}`" for k in colunas_chave)
    concat_chave = "concat_ws('', " + ", ".join(f"cast(`{k}` as string)" for k in colunas_chave) + ")"
    expr_qtd_duplicadas = f"count(*) - count(distinct {concat_chave}) as __dup_count"
    linha = sessao_spark.sql(
        f"SELECT {somas_nulos}, {expr_qtd_duplicadas} FROM {nome_completo_tabela}"
    ).collect()[0]

    for k in colunas_chave:
        qtd_nulos = linha[k] or 0
        if qtd_nulos > 0:
            raise RuntimeError(
                f"Coluna chave '{k}' de {nome_completo_tabela} tem {qtd_nulos} linha(s) "
                f"com valor NULO - viola a premissa de chave única desta tabela. "
                f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
            )

    qtd_duplicadas = linha["__dup_count"] or 0
    if qtd_duplicadas > 0:
        raise RuntimeError(
            f"Chave ({', '.join(colunas_chave)}) de {nome_completo_tabela} tem {qtd_duplicadas} "
            f"linha(s) duplicada(s) - viola a premissa de chave única desta "
            f"tabela (Unity Catalog não enforca unicidade de PRIMARY KEY). "
            f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
        )

    for k in colunas_chave:
        sessao_spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN `{k}` SET NOT NULL")

    sessao_spark.sql(f"ALTER TABLE {nome_completo_tabela} DROP CONSTRAINT IF EXISTS {nome_pk}")
    sessao_spark.sql(
        f"ALTER TABLE {nome_completo_tabela} ADD CONSTRAINT {nome_pk} "
        f"PRIMARY KEY ({', '.join(colunas_chave)})"
    )


# ============================================================================
# FUNÇÃO DE CARREGAMENTO DELTA/UNITY CATALOG
# ============================================================================
def salvar_na_silver(df_final, catalogo, esquema, nome_tabela, caminho_s3_silver,
                     colunas_particao=None, coluna_chave=None, coluna_ordenacao=None,
                     comentario_tabela=None, comentarios_colunas=None):
    """
    LOAD: grava df_final na camada Silver (Delta + Unity Catalog).

    - Delta ainda não existe no caminho: cria os arquivos (primeira carga).
    - Delta já existe e coluna_chave informado: MERGE incremental via DeltaTable.merge (builder da API Python).
    - Delta já existe e sem coluna_chave: overwrite completo (uso explícito do chamador).
    - Em qualquer caso, garante o registro da tabela no Unity Catalog sem nunca
      sobrescrever dados já gravados (CREATE TABLE IF NOT EXISTS).

    Args:
        df_final (DataFrame): DataFrame final para salvar
        catalogo, esquema, nome_tabela (str): identificação da tabela no Unity Catalog
        caminho_s3_silver (str): caminho/bucket S3 base para Silver (com ou sem "s3://")
        colunas_particao (list, optional): colunas para particionamento
        coluna_chave (str or list, optional): coluna(s) chave para merge incremental
        coluna_ordenacao (str, optional): coluna de recência que decide qual linha
            fica quando o lote tem chave duplicada. Sem ela, usa dropDuplicates
            (linha arbitrária).
        comentario_tabela (str, optional): descrição de negócio da tabela (ver
            silver_column_docs.py). Recebe a nota de chave única ao final.
        comentarios_colunas (dict, optional): {nome_coluna: descrição de negócio}
            (ver silver_column_docs.py).
    """
    if not caminho_s3_silver.startswith("s3://"):
        caminho_s3_silver = f"s3://{caminho_s3_silver}"
    caminho_delta = f"{caminho_s3_silver}/{nome_tabela}"
    nome_completo_tabela = f"{catalogo}.{esquema}.{nome_tabela}"
    sessao_spark = obter_sessao_spark()

    sessao_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema}")

    # Dedup antes de tudo: a primeira carga grava sem MERGE, e uma duplicata
    # gravada ali nunca seria limpa pelos MERGEs seguintes.
    if coluna_chave:
        colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)

        if coluna_ordenacao and coluna_ordenacao in df_final.columns:
            # nulls last: coluna_ordenacao nulo nunca vence um valor preenchido.
            # Desempate por hash das demais colunas, para ser determinístico.
            # Limitação: colisão de hash é possível; se incomodar, desempatar
            # por uma coluna natural (ex.: de ingestão).
            colunas_desempate = [c for c in df_final.columns if c not in colunas_chave and c != coluna_ordenacao]
            colunas_ordem = [col(coluna_ordenacao).desc_nulls_last()]
            if colunas_desempate:
                colunas_ordem.append(hash(*colunas_desempate).desc())
            janela = Window.partitionBy(*colunas_chave).orderBy(*colunas_ordem)
            df_final = df_final.withColumn("_rn_dedup", row_number().over(janela)) \
                                .filter(col("_rn_dedup") == 1).drop("_rn_dedup")
        else:
            df_final = df_final.dropDuplicates(colunas_chave)

    arquivos_existem = DeltaTable.isDeltaTable(sessao_spark, caminho_delta)

    if not arquivos_existem:
        print(f"Delta ainda não existe em {caminho_delta}. Criando (primeira carga).")
        escritor = df_final.write.format("delta").mode("overwrite")
        if colunas_particao:
            escritor = escritor.partitionBy(*colunas_particao)
        escritor.save(caminho_delta)
        print(f"Tabela criada com {df_final.count()} linhas.")

    elif coluna_chave:
        colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)

        # Só loga diferença de schema (metadado, sem scan). Coluna nova entra via
        # withSchemaEvolution(); remoção ou mudança de tipo pode falhar o MERGE.
        colunas_atuais = set(f.name for f in DeltaTable.forPath(sessao_spark, caminho_delta).toDF().schema.fields)
        colunas_novas = set(df_final.columns)
        if colunas_atuais != colunas_novas:
            print(f"Schema de {nome_completo_tabela} mudou: colunas removidas={sorted(colunas_atuais - colunas_novas)}, "
                  f"colunas novas={sorted(colunas_novas - colunas_atuais)}.")

        # <=> (null-safe): com =, chave nula nunca dá match e seria reinserida a cada run.
        condicao_merge = " AND ".join(f"silver.{k} <=> novo.{k}" for k in colunas_chave)

        # withSchemaEvolution() exige Delta Lake 3.1+ (DBR 15.2+).
        tabela_delta = DeltaTable.forPath(sessao_spark, caminho_delta)
        (
            tabela_delta.alias("silver")
            .merge(df_final.alias("novo"), condicao_merge)
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Merge concluído em {nome_completo_tabela}.")

    else:
        print("Tabela Delta já existe mas sem coluna_chave. Fazendo overwrite.")
        df_final.write.format("delta").mode("overwrite").save(caminho_delta)

    sessao_spark.sql(
        f"CREATE TABLE IF NOT EXISTS {nome_completo_tabela} USING DELTA LOCATION '{caminho_delta}'"
    )

    # Comentário da tabela = descrição de negócio + chave única, visível no catalog.
    comentario_final_tabela = comentario_tabela
    colunas_chave = None
    if coluna_chave:
        colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)
        nota_chave = f"Chave única: {', '.join(colunas_chave)}."
        comentario_final_tabela = f"{comentario_tabela} {nota_chave}" if comentario_tabela else nota_chave

    aplicar_documentacao_tabela(sessao_spark, nome_completo_tabela, comentario_final_tabela, comentarios_colunas)

    if colunas_chave:
        _declarar_chave_primaria(sessao_spark, nome_completo_tabela, nome_tabela, colunas_chave)

    print("Dados salvos com sucesso na camada Silver!")

# ============================================================================
# CLASSE AUXILIAR
# ============================================================================
class SilverTableProcessor:
    """Classe para processar tabelas Silver com padrões comuns"""

    def __init__(self, nome_tabela, config):
        self.nome_tabela = nome_tabela
        self.config = config
        self.spark = obter_sessao_spark()
        self.caminho_s3_silver = f"{self.config['s3_bucket']}/{self.config['s3_silver_prefix']}"

        configurar_unity_catalog(self.config['catalog_name'], self.config['schema_silver'])

    def extrair_da_bronze(self, nome_tabela_bronze):
        """Extrai dados da Bronze"""
        return extrair_da_bronze(self.config['catalog_name'], nome_tabela_bronze)

    def transformar_dados(self, df, funcao_transformacao, **kwargs):
        """Aplica função de transformação personalizada (lógica em SQL, no notebook)"""
        if funcao_transformacao:
            return funcao_transformacao(df, **kwargs)
        return df

    def salvar_tabela_silver(self, df, colunas_particao=None, coluna_chave=None, coluna_ordenacao=None,
                             comentario_tabela=None, comentarios_colunas=None):
        """Salva tabela na Silver com configurações padrão"""
        salvar_na_silver(
            df_final=df,
            catalogo=self.config['catalog_name'],
            esquema=self.config['schema_silver'],
            nome_tabela=self.nome_tabela,
            caminho_s3_silver=self.caminho_s3_silver,
            colunas_particao=colunas_particao,
            coluna_chave=coluna_chave,
            coluna_ordenacao=coluna_ordenacao,
            comentario_tabela=comentario_tabela,
            comentarios_colunas=comentarios_colunas
        )

        print(f"{self.nome_tabela} criada com sucesso!")
        print(f"Tabela criada: {self.config['catalog_name']}.{self.config['schema_silver']}.{self.nome_tabela}")
