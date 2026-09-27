# Databricks notebook source
# ============================================================================
# SILVER UTILS - Módulo de Funções Utilitárias para Camada Silver
# ============================================================================
"""
Módulo centralizado com funções utilitárias (config, extract, load) para
scripts da camada Silver. Transformação de negócio fica em SQL, dentro de
cada notebook (spark.sql sobre temp views) - este módulo é só orquestração.

ADAPTADO PARA DATABRICKS NOTEBOOKS:
- dbutils e spark são disponíveis globalmente nos notebooks
- SparkSession obtido automaticamente do contexto global
- Requer infraestrutura comum já carregada no notebook via:
  %run "../../00 - Common/Dev/base_utils"
- Use %run ./silver_utils para importar no notebook, DEPOIS do %run acima

EXEMPLO DE USO NO NOTEBOOK:

%run "../../00 - Common/Dev/base_utils"
%run ./silver_utils

config = create_manual_config("meu_catalog", "s3://meu-bucket")
processor = SilverTableProcessor("TB_FATO_CARTAS", config)

df_bronze = processor.extract_from_bronze("cards")
df_silver = processor.transform_data(df_bronze, transform_function)
processor.save_silver_table(df_silver, partition_cols=["ANO_INGESTAO", "MES_INGESTAO"],
                             key_column="ID_CARTA", order_by_col="DT_INGESTAO")
"""

import unicodedata

from pyspark.sql.functions import col, hash, row_number, trim, initcap, regexp_replace, udf, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================================
# INFRAESTRUTURA COMUM (Spark session, Unity Catalog, secrets)
# get_spark_session / setup_unity_catalog / get_secret vêm de base_utils.py,
# que o notebook chamador deve importar via %run ANTES deste arquivo (ver
# docstring acima). Não fazemos %run aninhado aqui: o lint estático de
# notebooks só resolve %run um nível, então um %run dentro deste arquivo
# vira texto Python inválido quando inlined por ele.
#
# ponytail: %run isola cada arquivo no seu próprio namespace antes de mesclar
# no notebook chamador - funções definidas AQUI (diferente de código de nível
# superior do notebook) não enxergam nomes de base_utils.py por esse merge.
# Puxa do IPython quando isso acontece; fora de um notebook Databricks (ex.:
# pytest local), get_ipython() é None e o bloco é ignorado.
try:
    get_spark_session, get_secret, setup_unity_catalog
except NameError:
    try:
        import IPython
        _user_ns = IPython.get_ipython().user_ns
        get_spark_session = _user_ns["get_spark_session"]
        get_secret = _user_ns["get_secret"]
        setup_unity_catalog = _user_ns["setup_unity_catalog"]
    except Exception:
        pass
# ============================================================================

# ============================================================================
# FUNÇÕES DE CONFIGURAÇÃO
# ============================================================================
def get_standard_config():
    """Retorna configuração padrão para scripts Silver com valores padrão seguros"""
    defaults = {
        'catalog_name': 'mtg_dev',
        's3_bucket': 's3://meu-bucket-default',
        's3_silver_prefix': 'silver'
    }

    config = {key: get_secret(key, extra_safe_defaults=defaults) for key in defaults}

    config['schema_bronze'] = "bronze"
    config['schema_silver'] = "silver"

    return config

def create_manual_config(catalog_name, s3_bucket, s3_silver_prefix=None):
    """
    Cria configuração manual sem usar secrets (para testes/desenvolvimento)

    Example:
        config = create_manual_config("meu_catalog", "s3://meu-bucket")
        processor = SilverTableProcessor("TB_FATO_CARTAS", config)
    """
    return {
        'catalog_name': catalog_name,
        'schema_bronze': "bronze",
        'schema_silver': "silver",
        's3_bucket': s3_bucket,
        # get_secret e nao a string crua: prd e dev dividem o bucket, entao
        # sem o override de S3_SILVER_PREFIX os dois gravariam Delta no mesmo
        # caminho. O argumento explicito continua vencendo.
        's3_silver_prefix': s3_silver_prefix or get_secret("s3_silver_prefix", "silver")
    }

# ============================================================================
# NORMALIZAÇÃO DE VALOR DE ATRIBUTO
# Title_Case por palavra + "_" no lugar de espaço + sem acento (ex.:
# "mana vermelha" -> "Mana_Vermelha"), aplicada via normalizar_valores() só
# nas colunas de texto categórico/nome (NÃO em Id_/Cod_/Url_ nem em texto
# livre longo, que têm convenção de case própria - ver docstring de cada
# tabela). NULL, string vazia e o sentinela 'NA' passam direto, sem
# Title-casear.
#
# ponytail: sem-acento via unicodedata.normalize NFKD + encode ASCII/ignore -
# idiom padrão do stdlib pra tirar acento de qualquer caractere, em vez de um
# mapa de tradução digitado à mão (translate()) que só cobre os caracteres
# que alguém lembrou de listar.
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
def extract_from_bronze(catalog, table_name_bronze):
    """EXTRACT: lê dados da camada Bronze"""
    spark_session = get_spark_session()
    bronze_table = f"{catalog}.bronze.{table_name_bronze}"
    # Não engolir a exceção aqui: um "return None" silencioso fazia o erro
    # real (ex.: tabela Bronze inexistente/corrompida) só aparecer 2 camadas
    # depois, na Gold, como UNRESOLVED_COLUMN sem nenhuma pista da causa. Deixar
    # propagar mostra a mensagem original (ex.: TABLE_OR_VIEW_NOT_FOUND) direto
    # no erro da task Silver.
    df = spark_session.table(bronze_table)
    print(f"Extraídos {df.count()} registros da Bronze: {bronze_table}")
    return df

# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG (mesmo padrão de bronze_utils.py)
#
# Duplicada de propósito em vez de extraída pra base_utils.py: função definida
# num arquivo %run'd não fica visível como variável livre dentro de outro
# arquivo %run'd (mesma causa do %run isolation citado no topo deste
# arquivo) - extrair quebraria a chamada em runtime com NameError.
# ============================================================================
def _escape_sql_string(value):
    # Spark SQL não trata '' (dobrar aspas, convenção ANSI) como aspas literal
    # dentro de um single-quoted string - fecha a string na 1a aspa e abre
    # outra logo em seguida, virando ParseException. Backslash é o que o
    # parser aceita (achado real ao rodar Bronze no Databricks).
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_documentation(spark, full_table_name, table_comment=None, column_comments=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Metadados apenas (não reescreve dado) - seguro rodar em toda execução,
    inclusive numa tabela já existente e comentada, pra manter em sincronia
    com silver_column_docs.py sem precisar de uma migração separada. Colunas
    em column_comments que ainda não existem na tabela são silenciosamente
    ignoradas.
    """
    if table_comment:
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{_escape_sql_string(table_comment)}'")

    if column_comments:
        existing_columns = {f.name for f in spark.table(full_table_name).schema.fields}
        for column_name, comment in column_comments.items():
            if column_name in existing_columns:
                spark.sql(
                    f"ALTER TABLE {full_table_name} "
                    f"ALTER COLUMN `{column_name}` COMMENT '{_escape_sql_string(comment)}'"
                )


def _declare_primary_key(spark_session, full_table_name, table_name, key_cols):
    """Declara a PRIMARY KEY de key_cols em full_table_name no Unity Catalog.

    Unity Catalog exige NOT NULL na PK mas não enforca unicidade - um único
    SELECT valida NULOs e duplicatas de uma vez (1 scan da tabela) e propaga
    RuntimeError com a contagem exata se a premissa de chave única for
    violada, em vez de derrubar a run com o erro genérico do engine.

    DROP+ADD constraint em vez de só ADD: idempotente entre execuções (ADD
    CONSTRAINT sem IF NOT EXISTS falharia na 2ª run).
    """
    pk_name = f"pk_{table_name.lower()}"

    null_sums = ", ".join(f"sum(case when `{k}` is null then 1 else 0 end) as `{k}`" for k in key_cols)
    key_concat = "concat_ws('', " + ", ".join(f"cast(`{k}` as string)" for k in key_cols) + ")"
    dup_count_expr = f"count(*) - count(distinct {key_concat}) as __dup_count"
    row = spark_session.sql(
        f"SELECT {null_sums}, {dup_count_expr} FROM {full_table_name}"
    ).collect()[0]

    for k in key_cols:
        null_count = row[k] or 0
        if null_count > 0:
            raise RuntimeError(
                f"Coluna chave '{k}' de {full_table_name} tem {null_count} linha(s) "
                f"com valor NULO - viola a premissa de chave única desta tabela. "
                f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
            )

    dup_count = row["__dup_count"] or 0
    if dup_count > 0:
        raise RuntimeError(
            f"Chave ({', '.join(key_cols)}) de {full_table_name} tem {dup_count} "
            f"linha(s) duplicada(s) - viola a premissa de chave única desta "
            f"tabela (Unity Catalog não enforca unicidade de PRIMARY KEY). "
            f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
        )

    for k in key_cols:
        spark_session.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN `{k}` SET NOT NULL")

    spark_session.sql(f"ALTER TABLE {full_table_name} DROP CONSTRAINT IF EXISTS {pk_name}")
    spark_session.sql(
        f"ALTER TABLE {full_table_name} ADD CONSTRAINT {pk_name} "
        f"PRIMARY KEY ({', '.join(key_cols)})"
    )


# ============================================================================
# FUNÇÃO DE CARREGAMENTO DELTA/UNITY CATALOG
# ============================================================================
def save_to_silver(df_final, catalog, schema, table_name, s3_silver_path,
                    partition_cols=None, key_column=None, order_by_col=None,
                    table_comment=None, column_comments=None):
    """
    LOAD: grava df_final na camada Silver (Delta + Unity Catalog).

    - Delta ainda não existe no caminho: cria os arquivos (primeira carga).
    - Delta já existe e key_column informado: MERGE INTO incremental via SQL puro.
    - Delta já existe e sem key_column: overwrite completo (uso explícito do chamador).
    - Em qualquer caso, garante o registro da tabela no Unity Catalog sem nunca
      sobrescrever dados já gravados (CREATE TABLE IF NOT EXISTS).

    Args:
        df_final (DataFrame): DataFrame final para salvar
        catalog, schema, table_name (str): identificação da tabela no Unity Catalog
        s3_silver_path (str): caminho/bucket S3 base para Silver (com ou sem "s3://")
        partition_cols (list, optional): colunas para particionamento
        key_column (str or list, optional): coluna(s) chave para merge incremental
        order_by_col (str, optional): coluna de recência usada para escolher
            deterministicamente qual linha sobrevive quando o lote tem mais de uma
            linha para a mesma key_column. Sem ela, duplicatas de chave no
            lote são resolvidas de forma não-determinística, mas ficam logadas.
        table_comment (str, optional): descrição de negócio da tabela (ver
            silver_column_docs.py). Combinada com a sinalização de chave única.
        column_comments (dict, optional): {nome_coluna: descrição de negócio}
            (ver silver_column_docs.py).
    """
    if not s3_silver_path.startswith("s3://"):
        s3_silver_path = f"s3://{s3_silver_path}"
    delta_path = f"{s3_silver_path}/{table_name}"
    full_table_name = f"{catalog}.{schema}.{table_name}"
    spark_session = get_spark_session()

    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Dedup por key_column ANTES de decidir entre primeira carga e merge: a
    # primeira carga escreve df_final direto (sem MERGE), então se não dedupar
    # aqui uma key duplicada no batch de ingestão vira duplicata permanente na
    # tabela - o MERGE de runs seguintes só dedupa o batch novo, nunca limpa
    # duplicata que já está na tabela.
    if key_column:
        key_cols = [key_column] if isinstance(key_column, str) else list(key_column)

        if order_by_col and order_by_col in df_final.columns:
            # nulls last é proposital - order_by_col nulo nunca deve vencer um valor
            # não-nulo mais antigo por acidente.
            # tie-break: hash das colunas restantes garante escolha determinística mesmo
            # com order_by_col empatado; ponytail: colisão de hash é possível (não-única),
            # revisitar com um tie-break natural (ex. coluna de ingestão) se isso doer.
            tie_break_cols = [c for c in df_final.columns if c not in key_cols and c != order_by_col]
            order_cols = [col(order_by_col).desc_nulls_last()]
            if tie_break_cols:
                order_cols.append(hash(*tie_break_cols).desc())
            window = Window.partitionBy(*key_cols).orderBy(*order_cols)
            df_final = df_final.withColumn("_rn_dedup", row_number().over(window)) \
                                .filter(col("_rn_dedup") == 1).drop("_rn_dedup")
        else:
            df_final = df_final.dropDuplicates(key_cols)

    files_exist = DeltaTable.isDeltaTable(spark_session, delta_path)

    if not files_exist:
        print(f"Delta ainda não existe em {delta_path}. Criando (primeira carga).")
        writer = df_final.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(delta_path)
        print(f"Tabela criada com {df_final.count()} linhas.")

    elif key_column:
        key_cols = [key_column] if isinstance(key_column, str) else list(key_column)

        # comparação de schema é metadado (sem scan de dados) - só visibilidade;
        # autoMerge (abaixo) resolve colunas novas sozinho, remoções/mudanças de tipo
        # podem falhar o MERGE e aparecem no log em vez de silenciosas
        current_cols = set(f.name for f in DeltaTable.forPath(spark_session, delta_path).toDF().schema.fields)
        new_cols = set(df_final.columns)
        if current_cols != new_cols:
            print(f"⚠️ Schema de {full_table_name} mudou: colunas removidas={sorted(current_cols - new_cols)}, "
                  f"colunas novas={sorted(new_cols - current_cols)}.")

        # <=> em vez de = : equality nula-segura, senão uma chave nula nunca daria
        # match e a linha seria reinserida a cada execução (duplicando o dado).
        merge_condition = " AND ".join(f"silver.{k} <=> novo.{k}" for k in key_cols)

        # withSchemaEvolution() no merge builder: coluna nova some sozinha,
        # sem precisar de migração manual. Exige Delta Lake 3.1+ (DBR 15.2+).
        delta_table = DeltaTable.forPath(spark_session, delta_path)
        (
            delta_table.alias("silver")
            .merge(df_final.alias("novo"), merge_condition)
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Merge concluído em {full_table_name}.")

    else:
        print("Tabela Delta já existe mas sem key_column. Fazendo overwrite.")
        df_final.write.format("delta").mode("overwrite").save(delta_path)

    spark_session.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{delta_path}'"
    )

    # Comentário de tabela combina a descrição de negócio (table_comment, ver
    # silver_column_docs.py) com a sinalização de chave única DENTRO da
    # tabela - quem abre o catalog vê sem precisar ler o notebook.
    final_table_comment = table_comment
    key_cols = None
    if key_column:
        key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
        key_note = f"Chave única: {', '.join(key_cols)}."
        final_table_comment = f"{table_comment} {key_note}" if table_comment else key_note

    apply_table_documentation(spark_session, full_table_name, final_table_comment, column_comments)

    if key_cols:
        _declare_primary_key(spark_session, full_table_name, table_name, key_cols)

    print("Dados salvos com sucesso na camada Silver!")

# ============================================================================
# CLASSE AUXILIAR
# ============================================================================
class SilverTableProcessor:
    """Classe para processar tabelas Silver com padrões comuns"""

    def __init__(self, table_name, config=None):
        self.table_name = table_name
        self.config = config or get_standard_config()
        self.spark = get_spark_session()
        self.s3_silver_path = f"{self.config['s3_bucket']}/{self.config['s3_silver_prefix']}"

        setup_unity_catalog(self.config['catalog_name'], self.config['schema_silver'])

    def extract_from_bronze(self, bronze_table_name):
        """Extrai dados da Bronze"""
        return extract_from_bronze(self.config['catalog_name'], bronze_table_name)

    def transform_data(self, df, transform_function, **kwargs):
        """Aplica função de transformação personalizada (lógica em SQL, no notebook)"""
        if transform_function:
            return transform_function(df, **kwargs)
        return df

    def save_silver_table(self, df, partition_cols=None, key_column=None, order_by_col=None,
                           table_comment=None, column_comments=None):
        """Salva tabela na Silver com configurações padrão"""
        save_to_silver(
            df_final=df,
            catalog=self.config['catalog_name'],
            schema=self.config['schema_silver'],
            table_name=self.table_name,
            s3_silver_path=self.s3_silver_path,
            partition_cols=partition_cols,
            key_column=key_column,
            order_by_col=order_by_col,
            table_comment=table_comment,
            column_comments=column_comments
        )

        print(f"✅ {self.table_name} criada com sucesso!")
        print(f"Tabela criada: {self.config['catalog_name']}.{self.config['schema_silver']}.{self.table_name}")
