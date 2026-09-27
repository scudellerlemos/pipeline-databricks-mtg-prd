# Databricks notebook source
# ============================================================================
# GOLD UTILS - Módulo de Funções Utilitárias para Camada Gold
# ============================================================================
"""
Módulo centralizado com funções utilitárias (config, extract, load, auditoria)
para scripts da camada Gold. Transformação de negócio (join/agregação) fica em
SQL, dentro de cada notebook (spark.sql sobre temp views) - este módulo é só
orquestração, mesmo padrão de silver_utils.py.

ADAPTADO PARA DATABRICKS NOTEBOOKS:
- dbutils e spark são disponíveis globalmente nos notebooks
- SparkSession obtido automaticamente do contexto global
- Requer infraestrutura comum já carregada no notebook via:
  %run "../../00 - Common/Dev/base_utils"
- Use %run ./gold_utils para importar no notebook, DEPOIS do %run acima

EXEMPLO DE USO NO NOTEBOOK:

%run "../../00 - Common/Dev/base_utils"
%run ./gold_utils

config = create_manual_config("meu_catalog", "s3://meu-bucket")
processor = GoldTableProcessor("TB_FATO_MERCADO_CARTAS", config)

df_cartas = processor.extract_from_silver("TB_FATO_CARTAS")
df_gold = processor.transform_data(df_cartas, transform_function)
processor.save_gold_table(df_gold, partition_cols=["ANO_COTACAO", "MES_COTACAO"],
                           key_column=["ID_CARTA", "DT_COTACAO"])
"""

import uuid
from datetime import datetime

from delta.tables import DeltaTable

# ============================================================================
# INFRAESTRUTURA COMUM (Spark session, Unity Catalog, secrets)
# ponytail: mesmo mecanismo de fallback via IPython user_ns de silver_utils.py -
# %run isola cada arquivo no seu próprio namespace antes de mesclar no notebook
# chamador, então funções de base_utils.py não ficam visíveis aqui por import
# comum.
# ============================================================================
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
    """Retorna configuração padrão para scripts Gold com valores padrão seguros"""
    defaults = {
        'catalog_name': 'mtg_dev',
        's3_bucket': 's3://meu-bucket-default',
        's3_gold_prefix': 'gold'
    }

    config = {key: get_secret(key, extra_safe_defaults=defaults) for key in defaults}

    config['schema_silver'] = "silver"
    config['schema_gold'] = "gold"

    return config

def create_manual_config(catalog_name, s3_bucket, s3_gold_prefix=None):
    """
    Cria configuração manual sem usar secrets (para testes/desenvolvimento)

    Example:
        config = create_manual_config("meu_catalog", "s3://meu-bucket")
        processor = GoldTableProcessor("TB_FATO_MERCADO_CARTAS", config)
    """
    return {
        'catalog_name': catalog_name,
        'schema_silver': "silver",
        'schema_gold': "gold",
        's3_bucket': s3_bucket,
        # get_secret e nao a string crua: prd e dev dividem o bucket, entao
        # sem o override de S3_GOLD_PREFIX os dois gravariam Delta no mesmo
        # caminho. O argumento explicito continua vencendo.
        's3_gold_prefix': s3_gold_prefix or get_secret("s3_gold_prefix", "gold")
    }

# ============================================================================
# FUNÇÕES DE EXTRAÇÃO DA SILVER
# ============================================================================
def extract_from_silver(catalog, table_name_silver):
    """EXTRACT: lê dados de uma tabela da camada Silver"""
    spark_session = get_spark_session()
    silver_table = f"{catalog}.silver.{table_name_silver}"
    # Não engolir a exceção: um "return None" silencioso esconde a causa real
    # (ex.: tabela Silver renomeada/inexistente) atrás de um erro genérico de
    # coluna faltando mais adiante no join.
    df = spark_session.table(silver_table)
    print(f"Extraídos {df.count()} registros da Silver: {silver_table}")
    return df

# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG (mesmo padrão de silver_utils.py)
#
# Duplicada de propósito em vez de extraída pra base_utils.py: função definida
# num arquivo %run'd não fica visível como variável livre dentro de outro
# arquivo %run'd - extrair quebraria a chamada em runtime com NameError (ver
# docstring de silver_utils.py).
# ============================================================================
def _escape_sql_string(value):
    # Spark SQL não trata '' (dobrar aspas) como aspas literal dentro de um
    # single-quoted string - backslash é o que o parser aceita.
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_documentation(spark, full_table_name, table_comment=None, column_comments=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Metadados apenas (não reescreve dado) - seguro rodar em toda execução,
    inclusive numa tabela já existente e comentada, pra manter em sincronia
    com gold_column_docs.py sem precisar de uma migração separada.
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
    violada, em vez de mascarar a chave com valor artificial (nunca fazer
    isso numa PK/FK - ver silver_utils.py para a mesma regra).
    """
    pk_name = f"pk_{table_name.lower()}"

    null_sums = ", ".join(f"sum(case when `{k}` is null then 1 else 0 end) as `{k}`" for k in key_cols)
    key_concat = "concat_ws('', " + ", ".join(f"cast(`{k}` as string)" for k in key_cols) + ")"
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
def save_to_gold(df_final, catalog, schema, table_name, s3_gold_path,
                  partition_cols=None, key_column=None,
                  table_comment=None, column_comments=None):
    """
    LOAD: grava df_final na camada Gold (Delta + Unity Catalog).

    Mesma estratégia de save_to_silver (full load na 1a carga, MERGE INTO
    idempotente por key_column nas seguintes) - Gold aqui é derivada
    determinística da Silver, então não há necessidade de uma estratégia de
    carga diferente. Sem order_by_col: a chave de Gold (ID_CARTA, DT_COTACAO)
    já é única por construção do join (ver docstring do notebook), não há
    empate de chave a desempatar.

    Args:
        df_final (DataFrame): DataFrame final para salvar
        catalog, schema, table_name (str): identificação da tabela no Unity Catalog
        s3_gold_path (str): caminho/bucket S3 base para Gold (com ou sem "s3://")
        partition_cols (list, optional): colunas para particionamento
        key_column (str or list, optional): coluna(s) chave para merge incremental
        table_comment (str, optional): descrição de negócio da tabela (ver
            gold_column_docs.py).
        column_comments (dict, optional): {nome_coluna: descrição de negócio}
            (ver gold_column_docs.py).
    """
    if not s3_gold_path.startswith("s3://"):
        s3_gold_path = f"s3://{s3_gold_path}"
    delta_path = f"{s3_gold_path}/{table_name}"
    full_table_name = f"{catalog}.{schema}.{table_name}"
    spark_session = get_spark_session()

    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    if key_column:
        key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
        # Dedup por key_column ANTES da 1a carga (sem MERGE pra dedupar): mesma
        # razão de save_to_silver - dado duplicado na 1a carga vira duplicata
        # permanente na tabela.
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

        current_cols = set(f.name for f in DeltaTable.forPath(spark_session, delta_path).toDF().schema.fields)
        new_cols = set(df_final.columns)
        if current_cols != new_cols:
            print(f"⚠️ Schema de {full_table_name} mudou: colunas removidas={sorted(current_cols - new_cols)}, "
                  f"colunas novas={sorted(new_cols - current_cols)}.")

        # <=> em vez de = : equality nula-segura, senão uma chave nula nunca daria
        # match e a linha seria reinserida a cada execução (duplicando o dado).
        merge_condition = " AND ".join(f"gold.{k} <=> novo.{k}" for k in key_cols)

        # withSchemaEvolution() no merge builder: coluna nova some sozinha,
        # sem precisar de migração manual. Exige Delta Lake 3.1+ (DBR 15.2+).
        delta_table = DeltaTable.forPath(spark_session, delta_path)
        (
            delta_table.alias("gold")
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

    final_table_comment = table_comment
    key_cols = None
    if key_column:
        key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
        key_note = f"Chave única: {', '.join(key_cols)}."
        final_table_comment = f"{table_comment} {key_note}" if table_comment else key_note

    apply_table_documentation(spark_session, full_table_name, final_table_comment, column_comments)

    if key_cols:
        _declare_primary_key(spark_session, full_table_name, table_name, key_cols)

    print("Dados salvos com sucesso na camada Gold!")


# ============================================================================
# DATA QUALITY E AUDITORIA
# Não existe helper de auditoria compartilhado (Silver/Gold não têm - só a
# Stage tem control table própria, _control/{table}/{run_id}.json, fora do
# escopo deste módulo) - construído mínimo aqui, direto em Delta/SQL.
# ============================================================================
class DataQualityError(RuntimeError):
    """DQ estourou o limite. Carrega .resultados pra auditoria ainda ser gravada."""

    def __init__(self, mensagem, resultados):
        super().__init__(mensagem)
        self.resultados = resultados


def run_data_quality_checks(spark_session, rotulo, checks):
    """
    Roda uma lista de checagens de DQ (cada uma um SELECT que retorna 1 linha/
    1 coluna com uma contagem), loga o resultado e ABORTA se alguma passar do
    limite dela.

    Cada valor do dict de checks diz qual e o limite:

        "nome": query                 -> limite 0: qualquer ocorrencia aborta.
        "nome": (query, 12000)        -> aborta acima de 12000 (tripwire).
        "nome": (query, None)         -> so loga, contagem esperada e > 0.

    O limite e obrigatorio de pensar porque DQ que so imprime e DQ que ninguem
    le: a task fica verde e o numero so aparece pra quem for atras do log do
    cluster, que nem sempre sobrevive.

    Roda TODAS as checagens antes de abortar - saber que tres coisas quebraram
    de uma vez e mais util do que descobrir uma por run.

    Args:
        rotulo (str): de onde vem a checagem, so pro log (nome da tabela,
            "pré-join", etc).
        checks (dict): {nome: query} ou {nome: (query, limite)}

    Returns:
        dict: {nome_da_checagem: contagem} - usado no resumo de auditoria.

    Raises:
        DataQualityError: se alguma contagem passou do limite.
    """
    resultados = {}
    estourados = []
    for nome, check in checks.items():
        query, limite = check if isinstance(check, tuple) else (check, 0)
        contagem = spark_session.sql(query).collect()[0][0] or 0
        resultados[nome] = contagem

        if limite is None:
            nivel = "ℹ️"
        elif contagem > limite:
            nivel = "❌"
            estourados.append(f"{nome}={contagem} (limite {limite})")
        else:
            nivel = "✅"
        print(f"{nivel} DQ [{rotulo}] {nome}: {contagem}")

    if estourados:
        raise DataQualityError(
            f"DQ [{rotulo}] estourou o limite: " + "; ".join(estourados), resultados
        )
    return resultados


def start_audit_run():
    """Abre um run de auditoria - id determinístico por execução + timestamp de início."""
    return {"id_execucao": str(uuid.uuid4()), "dt_inicio": datetime.now()}


def record_gold_audit(spark_session, catalog, schema, table_name, audit_run,
                       qtd_lidos, qtd_processados, qtd_inseridos_atualizados,
                       dq_resultados, status):
    """
    Fecha o run de auditoria e grava 1 linha em `{catalog}.{schema}.TB_AUDITORIA_GOLD`
    (criada automaticamente na 1a chamada). 1 linha por execução de notebook Gold,
    nunca é atualizada depois de gravada (log de execução, não estado).
    """
    dt_fim = datetime.now()
    duracao_segundos = (dt_fim - audit_run["dt_inicio"]).total_seconds()
    full_audit_table = f"{catalog}.{schema}.TB_AUDITORIA_GOLD"

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_audit_table} (
            ID_EXECUCAO STRING,
            NME_TABELA STRING,
            DT_INICIO TIMESTAMP,
            DT_FIM TIMESTAMP,
            QTD_SEGUNDOS_DURACAO DOUBLE,
            QTD_LIDOS BIGINT,
            QTD_PROCESSADOS BIGINT,
            QTD_INSERIDOS_ATUALIZADOS BIGINT,
            DESC_DQ_RESULTADO STRING,
            DESC_STATUS STRING
        ) USING DELTA
    """)

    dq_resumo = ", ".join(f"{k}={v}" for k, v in dq_resultados.items()) if dq_resultados else "sem checagens"

    spark_session.sql(f"""
        INSERT INTO {full_audit_table} VALUES (
            '{audit_run["id_execucao"]}',
            '{_escape_sql_string(table_name)}',
            '{audit_run["dt_inicio"].isoformat()}',
            '{dt_fim.isoformat()}',
            {duracao_segundos},
            {qtd_lidos},
            {qtd_processados},
            {qtd_inseridos_atualizados},
            '{_escape_sql_string(dq_resumo)}',
            '{_escape_sql_string(status)}'
        )
    """)

    print(f"Auditoria registrada em {full_audit_table} (run {audit_run['id_execucao']}, {duracao_segundos:.1f}s).")


# ============================================================================
# CLASSE AUXILIAR
# ============================================================================
class GoldTableProcessor:
    """Classe para processar tabelas Gold com padrões comuns"""

    def __init__(self, table_name, config=None):
        self.table_name = table_name
        self.config = config or get_standard_config()
        self.spark = get_spark_session()
        self.s3_gold_path = f"{self.config['s3_bucket']}/{self.config['s3_gold_prefix']}"

        setup_unity_catalog(self.config['catalog_name'], self.config['schema_gold'])

    def extract_from_silver(self, silver_table_name):
        """Extrai dados de uma tabela Silver"""
        return extract_from_silver(self.config['catalog_name'], silver_table_name)

    def transform_data(self, df, transform_function, **kwargs):
        """Aplica função de transformação personalizada (lógica em SQL, no notebook)"""
        if transform_function:
            return transform_function(df, **kwargs)
        return df

    def save_gold_table(self, df, partition_cols=None, key_column=None,
                         table_comment=None, column_comments=None):
        """Salva tabela na Gold com configurações padrão"""
        save_to_gold(
            df_final=df,
            catalog=self.config['catalog_name'],
            schema=self.config['schema_gold'],
            table_name=self.table_name,
            s3_gold_path=self.s3_gold_path,
            partition_cols=partition_cols,
            key_column=key_column,
            table_comment=table_comment,
            column_comments=column_comments
        )

        print(f"✅ {self.table_name} criada com sucesso!")
        print(f"Tabela criada: {self.config['catalog_name']}.{self.config['schema_gold']}.{self.table_name}")
