# Databricks notebook source
# ============================================================================
# GOLD UTILS - Módulo de Funções Utilitárias para Camada Gold
# ============================================================================
"""
Funções utilitárias da camada Gold: config, extract, load e auditoria.
A transformação de negócio fica em SQL dentro de cada notebook; mesmo padrão
de silver_utils.py.

Requer base_utils carregado antes:
  %run "../../00 - Common/Dev/base_utils"
  %run ./gold_utils

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
# %run isola o namespace de cada arquivo, então as funções de base_utils.py
# são buscadas no user_ns do IPython (mesmo esquema de silver_utils.py).
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
        # Override por ambiente via MTG_S3_GOLD_PREFIX; o argumento explicito vence.
        's3_gold_prefix': s3_gold_prefix or get_secret("s3_gold_prefix", "gold")
    }

# ============================================================================
# FUNÇÕES DE EXTRAÇÃO DA SILVER
# ============================================================================
def extract_from_silver(catalog, table_name_silver):
    """EXTRACT: lê dados de uma tabela da camada Silver"""
    spark_session = get_spark_session()
    silver_table = f"{catalog}.silver.{table_name_silver}"
    # Sem try/except: tabela inexistente deve falhar aqui, com a causa real.
    df = spark_session.table(silver_table)
    print(f"Extraídos {df.count()} registros da Silver: {silver_table}")
    return df

# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG
# Duplicada de silver_utils.py de propósito: função de um arquivo %run'd não
# fica visível dentro de outro arquivo %run'd.
# ============================================================================
def _escape_sql_string(value):
    # Spark SQL não aceita '' como aspas escapada; usa backslash.
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_documentation(spark, full_table_name, table_comment=None, column_comments=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Só metadados: roda em toda execução para manter a tabela em sincronia com
    gold_column_docs.py.
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

    Unity Catalog exige NOT NULL na PK mas não garante unicidade, então um
    SELECT valida nulos e duplicatas antes e levanta RuntimeError com a
    contagem se houver. A chave nunca é mascarada com valor artificial.
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

    Full load na 1a carga, MERGE por key_column nas seguintes (como
    save_to_silver). Lote com chave duplicada aborta antes de gravar.

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
        # Duplicata no lote é bug a montante (PK da Silver ou join): aborta em
        # vez de dropDuplicates, que esconderia o problema.
        dup_count = df_final.groupBy(*key_cols).count().filter("count > 1").count()
        if dup_count > 0:
            raise RuntimeError(
                f"Lote de {full_table_name} tem {dup_count} chave(s) "
                f"({', '.join(key_cols)}) duplicada(s) - nada foi gravado. "
                f"Corrija a fonte/transformação."
            )

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
            print(f"Schema de {full_table_name} mudou: colunas removidas={sorted(current_cols - new_cols)}, "
                  f"colunas novas={sorted(new_cols - current_cols)}.")

        # <=> (null-safe): com "=", chave nula nunca casa e seria reinserida a cada run.
        merge_condition = " AND ".join(f"gold.{k} <=> novo.{k}" for k in key_cols)

        # withSchemaEvolution(): coluna nova entra sem migração. Exige Delta 3.1+ (DBR 15.2+).
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
# ============================================================================
class DataQualityError(RuntimeError):
    """DQ estourou o limite. Carrega .resultados pra auditoria ainda ser gravada."""

    def __init__(self, mensagem, resultados):
        super().__init__(mensagem)
        self.resultados = resultados


def run_data_quality_checks(spark_session, rotulo, checks):
    """
    Roda checagens de DQ (cada uma um SELECT que retorna uma contagem), loga
    e aborta se alguma passar do limite. Roda todas antes de abortar.

        "nome": query                 -> limite 0: qualquer ocorrencia aborta.
        "nome": (query, 12000)        -> aborta acima de 12000.
        "nome": (query, None)         -> so loga (contagem esperada > 0).

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
            nivel = "INFO"
        elif contagem > limite:
            nivel = "FALHA"
            estourados.append(f"{nome}={contagem} (limite {limite})")
        else:
            nivel = "OK"
        print(f"{nivel} DQ [{rotulo}] {nome}: {contagem}")

    if estourados:
        raise DataQualityError(
            f"DQ [{rotulo}] estourou o limite: " + "; ".join(estourados), resultados
        )
    return resultados


def start_audit_run():
    """Abre um run de auditoria: id único (uuid4) + timestamp de início."""
    return {"id_execucao": str(uuid.uuid4()), "dt_inicio": datetime.now()}


def record_gold_audit(spark_session, catalog, schema, table_name, audit_run,
                       qtd_lidos, qtd_processados, qtd_inseridos_atualizados,
                       dq_resultados, status):
    """
    Fecha o run e grava 1 linha em `{catalog}.{schema}.TB_AUDITORIA_GOLD`
    (criada na 1a chamada). O notebook chama isto num finally, então toda
    execução é registrada, inclusive as que abortam. Linhas são só inseridas.
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

    def __init__(self, table_name, config):
        self.table_name = table_name
        self.config = config
        self.spark = get_spark_session()
        self.s3_gold_path = f"{self.config['s3_bucket']}/{self.config['s3_gold_prefix']}"

        setup_unity_catalog(self.config['catalog_name'], self.config['schema_gold'])

    def extract_from_silver(self, silver_table_name):
        """Extrai dados de uma tabela Silver"""
        return extract_from_silver(self.config['catalog_name'], silver_table_name)

    def transform_data(self, df, transform_function, **kwargs):
        """Aplica transform_function ao df (a lógica fica no notebook)."""
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

        print(f"{self.table_name} criada com sucesso!")
        print(f"Tabela criada: {self.config['catalog_name']}.{self.config['schema_gold']}.{self.table_name}")
