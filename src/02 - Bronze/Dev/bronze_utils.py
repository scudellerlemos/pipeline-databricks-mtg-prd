# Databricks notebook source
# ============================================================================
# BRONZE UTILS - Funções compartilhadas pelos notebooks de Bronze
# ============================================================================
"""
Requer infraestrutura comum já carregada no notebook via:
    %run "../../00 - Common/Dev/base_utils"
Use %run ./bronze_utils para importar no notebook, DEPOIS do %run acima.

Escopo desta camada (Bronze): EL puro (Extract & Load) da Stage (S3/Parquet)
para Delta, com metadados técnicos de rastreabilidade. Sem regra de negócio,
sem renomeação/padronização de colunas (isso é Silver), sem deduplicação por
chave de negócio (o mesmo card_id com price diferente em runs diferentes é
histórico legítimo, não duplicata) e sem MERGE/upsert (que colapsaria esse
histórico) - só APPEND. Preserva o schema de origem 1:1, adicionando apenas
source_file/bronze_run_id/bronze_ingestion_timestamp por cima.

Idempotência: identifica arquivos da Stage já carregados por identidade de
arquivo (source_file), não por SELECT DISTINCT nos dados de negócio - reprocessa
só o que a Stage gravou de novo desde a última execução da Bronze.
"""

import json
import uuid
from datetime import datetime, timezone

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, current_timestamp, lit

# get_secret / setup_unity_catalog vêm de base_utils.py, que o notebook
# chamador deve importar via %run ANTES deste arquivo (ver docstring acima).
# Não fazemos %run aninhado aqui: o lint estático de notebooks só resolve
# %run um nível, então um %run dentro deste arquivo vira texto Python
# inválido quando inlined por ele (mesma razão em silver_utils.py/gold_utils.py).


# ============================================================================
# EXTRACT - IDENTIFICAÇÃO DE ARQUIVOS NOVOS NA STAGE
# ============================================================================

def list_stage_files(dbutils, s3_stage_path, stage_table_name):
    """Lista os arquivos Parquet da Stage pertencentes a stage_table_name
    (pasta própria em S3_STAGE_PATH/{stage_table_name}/, ver save_to_parquet
    em ingestion_utils.py).

    df.write.save(path) grava `path` como um DIRETÓRIO - dbutils.fs.ls devolve
    seu nome com "/" no final (ex.: "2026_09_15_cards.parquet/"), daí o
    rstrip("/") antes do endswith(".parquet").

    Diretório sem part-file dentro é escrita que começou e não commitou (sobra
    só o marcador _started_* do protocolo de commit). Ignorar aqui, senão ele
    entra em new_files e spark.read.parquet quebra a Bronze inteira com
    UNABLE_TO_INFER_SCHEMA por causa de um resto de run antiga.
    """
    table_path = f"{s3_stage_path}/{stage_table_name}"
    all_files = dbutils.fs.ls(table_path)
    dirs = [f.path for f in all_files if f.name.rstrip("/").endswith(".parquet")]
    validos = []
    for d in dirs:
        if any(i.name.endswith(".parquet") for i in dbutils.fs.ls(d)):
            validos.append(d)
        else:
            print(f"[stage] ignorando escrita nao commitada: {d}")
    return sorted(validos)


def normalize_path(path):
    """Remove o esquema de URI e desce ao nível do diretório ".parquet" para
    comparação de identidade entre list_stage_files (devolve o diretório) e
    _metadata.file_path (aponta pro part-file dentro dele, ex.:
    ".../2026_09_15_cards.parquet/part-00000-xxx.snappy.parquet") - sem essa
    normalização, a comparação de idempotência nunca bateria.
    """
    path = path.split("://", 1)[-1]
    if ".parquet/" in path:
        path = path.split(".parquet/", 1)[0] + ".parquet"
    return path


def get_already_loaded_files(spark, delta_path):
    """Arquivos de Stage já carregados nesta tabela Bronze, via source_file.
    O DISTINCT aqui não é deduplicação de negócio (proibida na Bronze) - é a
    identificação de arquivo/run exigida para idempotência.

    Só engole AnalysisException (tabela/path ainda não existe - 1a carga);
    qualquer outro erro sobe, senão a run reingeriria e duplicaria todo o
    histórico. collect() fica dentro do try porque .load() é lazy em Spark
    Connect - o PATH_NOT_FOUND só estoura quando essa action roda.
    """
    try:
        df = spark.read.format("delta").load(delta_path)
        return {normalize_path(row.source_file) for row in df.select("source_file").distinct().collect()}
    except AnalysisException:
        return set()


# ============================================================================
# SCHEMA - LOG DE DIVERGÊNCIA (SEM BLOQUEAR EVOLUÇÃO ADITIVA)
# ============================================================================

def log_schema_diff(spark, delta_path, incoming_df):
    """Loga colunas novas/ausentes/com tipo diferente vs. a tabela Bronze atual.

    Não bloqueia a escrita: colunas novas são aceitas via mergeSchema (schema
    evolution aditiva), colunas ausentes neste lote ficam NULL nas linhas novas
    sem apagar as antigas. Incompatibilidade real de tipo é rejeitada pelo
    próprio Delta na escrita (AnalysisException) - aqui é só log para
    diagnóstico, sem duplicar essa validação.
    """
    try:
        existing_fields = {f.name: str(f.dataType) for f in spark.read.format("delta").load(delta_path).schema.fields}
    except AnalysisException:
        existing_fields = {}

    incoming_fields = {f.name: str(f.dataType) for f in incoming_df.schema.fields}
    new_cols = sorted(c for c in incoming_fields if c not in existing_fields)
    missing_cols = sorted(c for c in existing_fields if c not in incoming_fields)
    type_changed = sorted(
        c for c in incoming_fields
        if c in existing_fields and incoming_fields[c] != existing_fields[c]
    )

    if not existing_fields:
        print(f"[schema] primeira carga - {len(incoming_fields)} colunas")
    if new_cols:
        print(f"[schema] colunas novas neste lote (schema evolution): {new_cols}")
    if missing_cols:
        print(f"[schema] colunas ausentes neste lote (preservadas como NULL nas linhas existentes): {missing_cols}")
    if type_changed:
        print(f"[schema] ALERTA tipos divergentes (a escrita falha se for incompatível de verdade): {type_changed}")


# ============================================================================
# LOAD - APPEND PURO (SEM MERGE/UPSERT) + REGISTRO NO UNITY CATALOG
# ============================================================================

def ensure_unity_catalog_table(spark, full_table_name, delta_path, table_comment=None):
    """Registra a tabela externa Delta no Unity Catalog se ainda não existir.

    Só cria - nunca ALTER/DROP automático aqui. Se a tabela já existe, deixa
    como está (preserva qualquer modificação manual feita fora do pipe).
    """
    if not spark.catalog.tableExists(full_table_name):
        comment = table_comment or "Camada Bronze - dado bruto da Stage, 1:1, sem regra de negócio"
        spark.sql(f"""
            CREATE TABLE {full_table_name}
            USING DELTA
            LOCATION '{delta_path}'
            COMMENT '{_escape_sql_string(comment)}'
        """)
        print(f"Tabela Unity Catalog criada: {full_table_name}")


def _escape_sql_string(value):
    # ponytail: testado ao vivo - Spark SQL nao trata '' (dobrar aspas, padrao
    # ANSI) como aspas literal dentro de um single-quoted string; ele fecha a
    # string na primeira aspa e abre outra logo em seguida, gerando dois
    # literais adjacentes = ParseException. Backslash e o que o parser aceita.
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_documentation(spark, full_table_name, table_comment=None, column_comments=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Metadados apenas (não reescreve dado) - seguro rodar em toda execução,
    inclusive numa tabela já existente e comentada, pra manter em sincronia
    com bronze_column_docs.py sem precisar de uma migração separada. Colunas
    em column_comments que ainda não existem na tabela (schema evolution
    futura) são silenciosamente ignoradas.
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


def append_to_bronze(df, delta_path, full_table_name, table_comment=None):
    """Escreve por APPEND (cria a tabela Delta automaticamente na 1a carga)."""
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .save(delta_path))
    ensure_unity_catalog_table(df.sparkSession, full_table_name, delta_path, table_comment)


# ============================================================================
# CONTROLE DE EXECUÇÃO
# ============================================================================
# Um JSON por run em {s3_bronze_path}/_control/{bronze_table_name}/{run_id}.json -
# mesmo padrão da Stage (ver ingestion_utils.py), com os campos pedidos para
# a Bronze: run_id, tabela, datas, registros lidos/gravados, arquivos
# processados, registros rejeitados (sempre 0 - Bronze nunca descarta nada),
# status e erro.

def start_bronze_run(table_name):
    return {
        "run_id": uuid.uuid4().hex[:12],
        "table": table_name,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
        "files_processed": 0,
        "records_read": 0,
        "records_written": 0,
        "rejected_records": 0,
    }


def finish_bronze_run(dbutils, run, s3_bronze_path, status, error=None):
    started_at = datetime.fromisoformat(run["started_at"])
    finished_at = datetime.now(timezone.utc)

    run["finished_at"] = finished_at.isoformat()
    run["duration_seconds"] = round((finished_at - started_at).total_seconds(), 1)
    run["status"] = status
    run["error"] = error

    # ponytail: duplicado de ingestion_utils.finish_run em vez de compartilhado
    # via base_utils.py - um nome definido por um %run não fica visível dentro
    # de função de outro arquivo também %run (NameError). Mantido
    # self-contained até achar uma forma de compartilhar que sobreviva a isso.
    control_dir = f"{s3_bronze_path}/_control/{run['table']}"
    control_path = f"{control_dir}/{run['run_id']}.json"
    try:
        dbutils.fs.mkdirs(control_dir)
        dbutils.fs.put(control_path, json.dumps(run, default=str), overwrite=True)
    except Exception as e:
        print(f"Aviso: falha ao gravar controle de execução em {control_path}: {e}")

    print(
        f"[{run['table']}] run={run['run_id']} status={status} "
        f"arquivos_processados={run['files_processed']} "
        f"registros_lidos={run['records_read']} registros_gravados={run['records_written']}"
        + (f" erro={error}" if error else "")
    )
    return run


# ============================================================================
# ORQUESTRAÇÃO
# ============================================================================

def run_bronze_ingestion(spark, dbutils, catalog_name, schema_name,
                          bronze_table_name, stage_table_name,
                          s3_stage_path, s3_bronze_path,
                          table_comment=None, column_comments=None):
    """EL completo: identifica arquivos novos da Stage -> lê -> adiciona
    metadados técnicos -> append na Bronze (schema evolution aditiva) ->
    garante a tabela no Unity Catalog -> grava o controle de execução.

    Idempotente: se não há arquivo novo da Stage desde a última execução,
    não escreve nada e a run fecha como SUCCESS com 0 registros.

    table_comment/column_comments (ver bronze_column_docs.py) documentam a
    tabela no Unity Catalog. Aplicados também no caminho "nada a fazer" pra
    tabela já existente pegar comentário novo/alterado sem depender de
    escrever dado novo.
    """
    run = start_bronze_run(bronze_table_name)
    delta_path = f"{s3_bronze_path}/{bronze_table_name}"
    full_table_name = f"{catalog_name}.{schema_name}.{bronze_table_name}"

    try:
        all_files = list_stage_files(dbutils, s3_stage_path, stage_table_name)
        already_loaded = get_already_loaded_files(spark, delta_path)
        new_files = [f for f in all_files if normalize_path(f) not in already_loaded]
        run["files_processed"] = len(new_files)

        if not new_files:
            # Documenta aqui (só neste caminho) pra tabela já existente pegar
            # comentário novo/alterado mesmo sem escrever dado novo - evita
            # repetir a mesma chamada logo abaixo, depois do append.
            if spark.catalog.tableExists(full_table_name):
                apply_table_documentation(spark, full_table_name, table_comment, column_comments)
            print(f"[{bronze_table_name}] Nenhum arquivo novo da Stage - nada a fazer (idempotente).")
            finish_bronze_run(dbutils, run, s3_bronze_path, "SUCCESS")
            return None, run

        print(f"[{bronze_table_name}] Arquivos novos da Stage: {len(new_files)}")
        # input_file_name() não é suportado em Unity Catalog com Shared/User
        # Isolation ([UC_COMMAND_NOT_SUPPORTED.WITH_RECOMMENDATION]) - o
        # substituto recomendado é a coluna oculta _metadata.file_path.
        df = spark.read.parquet(*new_files) \
            .withColumn("source_file", col("_metadata.file_path")) \
            .withColumn("bronze_run_id", lit(run["run_id"])) \
            .withColumn("bronze_ingestion_timestamp", current_timestamp()) \
            .cache()

        run["records_read"] = df.count()

        log_schema_diff(spark, delta_path, df)
        append_to_bronze(df, delta_path, full_table_name, table_comment)
        apply_table_documentation(spark, full_table_name, table_comment, column_comments)

        run["records_written"] = run["records_read"]
        finish_bronze_run(dbutils, run, s3_bronze_path, "SUCCESS")
        return df, run

    except Exception as e:
        finish_bronze_run(dbutils, run, s3_bronze_path, "FAILED", error=str(e))
        raise
