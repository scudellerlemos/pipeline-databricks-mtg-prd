# Databricks notebook source
# ============================================================================
# BRONZE UTILS - Funções compartilhadas pelos notebooks de Bronze
# ============================================================================
"""
Importar no notebook com %run ./bronze_utils, DEPOIS de
    %run "../../00 - Common/Dev/base_utils"

Bronze = EL puro da Stage (S3/Parquet) para Delta, só APPEND. Schema de origem
1:1 + source_file/bronze_run_id/bronze_ingestion_timestamp. Sem regra de
negócio, renomeação, dedup ou MERGE: o mesmo id com valores diferentes em runs
diferentes é histórico, não duplicata.

Idempotência por arquivo (source_file): só carrega arquivos da Stage que
ainda não estão na tabela.
"""

import json
import uuid
from datetime import datetime, timezone

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, current_timestamp, lit

# Sem %run aninhado aqui: o lint estático de notebooks só resolve %run de um
# nível. Por isso o notebook importa base_utils antes deste arquivo.


# ============================================================================
# EXTRACT - IDENTIFICAÇÃO DE ARQUIVOS NOVOS NA STAGE
# ============================================================================

def list_stage_files(dbutils, s3_stage_path, stage_table_name):
    """Lista os diretórios .parquet em {s3_stage_path}/{stage_table_name}/.

    Cada arquivo da Stage é um diretório, e dbutils.fs.ls devolve o nome com
    "/" no final - daí o rstrip. Diretório sem part-file (só o marcador
    _started_*) é escrita não commitada e é ignorado; senão read.parquet
    falha com UNABLE_TO_INFER_SCHEMA.
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
    """Remove o esquema de URI e corta no diretório ".parquet".

    list_stage_files devolve o diretório; _metadata.file_path aponta pro
    part-file dentro dele. Normalizados, os dois podem ser comparados.
    """
    path = path.split("://", 1)[-1]
    if ".parquet/" in path:
        path = path.split(".parquet/", 1)[0] + ".parquet"
    return path


def get_already_loaded_files(spark, delta_path):
    """Arquivos de Stage já carregados nesta tabela Bronze (via source_file).

    Só trata AnalysisException (tabela ainda não existe - 1a carga); qualquer
    outro erro sobe, senão a run reingeriria todo o histórico. O collect()
    fica dentro do try porque .load() é lazy em Spark Connect e o
    PATH_NOT_FOUND só aparece na action.
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

    Só log, não bloqueia: colunas novas entram via mergeSchema, ausentes ficam
    NULL nas linhas novas. Tipo incompatível o próprio Delta rejeita na escrita.
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
        print(f"[schema] colunas ausentes neste lote (ficam NULL nas linhas novas; linhas existentes preservadas): {missing_cols}")
    if type_changed:
        print(f"[schema] ALERTA tipos divergentes (a escrita falha se for incompatível de verdade): {type_changed}")


# ============================================================================
# LOAD - APPEND PURO (SEM MERGE/UPSERT) + REGISTRO NO UNITY CATALOG
# ============================================================================

def ensure_unity_catalog_table(spark, full_table_name, delta_path, table_comment=None):
    """Registra a tabela externa Delta no Unity Catalog se ainda não existir.
    Se já existe, não altera nada."""
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
    # Spark SQL não aceita '' (padrão ANSI) como aspa literal - dá
    # ParseException. O escape que funciona é com backslash.
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_documentation(spark, full_table_name, table_comment=None, column_comments=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog.

    Só metadado, então roda em toda execução pra manter a tabela em sincronia
    com bronze_column_docs.py. Colunas que ainda não existem na tabela são
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
# Um JSON por run em {s3_bronze_path}/_control/{bronze_table_name}/{run_id}.json,
# mesmo padrão da Stage. rejected_records é sempre 0: a Bronze não descarta nada.

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

    # Duplicado de ingestion_utils.finish_run: função de um arquivo carregado
    # por %run não enxerga nomes de outro %run (NameError).
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
    """EL completo: arquivos novos da Stage -> metadados técnicos -> append na
    Bronze -> tabela no Unity Catalog -> controle de execução.

    Sem arquivo novo, não escreve nada e fecha como SUCCESS com 0 registros.
    table_comment/column_comments vêm de bronze_column_docs.py.
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
            # Atualiza os comentários mesmo sem dado novo.
            if spark.catalog.tableExists(full_table_name):
                apply_table_documentation(spark, full_table_name, table_comment, column_comments)
            print(f"[{bronze_table_name}] Nenhum arquivo novo da Stage - nada a fazer (idempotente).")
            finish_bronze_run(dbutils, run, s3_bronze_path, "SUCCESS")
            return None, run

        print(f"[{bronze_table_name}] Arquivos novos da Stage: {len(new_files)}")
        # _metadata.file_path em vez de input_file_name(), que o Unity Catalog
        # não suporta em Shared/User Isolation.
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
