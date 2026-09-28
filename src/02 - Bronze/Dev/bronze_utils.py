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
# Função de um arquivo %run'd não enxerga nomes de outro %run'd, então as de
# base_utils vêm do namespace do IPython (mesmo esquema de silver_utils.py).
try:
    validar_contrato_esquema, campos_do_esquema, aplicar_documentacao_tabela
except NameError:
    try:
        import IPython
        _namespace_usuario = IPython.get_ipython().user_ns
        validar_contrato_esquema = _namespace_usuario["validar_contrato_esquema"]
        campos_do_esquema = _namespace_usuario["campos_do_esquema"]
        aplicar_documentacao_tabela = _namespace_usuario["aplicar_documentacao_tabela"]
    except Exception:
        pass


# ============================================================================
# EXTRACT - IDENTIFICAÇÃO DE ARQUIVOS NOVOS NA STAGE
# ============================================================================

def listar_arquivos_stage(dbutils, caminho_s3_stage, nome_tabela_stage):
    """Lista os diretórios .parquet em {caminho_s3_stage}/{nome_tabela_stage}/.

    Cada arquivo da Stage é um diretório, e dbutils.fs.ls devolve o nome com
    "/" no final - daí o rstrip. Diretório sem part-file (só o marcador
    _started_*) é escrita não commitada e é ignorado; senão read.parquet
    falha com UNABLE_TO_INFER_SCHEMA.
    """
    caminho_tabela = f"{caminho_s3_stage}/{nome_tabela_stage}"
    todos_arquivos = dbutils.fs.ls(caminho_tabela)
    diretorios = [f.path for f in todos_arquivos if f.name.rstrip("/").endswith(".parquet")]
    validos = []
    for d in diretorios:
        if any(i.name.endswith(".parquet") for i in dbutils.fs.ls(d)):
            validos.append(d)
        else:
            print(f"[stage] ignorando escrita nao commitada: {d}")
    return sorted(validos)


def normalizar_caminho(caminho):
    """Remove o esquema de URI e corta no diretório ".parquet".

    listar_arquivos_stage devolve o diretório; _metadata.file_path aponta pro
    part-file dentro dele. Normalizados, os dois podem ser comparados.
    """
    caminho = caminho.split("://", 1)[-1]
    if ".parquet/" in caminho:
        caminho = caminho.split(".parquet/", 1)[0] + ".parquet"
    return caminho


def obter_arquivos_ja_carregados(spark, caminho_delta):
    """Arquivos de Stage já carregados nesta tabela Bronze (via source_file).

    Só trata AnalysisException (tabela ainda não existe - 1a carga); qualquer
    outro erro sobe, senão a run reingeriria todo o histórico. O collect()
    fica dentro do try porque .load() é lazy em Spark Connect e o
    PATH_NOT_FOUND só aparece na action.
    """
    try:
        df = spark.read.format("delta").load(caminho_delta)
        return {normalizar_caminho(linha.source_file) for linha in df.select("source_file").distinct().collect()}
    except AnalysisException:
        return set()


# ============================================================================
# SCHEMA - CONTRATO (validar_contrato_esquema em base_utils)
# ============================================================================

def validar_esquema_bronze(spark, caminho_delta, nome_completo_tabela, df_entrada,
                           comentarios_colunas=None, permitir_quebra_esquema=False):
    """Coluna nova da Stage entra via mergeSchema; coluna removida ou com tipo
    alterado aborta antes do append (senão viraria NULL nas linhas novas)."""
    try:
        campos_atuais = campos_do_esquema(spark.read.format("delta").load(caminho_delta).schema)
    except AnalysisException:
        campos_atuais = {}
    validar_contrato_esquema(
        nome_completo_tabela, campos_atuais, campos_do_esquema(df_entrada.schema),
        colunas_documentadas=list(comentarios_colunas) if comentarios_colunas else None,
        permitir_quebra=permitir_quebra_esquema,
    )


# ============================================================================
# LOAD - APPEND PURO (SEM MERGE/UPSERT) + REGISTRO NO UNITY CATALOG
# ============================================================================

def garantir_tabela_unity_catalog(spark, nome_completo_tabela, caminho_delta, comentario_tabela=None):
    """Registra a tabela externa Delta no Unity Catalog se ainda não existir.
    Se já existe, não altera nada."""
    if not spark.catalog.tableExists(nome_completo_tabela):
        comentario = comentario_tabela or "Camada Bronze - dado bruto da Stage, 1:1, sem regra de negócio"
        spark.sql(f"""
            CREATE TABLE {nome_completo_tabela}
            USING DELTA
            LOCATION '{caminho_delta}'
            COMMENT '{_escapar_string_sql(comentario)}'
        """)
        print(f"Tabela Unity Catalog criada: {nome_completo_tabela}")


def _escapar_string_sql(valor):
    # Spark SQL não aceita '' (padrão ANSI) como aspa literal - dá
    # ParseException. O escape que funciona é com backslash.
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def anexar_na_bronze(df, caminho_delta, nome_completo_tabela, comentario_tabela=None):
    """Escreve por APPEND (cria a tabela Delta automaticamente na 1a carga)."""
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .save(caminho_delta))
    garantir_tabela_unity_catalog(df.sparkSession, nome_completo_tabela, caminho_delta, comentario_tabela)


# ============================================================================
# CONTROLE DE EXECUÇÃO
# ============================================================================
# Um JSON por run em {caminho_s3_bronze}/_control/{nome_tabela_bronze}/{run_id}.json,
# mesmo padrão da Stage. rejected_records é sempre 0: a Bronze não descarta nada.

def iniciar_execucao_bronze(nome_tabela):
    return {
        "run_id": uuid.uuid4().hex[:12],
        "table": nome_tabela,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
        "files_processed": 0,
        "records_read": 0,
        "records_written": 0,
        "rejected_records": 0,
    }


def finalizar_execucao_bronze(dbutils, execucao, caminho_s3_bronze, status, erro=None):
    inicio = datetime.fromisoformat(execucao["started_at"])
    fim = datetime.now(timezone.utc)

    execucao["finished_at"] = fim.isoformat()
    execucao["duration_seconds"] = round((fim - inicio).total_seconds(), 1)
    execucao["status"] = status
    execucao["error"] = erro

    # Duplica o finalizar_execucao de ingestion_utils: função de um arquivo carregado
    # por %run não enxerga nomes de outro %run (NameError).
    dir_controle = f"{caminho_s3_bronze}/_control/{execucao['table']}"
    caminho_controle = f"{dir_controle}/{execucao['run_id']}.json"
    try:
        dbutils.fs.mkdirs(dir_controle)
        dbutils.fs.put(caminho_controle, json.dumps(execucao, default=str), overwrite=True)
    except Exception as e:
        print(f"Aviso: falha ao gravar controle de execução em {caminho_controle}: {e}")

    print(
        f"[{execucao['table']}] run={execucao['run_id']} status={status} "
        f"arquivos_processados={execucao['files_processed']} "
        f"registros_lidos={execucao['records_read']} registros_gravados={execucao['records_written']}"
        + (f" erro={erro}" if erro else "")
    )
    return execucao


# ============================================================================
# ORQUESTRAÇÃO
# ============================================================================

def executar_ingestao_bronze(spark, dbutils, catalogo, esquema,
                             nome_tabela_bronze, nome_tabela_stage,
                             caminho_s3_stage, caminho_s3_bronze,
                             comentario_tabela=None, comentarios_colunas=None,
                             permitir_quebra_esquema=False):
    """EL completo: arquivos novos da Stage -> metadados técnicos -> contrato de
    schema -> append na Bronze -> tabela no Unity Catalog -> controle de execução.

    Sem arquivo novo, não escreve nada e fecha como SUCCESS com 0 registros.
    comentario_tabela/comentarios_colunas vêm de bronze_column_docs.py; as
    colunas documentadas são o contrato de nomes da tabela.
    permitir_quebra_esquema: True só para remover coluna ou mudar tipo de propósito.
    """
    execucao = iniciar_execucao_bronze(nome_tabela_bronze)
    caminho_delta = f"{caminho_s3_bronze}/{nome_tabela_bronze}"
    nome_completo_tabela = f"{catalogo}.{esquema}.{nome_tabela_bronze}"

    try:
        todos_arquivos = listar_arquivos_stage(dbutils, caminho_s3_stage, nome_tabela_stage)
        ja_carregados = obter_arquivos_ja_carregados(spark, caminho_delta)
        arquivos_novos = [f for f in todos_arquivos if normalizar_caminho(f) not in ja_carregados]
        execucao["files_processed"] = len(arquivos_novos)

        if not arquivos_novos:
            # Atualiza os comentários mesmo sem dado novo.
            if spark.catalog.tableExists(nome_completo_tabela):
                aplicar_documentacao_tabela(spark, nome_completo_tabela, comentario_tabela, comentarios_colunas)
            print(f"[{nome_tabela_bronze}] Nenhum arquivo novo da Stage - nada a fazer (idempotente).")
            finalizar_execucao_bronze(dbutils, execucao, caminho_s3_bronze, "SUCCESS")
            return None, execucao

        print(f"[{nome_tabela_bronze}] Arquivos novos da Stage: {len(arquivos_novos)}")
        # _metadata.file_path em vez de input_file_name(), que o Unity Catalog
        # não suporta em Shared/User Isolation.
        df = spark.read.parquet(*arquivos_novos) \
            .withColumn("source_file", col("_metadata.file_path")) \
            .withColumn("bronze_run_id", lit(execucao["run_id"])) \
            .withColumn("bronze_ingestion_timestamp", current_timestamp()) \
            .cache()

        execucao["records_read"] = df.count()

        validar_esquema_bronze(spark, caminho_delta, nome_completo_tabela, df,
                               comentarios_colunas, permitir_quebra_esquema)
        anexar_na_bronze(df, caminho_delta, nome_completo_tabela, comentario_tabela)
        aplicar_documentacao_tabela(spark, nome_completo_tabela, comentario_tabela, comentarios_colunas)

        execucao["records_written"] = execucao["records_read"]
        finalizar_execucao_bronze(dbutils, execucao, caminho_s3_bronze, "SUCCESS")
        return df, execucao

    except Exception as e:
        finalizar_execucao_bronze(dbutils, execucao, caminho_s3_bronze, "FAILED", erro=str(e))
        raise
