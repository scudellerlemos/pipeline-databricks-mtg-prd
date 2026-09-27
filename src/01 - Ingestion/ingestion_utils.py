# Databricks notebook source
# ============================================================================
# INGESTION UTILS - Funções compartilhadas pelos notebooks de Ingestão (Stage)
# ============================================================================
"""
Uso no notebook (Databricks):
    %run ./ingestion_utils

Código compartilhado pelos 6 notebooks da Stage: coleta da API Scryfall,
gravação em Parquet no S3 e controle de execução. Sem CDC (a API não tem
captura de alteração) e sem regra de negócio - isso fica na Bronze/Silver.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from pyspark.sql.functions import coalesce, col, lit, current_timestamp, year, month, when

# Em Serverless + Git source, o %run pode executar este arquivo num namespace
# sem o `dbutils` do notebook; nesse caso pega do IPython. Fora do Databricks
# (pytest local) o bloco não faz nada e `dbutils` segue indefinido.
try:
    dbutils
except NameError:
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        pass


def config_override(secret_name):
    """Valor da env var MTG_<NOME>, ou None.

    Mesma precedencia do base_utils (env var > secret > default), duplicada
    aqui porque o Stage nao importa base_utils.
    """
    return os.environ.get("MTG_" + secret_name.upper()) or None


def _bucket_sem_esquema(secret_name, value):
    """Tira o "s3://" do s3_bucket - os notebooks montam f"s3://{bucket}/...".

    Copia de base_utils._bucket_sem_esquema (o Stage nao importa base_utils).
    """
    if secret_name == "s3_bucket" and value.startswith("s3://"):
        return value[len("s3://"):]
    return value


def get_secret(secret_name, default_value=None):
    override = config_override(secret_name)
    if override:
        print(f"Config '{secret_name}' veio do ambiente: {override}")
        return _bucket_sem_esquema(secret_name, override)

    try:
        return _bucket_sem_esquema(secret_name, dbutils.secrets.get(scope="mtg-pipeline", key=secret_name))
    except Exception:
        if default_value is not None:
            print(f"Segredo '{secret_name}' não encontrado, usando valor padrão")
            return default_value
        print(f"Segredo obrigatório '{secret_name}' não encontrado")
        raise Exception(f"Segredo '{secret_name}' não configurado")


def setup_s3_storage(base_path):
    try:
        dbutils.fs.ls(base_path)
        print("Diretório do S3 já existe")
        return True
    except Exception:
        pass
    try:
        dbutils.fs.mkdirs(base_path)
        print("Diretório do S3 criado com sucesso")
        return True
    except Exception as e:
        # Propaga a causa real (credencial/IAM/path inválido).
        raise Exception(f"Erro ao configurar S3 storage em '{base_path}': {e}")


def http_get_with_retry(url, headers=None, timeout=30, retries=3):
    """
    GET com retry/backoff para timeout/erro de conexão, 429 (rate limit) e 5xx. Demais 4xx
    falham na hora - erro do cliente, repetir não muda o resultado.
    """
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            print(f"Tentativa {attempt + 1}/{retries} falhou para {url}: {e}")
            if attempt < retries - 1:
                time.sleep(5)
            continue

        if response.status_code == 429:
            wait_time = min((attempt + 1) * 5, 60)
            print(f"Rate limit atingido em {url}. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            last_error = requests.exceptions.HTTPError(f"429 em {url}")
            continue
        if response.status_code >= 500:
            wait_time = min((attempt + 1) * 10, 120)
            print(f"Erro {response.status_code} em {url}. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            last_error = requests.exceptions.HTTPError(f"{response.status_code} em {url}")
            continue

        response.raise_for_status()  # 4xx: falha imediata, sem retry
        return response

    raise last_error or Exception(f"Falha ao obter {url} após {retries} tentativas")


def get_scryfall_set_codes_since(scryfall_api_url, headers, cutoff_date_str, retries=3):
    """
    Códigos (minúsculos) das coleções lançadas a partir de cutoff_date_str.
    Faz 1 request ao /sets da Scryfall, sem seguir next_page.
    """
    response = http_get_with_retry(f"{scryfall_api_url}/sets", headers=headers, retries=retries)
    all_sets = response.json()["data"]
    codes = [
        s["code"].lower() for s in all_sets
        if s.get("code") and s.get("released_at") and s["released_at"] >= cutoff_date_str
    ]
    print(f"Coleções dentro da janela temporal (released_at >= {cutoff_date_str}): {len(codes)}/{len(all_sets)} sets")
    return codes


def as_float(valor):
    """Converte para float, preservando None.

    A Scryfall pode mandar int (ex.: mana_value 0) em campos Double/Float do
    schema, e createDataFrame rejeita int em DoubleType.
    """
    return float(valor) if valor is not None else None


def parquet_file_name(partition_year, partition_month, run_date_str, table_name):
    return f"{partition_year}_{partition_month:02d}_{run_date_str}_{table_name}.parquet"


def _run_timestamp(run):
    """Carimbo unico da execucao, como literal Python.

    Nao usar current_timestamp(): save_to_parquet faz um .write por particao e
    o Spark reavalia a expressao a cada action, gerando carimbos diferentes na
    mesma run. O literal e o mesmo em todas as escritas.
    """
    if run and run.get("started_at"):
        return datetime.fromisoformat(run["started_at"])
    return datetime.now(timezone.utc)


def save_to_parquet(spark, data, table_name, base_path, schema=None,
                     partition_source_col=None, cutoff_date_str=None, run=None):
    """
    partition_source_col: coluna já presente no dado (ex.: 'releaseDate') usada para
        derivar partition_year/partition_month. Se None, usa a data de ingestão (agora).
    cutoff_date_str: se informado, mantém apenas registros com partition_source_col >= cutoff_date_str.
    run: dict de start_run(), opcional - se informado, acumula files_written/
        files_skipped/records_written nele para o controle de execução.
    """
    if not data:
        print(f"Nenhum dado para salvar na tabela {table_name}")
        return None

    try:
        df = spark.createDataFrame(data, schema) if schema else spark.createDataFrame(data)

        # Preserva o `source` que vem no dado (ex.: rulings: 'wotc'/'scryfall');
        # usa 'scryfall' quando a fonte nao traz o campo.
        source = coalesce(col("source"), lit("scryfall")) if "source" in df.columns else lit("scryfall")
        df = df.withColumn("ingestion_timestamp", lit(_run_timestamp(run))) \
               .withColumn("source", source) \
               .withColumn("endpoint", lit(table_name))

        if partition_source_col and partition_source_col in df.columns:
            df = df.withColumn(
                "partition_year",
                when(col(partition_source_col).isNotNull(), year(col(partition_source_col)))
                .otherwise(lit(datetime.now().year))
            ).withColumn(
                "partition_month",
                when(col(partition_source_col).isNotNull(), month(col(partition_source_col)))
                .otherwise(lit(datetime.now().month))
            )
            if cutoff_date_str:
                total = df.count()
                df = df.filter(col(partition_source_col) >= lit(cutoff_date_str))
                print(f"{table_name} filtrados pela janela temporal: {df.count()}/{total}")
        else:
            df = df.withColumn("partition_year", year(col("ingestion_timestamp"))) \
                   .withColumn("partition_month", month(col("ingestion_timestamp")))

        run_date_str = _run_timestamp(run).strftime("%Y%m%d")
        partition_combinations = df.select("partition_year", "partition_month").distinct().collect()

        for partition_row in partition_combinations:
            partition_year = partition_row["partition_year"]
            partition_month = partition_row["partition_month"]

            partition_df = df.filter(
                (col("partition_year") == partition_year) & (col("partition_month") == partition_month)
            )

            # Um arquivo por partição por dia de execução (data completa YYYYMMDD
            # no nome), em base_path/{table_name}/. Se já existe, pula.
            file_name = parquet_file_name(partition_year, partition_month, run_date_str, table_name)
            file_path = f"{base_path}/{table_name}/{file_name}"

            try:
                existing_files = dbutils.fs.ls(file_path)
                if len(existing_files) > 0:
                    print(f"Arquivo {file_name} já existe - pulando (já ingerido hoje)")
                    if run is not None:
                        run["files_skipped"] = run.get("files_skipped", 0) + 1
                    continue
            except Exception:
                pass

            partition_df.drop("partition_year", "partition_month") \
                .write.mode("overwrite").format("parquet").save(file_path)
            print(f"Arquivo {file_name} criado com sucesso")
            if run is not None:
                run["files_written"] = run.get("files_written", 0) + 1
                run["records_written"] = run.get("records_written", 0) + partition_df.count()

        print(f"Registros salvos como Parquet para {table_name}")
        return df

    except Exception as e:
        print(f"Erro ao salvar dados em {table_name}: {e}")
        if run is not None:
            run["error"] = str(e)
        return None


# ============================================================================
# CONTROLE DE EXECUÇÃO
# ============================================================================
# Um JSON por run em {base_path}/_control/{table_name}/{run_id}.json com
# run_id, endpoint, parâmetros, início/fim, contagens, status e erro.

def start_run(table_name, endpoint, params=None):
    return {
        "run_id": uuid.uuid4().hex[:12],
        "table_name": table_name,
        "origem": "scryfall",
        "endpoint": endpoint,
        "params": params or {},
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
    }


def finish_run(run, base_path, status, error=None):
    """status: SUCCESS | FAILED. Grava o JSON de controle e devolve o dict."""
    started_at = datetime.fromisoformat(run["started_at"])
    finished_at = datetime.now(timezone.utc)

    run["finished_at"] = finished_at.isoformat()
    run["duration_seconds"] = round((finished_at - started_at).total_seconds(), 1)
    run["status"] = status
    run["error"] = error or run.get("error")
    run.setdefault("files_written", 0)
    run.setdefault("files_skipped", 0)
    run.setdefault("records_written", 0)

    control_dir = f"{base_path}/_control/{run['table_name']}"
    control_path = f"{control_dir}/{run['run_id']}.json"
    try:
        dbutils.fs.mkdirs(control_dir)
        dbutils.fs.put(control_path, json.dumps(run, default=str), overwrite=True)
    except Exception as e:
        # Falha ao gravar o controle não deve mudar o resultado da run.
        print(f"Aviso: falha ao gravar controle de execução em {control_path}: {e}")

    print(
        f"[{run['table_name']}] run={run['run_id']} status={status} "
        f"arquivos_novos={run['files_written']} arquivos_pulados={run['files_skipped']} "
        f"registros={run['records_written']} duracao={run['duration_seconds']}s"
        + (f" erro={error}" if error else "")
    )
    return run


def run_stage_ingestion(table_name, endpoint, ingest_fn, base_path, params=None):
    """
    Roda start_run -> ingest_fn(run) -> finish_run, usado pelos 6 notebooks da Stage.

    ingest_fn deve devolver o DataFrame gravado; None levanta exceção para o
    job falhar. Devolve (df, run).
    """
    run = start_run(table_name, endpoint=endpoint, params=params)
    try:
        print(f"Iniciando ingestão de {table_name}...")
        df = ingest_fn(run)
        if df is None:
            # Levanta para a task do job falhar; o except abaixo grava FAILED.
            raise Exception(
                f"Ingestao de {table_name} nao gravou nada: {run.get('error') or 'sem dados'}"
            )
        finish_run(run, base_path, "SUCCESS")
        return df, run
    except Exception as e:
        finish_run(run, base_path, "FAILED", error=str(e))
        raise
