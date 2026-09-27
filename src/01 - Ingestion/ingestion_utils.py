# Databricks notebook source
# ============================================================================
# INGESTION UTILS - Funções compartilhadas pelos notebooks de Ingestão (Stage)
# ============================================================================
"""
Uso no notebook (Databricks):
    %run ./ingestion_utils

Consolida o boilerplate compartilhado por cards/sets/card_prices. Nome do
arquivo de staging inclui o dia da execução, então cada run diário grava seu
próprio arquivo em vez de "pular" o mês inteiro assim que o primeiro arquivo
daquele mês existisse.

Escopo desta camada (Stage): coleta da API Scryfall + validação da ingestão +
persistência em S3 + controle de execução. Sem CDC (a origem é uma API sem
mecanismo de captura de alteração) e sem regra de negócio - isso é Bronze/Silver.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from pyspark.sql.functions import col, lit, current_timestamp, year, month, when

# ponytail: em Serverless + Git source, %run às vezes executa este arquivo num
# namespace que não herda o `dbutils` implícito do notebook. Puxa do IPython
# quando isso acontece; fora de um notebook Databricks (ex.: pytest local),
# get_ipython() é None e o bloco é ignorado, preservando o NameError esperado
# pelos testes locais (ver test_base_utils_get_secret.py).
try:
    dbutils
except NameError:
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        pass


def config_override(secret_name):
    """Valor por ambiente, vindo de env var, ou None.

    Stage nao importa base_utils (camadas separadas, cada uma com seu %run),
    entao a precedencia env var > secret > default precisa existir aqui
    tambem - senao o Stage de prd grava no prefixo de S3 do dev.
    """
    return os.environ.get("MTG_" + secret_name.upper()) or None


def get_secret(secret_name, default_value=None):
    override = config_override(secret_name)
    if override:
        print(f"Config '{secret_name}' veio do ambiente: {override}")
        return override

    try:
        return dbutils.secrets.get(scope="mtg-pipeline", key=secret_name)
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
        # ponytail: propaga a exceção real (credencial/IAM/path inválido) em vez
        # de engolir e devolver False - o chamador só sabia dizer "falhou", nunca por quê.
        raise Exception(f"Erro ao configurar S3 storage em '{base_path}': {e}")


def http_get_with_retry(url, headers=None, timeout=30, retries=3):
    """
    GET com retry/backoff para 429 (rate limit) e 5xx (indisponibilidade) -
    nenhum request feito direto pelos notebooks (bulk-data/sets da Scryfall)
    tinha isso antes: uma falha transitória derrubava a run inteira sem
    tentar de novo. 4xx (exceto 429) não tem retry - erro do cliente, tentar
    de novo não muda o resultado.
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
    Códigos (lowercase) das coleções lançadas a partir de cutoff_date_str,
    segundo o /sets da Scryfall - 1 request só, devolve o catálogo inteiro
    (sem paginação, igual ao fetch_all_sets de sets.ipynb).
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
    """A Scryfall manda inteiro (ex.: mana_value 0) em campos que o schema
    declara Double/Float, e createDataFrame rejeita int em DoubleType com
    FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME. Converte preservando None.
    """
    return float(valor) if valor is not None else None


def _run_timestamp(run):
    """Carimbo unico da execucao, como literal Python.

    ponytail: NAO trocar por current_timestamp() - save_to_parquet chama .write
    uma vez por particao (loop abaixo) e o Spark reavalia a expressao a cada
    action, entao card_prices saia com 71 carimbos diferentes numa run so (1 por
    mes de releaseDate). Um literal e avaliado uma vez e sobrevive a todas as
    escritas. Mesmo padrao da Bronze com lit(run["run_id"]).
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

        df = df.withColumn("ingestion_timestamp", lit(_run_timestamp(run))) \
               .withColumn("source", lit("scryfall")) \
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

        run_date_str = datetime.now().strftime("%d")
        partition_combinations = df.select("partition_year", "partition_month").distinct().collect()

        for partition_row in partition_combinations:
            partition_year = partition_row["partition_year"]
            partition_month = partition_row["partition_month"]

            partition_df = df.filter(
                (col("partition_year") == partition_year) & (col("partition_month") == partition_month)
            )

            # Nome inclui o dia da execução para permitir um arquivo por run diário
            # (senão o check de "arquivo já existe" abaixo pularia o mês inteiro).
            # Cada tabela grava na sua própria pasta em base_path/{table_name}/.
            file_name = f"{partition_year}_{partition_month:02d}_{run_date_str}_{table_name}.parquet"
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
# Um JSON por run em {base_path}/_control/{table_name}/{run_id}.json - simples
# o bastante pra auditar (listar a pasta) sem precisar de uma tabela Delta só
# pra isso. Cobre run_id, endpoint, parâmetros, início/fim, contagens,
# status e erro (seção 8 do pedido de refatoração da Stage).

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
    """status: SUCCESS | FAILED | PARTIAL. Grava o JSON de controle e devolve o dict."""
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
        # O controle de execução é observabilidade, não deve mascarar o resultado real da run.
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
    Padroniza o wrapper start_run -> try/ingest_fn -> finish_run repetido
    quase byte-a-byte nos 6 notebooks de Stage (cards/sets/card_prices/
    symbology/rulings/migrations). ingest_fn é chamado como ingest_fn(run) e
    deve devolver o DataFrame gravado; None significa que nada foi gravado e
    levanta - o job precisa ficar vermelho. Devolve (df, run) - o relatório
    impresso ao final continua no notebook, já que o conteúdo varia por tabela.
    """
    run = start_run(table_name, endpoint=endpoint, params=params)
    try:
        print(f"Iniciando ingestão de {table_name}...")
        df = ingest_fn(run)
        if df is None:
            # O status ja era FAILED aqui, mas a funcao retornava normalmente e
            # a task do job fechava verde. Foi assim que uma escrita que nao
            # commitou ficou no S3 sem ninguem ver, ate a Bronze quebrar nela.
            # O except abaixo cuida do finish_run(FAILED).
            raise Exception(
                f"Ingestao de {table_name} nao gravou nada: {run.get('error') or 'sem dados'}"
            )
        finish_run(run, base_path, "SUCCESS")
        return df, run
    except Exception as e:
        finish_run(run, base_path, "FAILED", error=str(e))
        raise
