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


def config_do_ambiente(nome_segredo):
    """Valor da env var MTG_<NOME>, ou None.

    Mesma precedencia do base_utils (env var > secret > default), duplicada
    aqui porque o Stage nao importa base_utils.
    """
    return os.environ.get("MTG_" + nome_segredo.upper()) or None


def _bucket_sem_esquema(nome_segredo, valor):
    """Tira o "s3://" do s3_bucket - os notebooks montam f"s3://{bucket}/...".

    Copia de base_utils._bucket_sem_esquema (o Stage nao importa base_utils).
    """
    if nome_segredo == "s3_bucket" and valor.startswith("s3://"):
        return valor[len("s3://"):]
    return valor


def obter_segredo(nome_segredo, valor_padrao=None):
    valor_ambiente = config_do_ambiente(nome_segredo)
    if valor_ambiente:
        print(f"Config '{nome_segredo}' veio do ambiente: {valor_ambiente}")
        return _bucket_sem_esquema(nome_segredo, valor_ambiente)

    try:
        return _bucket_sem_esquema(nome_segredo, dbutils.secrets.get(scope="mtg-pipeline", key=nome_segredo))
    except Exception:
        if valor_padrao is not None:
            print(f"Segredo '{nome_segredo}' não encontrado, usando valor padrão")
            return valor_padrao
        print(f"Segredo obrigatório '{nome_segredo}' não encontrado")
        raise Exception(f"Segredo '{nome_segredo}' não configurado")


def configurar_armazenamento_s3(caminho_base):
    try:
        dbutils.fs.ls(caminho_base)
        print("Diretório do S3 já existe")
        return True
    except Exception:
        pass
    try:
        dbutils.fs.mkdirs(caminho_base)
        print("Diretório do S3 criado com sucesso")
        return True
    except Exception as e:
        # Propaga a causa real (credencial/IAM/path inválido).
        raise Exception(f"Erro ao configurar S3 storage em '{caminho_base}': {e}")


def obter_http_com_retentativa(url, cabecalhos=None, tempo_limite=30, tentativas=3):
    """
    GET com retry/backoff para timeout/erro de conexão, 429 (rate limit) e 5xx. Demais 4xx
    falham na hora - erro do cliente, repetir não muda o resultado.
    """
    ultimo_erro = None
    for tentativa in range(tentativas):
        try:
            resposta = requests.get(url, headers=cabecalhos, timeout=tempo_limite)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            ultimo_erro = e
            print(f"Tentativa {tentativa + 1}/{tentativas} falhou para {url}: {e}")
            if tentativa < tentativas - 1:
                time.sleep(5)
            continue

        if resposta.status_code == 429:
            espera = min((tentativa + 1) * 5, 60)
            print(f"Rate limit atingido em {url}. Aguardando {espera}s...")
            time.sleep(espera)
            ultimo_erro = requests.exceptions.HTTPError(f"429 em {url}")
            continue
        if resposta.status_code >= 500:
            espera = min((tentativa + 1) * 10, 120)
            print(f"Erro {resposta.status_code} em {url}. Aguardando {espera}s...")
            time.sleep(espera)
            ultimo_erro = requests.exceptions.HTTPError(f"{resposta.status_code} em {url}")
            continue

        resposta.raise_for_status()  # 4xx: falha imediata, sem retry
        return resposta

    raise ultimo_erro or Exception(f"Falha ao obter {url} após {tentativas} tentativas")


def obter_codigos_colecoes_scryfall_desde(url_api_scryfall, cabecalhos, data_corte, tentativas=3):
    """
    Códigos (minúsculos) das coleções lançadas a partir de data_corte.
    Faz 1 request ao /sets da Scryfall, sem seguir next_page.
    """
    resposta = obter_http_com_retentativa(f"{url_api_scryfall}/sets", cabecalhos=cabecalhos, tentativas=tentativas)
    todas_colecoes = resposta.json()["data"]
    codigos = [
        s["code"].lower() for s in todas_colecoes
        if s.get("code") and s.get("released_at") and s["released_at"] >= data_corte
    ]
    print(f"Coleções dentro da janela temporal (released_at >= {data_corte}): {len(codigos)}/{len(todas_colecoes)} sets")
    return codigos


def como_float(valor):
    """Converte para float, preservando None.

    A Scryfall pode mandar int (ex.: mana_value 0) em campos Double/Float do
    schema, e createDataFrame rejeita int em DoubleType.
    """
    return float(valor) if valor is not None else None


def nome_arquivo_parquet(ano_particao, mes_particao, data_execucao, nome_tabela):
    return f"{ano_particao}_{mes_particao:02d}_{data_execucao}_{nome_tabela}.parquet"


def _carimbo_da_execucao(execucao):
    """Carimbo unico da execucao, como literal Python.

    Nao usar current_timestamp(): salvar_em_parquet faz um .write por particao e
    o Spark reavalia a expressao a cada action, gerando carimbos diferentes na
    mesma run. O literal e o mesmo em todas as escritas.
    """
    if execucao and execucao.get("started_at"):
        return datetime.fromisoformat(execucao["started_at"])
    return datetime.now(timezone.utc)


def salvar_em_parquet(spark, dados, nome_tabela, caminho_base, esquema=None,
                     coluna_origem_particao=None, data_corte=None, execucao=None):
    """
    coluna_origem_particao: coluna já presente no dado (ex.: 'releaseDate') usada para
        derivar partition_year/partition_month. Se None, usa a data de ingestão (agora).
    data_corte: se informado, mantém apenas registros com coluna_origem_particao >= data_corte.
    execucao: dict de iniciar_execucao(), opcional - se informado, acumula files_written/
        files_skipped/records_written nele para o controle de execução.
    """
    if not dados:
        print(f"Nenhum dado para salvar na tabela {nome_tabela}")
        return None

    try:
        df = spark.createDataFrame(dados, esquema) if esquema else spark.createDataFrame(dados)

        # Preserva o `source` que vem no dado (ex.: rulings: 'wotc'/'scryfall');
        # usa 'scryfall' quando a fonte nao traz o campo.
        origem = coalesce(col("source"), lit("scryfall")) if "source" in df.columns else lit("scryfall")
        df = df.withColumn("ingestion_timestamp", lit(_carimbo_da_execucao(execucao))) \
               .withColumn("source", origem) \
               .withColumn("endpoint", lit(nome_tabela))

        if coluna_origem_particao and coluna_origem_particao in df.columns:
            df = df.withColumn(
                "partition_year",
                when(col(coluna_origem_particao).isNotNull(), year(col(coluna_origem_particao)))
                .otherwise(lit(datetime.now().year))
            ).withColumn(
                "partition_month",
                when(col(coluna_origem_particao).isNotNull(), month(col(coluna_origem_particao)))
                .otherwise(lit(datetime.now().month))
            )
            if data_corte:
                total = df.count()
                df = df.filter(col(coluna_origem_particao) >= lit(data_corte))
                print(f"{nome_tabela} filtrados pela janela temporal: {df.count()}/{total}")
        else:
            df = df.withColumn("partition_year", year(col("ingestion_timestamp"))) \
                   .withColumn("partition_month", month(col("ingestion_timestamp")))

        data_execucao = _carimbo_da_execucao(execucao).strftime("%Y%m%d")
        combinacoes_particao = df.select("partition_year", "partition_month").distinct().collect()

        for linha_particao in combinacoes_particao:
            ano_particao = linha_particao["partition_year"]
            mes_particao = linha_particao["partition_month"]

            df_particao = df.filter(
                (col("partition_year") == ano_particao) & (col("partition_month") == mes_particao)
            )

            # Um arquivo por partição por dia de execução (data completa YYYYMMDD
            # no nome), em caminho_base/{nome_tabela}/. Se já existe, pula.
            nome_arquivo = nome_arquivo_parquet(ano_particao, mes_particao, data_execucao, nome_tabela)
            caminho_arquivo = f"{caminho_base}/{nome_tabela}/{nome_arquivo}"

            try:
                arquivos_existentes = dbutils.fs.ls(caminho_arquivo)
                if len(arquivos_existentes) > 0:
                    print(f"Arquivo {nome_arquivo} já existe - pulando (já ingerido hoje)")
                    if execucao is not None:
                        execucao["files_skipped"] = execucao.get("files_skipped", 0) + 1
                    continue
            except Exception:
                pass

            df_particao.drop("partition_year", "partition_month") \
                .write.mode("overwrite").format("parquet").save(caminho_arquivo)
            print(f"Arquivo {nome_arquivo} criado com sucesso")
            if execucao is not None:
                execucao["files_written"] = execucao.get("files_written", 0) + 1
                execucao["records_written"] = execucao.get("records_written", 0) + df_particao.count()

        print(f"Registros salvos como Parquet para {nome_tabela}")
        return df

    except Exception as e:
        print(f"Erro ao salvar dados em {nome_tabela}: {e}")
        if execucao is not None:
            execucao["error"] = str(e)
        return None


# ============================================================================
# CONTROLE DE EXECUÇÃO
# ============================================================================
# Um JSON por run em {caminho_base}/_control/{nome_tabela}/{run_id}.json com
# run_id, endpoint, parâmetros, início/fim, contagens, status e erro.

def iniciar_execucao(nome_tabela, endpoint, parametros=None):
    return {
        "run_id": uuid.uuid4().hex[:12],
        "table_name": nome_tabela,
        "origem": "scryfall",
        "endpoint": endpoint,
        "params": parametros or {},
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
    }


def finalizar_execucao(execucao, caminho_base, status, erro=None):
    """status: SUCCESS | FAILED. Grava o JSON de controle e devolve o dict."""
    inicio = datetime.fromisoformat(execucao["started_at"])
    fim = datetime.now(timezone.utc)

    execucao["finished_at"] = fim.isoformat()
    execucao["duration_seconds"] = round((fim - inicio).total_seconds(), 1)
    execucao["status"] = status
    execucao["error"] = erro or execucao.get("error")
    execucao.setdefault("files_written", 0)
    execucao.setdefault("files_skipped", 0)
    execucao.setdefault("records_written", 0)

    dir_controle = f"{caminho_base}/_control/{execucao['table_name']}"
    caminho_controle = f"{dir_controle}/{execucao['run_id']}.json"
    try:
        dbutils.fs.mkdirs(dir_controle)
        dbutils.fs.put(caminho_controle, json.dumps(execucao, default=str), overwrite=True)
    except Exception as e:
        # Falha ao gravar o controle não deve mudar o resultado da run.
        print(f"Aviso: falha ao gravar controle de execução em {caminho_controle}: {e}")

    print(
        f"[{execucao['table_name']}] run={execucao['run_id']} status={status} "
        f"arquivos_novos={execucao['files_written']} arquivos_pulados={execucao['files_skipped']} "
        f"registros={execucao['records_written']} duracao={execucao['duration_seconds']}s"
        + (f" erro={erro}" if erro else "")
    )
    return execucao


def executar_ingestao_stage(nome_tabela, endpoint, funcao_ingestao, caminho_base, parametros=None):
    """
    Roda iniciar_execucao -> funcao_ingestao(execucao) -> finalizar_execucao, usado pelos 6 notebooks da Stage.

    funcao_ingestao deve devolver o DataFrame gravado; None levanta exceção para o
    job falhar. Devolve (df, execucao).
    """
    execucao = iniciar_execucao(nome_tabela, endpoint=endpoint, parametros=parametros)
    try:
        print(f"Iniciando ingestão de {nome_tabela}...")
        df = funcao_ingestao(execucao)
        if df is None:
            # Levanta para a task do job falhar; o except abaixo grava FAILED.
            raise Exception(
                f"Ingestao de {nome_tabela} nao gravou nada: {execucao.get('error') or 'sem dados'}"
            )
        finalizar_execucao(execucao, caminho_base, "SUCCESS")
        return df, execucao
    except Exception as e:
        finalizar_execucao(execucao, caminho_base, "FAILED", erro=str(e))
        raise
