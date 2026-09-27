# Databricks notebook source
# ============================================================================
# BASE UTILS - Funções compartilhadas entre as camadas Bronze, Silver e Gold
# ============================================================================
"""
Infraestrutura comum aos notebooks de Bronze, Silver, Gold e ao smoke_deploy:
sessão Spark, Unity Catalog e leitura de secrets.

Carregado via %run "../../00 - Common/Dev/base_utils" (smoke_deploy usa
%run ./base_utils). dbutils e spark vêm do escopo global do notebook.
"""

# Em Serverless + Git source, o %run pode rodar este arquivo sem o `dbutils`
# do notebook; nesse caso pega do IPython. Fora do Databricks (pytest local)
# o bloco não faz nada e dbutils continua indefinido.
import os

try:
    dbutils
except NameError:
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        pass


def config_do_ambiente(nome_segredo):
    """Valor da env var MTG_<NOME_SEGREDO>, ou None.

    dev e prd dividem o mesmo secret scope (com a config de dev). O que prd
    muda (catalogo, bucket) chega como env var, injetada pelo deploy.py em
    spark_env_vars. Precedencia no obter_segredo: env var > secret > default.
    """
    return os.environ.get("MTG_" + nome_segredo.upper()) or None


def _bucket_sem_esquema(nome_segredo, valor):
    """Remove o "s3://" do s3_bucket, venha de onde vier.

    Stage e Bronze montam f"s3://{bucket}/...", entao o valor nao pode trazer o esquema.
    """
    if nome_segredo == "s3_bucket" and valor.startswith("s3://"):
        return valor[len("s3://"):]
    return valor


def _barra_catalogo_de_dev_em_producao(nome_segredo, valor):
    """Falha se producao resolver o catalogo pra mtg_dev.

    dev e prd estao no mesmo workspace: sem MTG_CATALOG_NAME, o job de
    producao gravaria nas tabelas de dev.
    """
    if (
        nome_segredo == "catalog_name"
        and os.environ.get("MTG_ENVIRONMENT") == "production"
        and valor == "mtg_dev"
    ):
        raise Exception(
            "catalog_name resolveu para mtg_dev com MTG_ENVIRONMENT=production - "
            "injete MTG_CATALOG_NAME no alvo de deploy"
        )
    return valor

# ============================================================================
# INICIALIZAÇÃO PARA DATABRICKS
# ============================================================================
def obter_sessao_spark():
    """Obtém SparkSession do contexto global do Databricks"""
    try:
        return spark  # Disponível globalmente no Databricks
    except:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

# ============================================================================
# UNITY CATALOG
# ============================================================================
def configurar_unity_catalog(catalogo, esquema):
    """
    Configura Unity Catalog criando catalog e schema se necessário

    Args:
        catalogo (str): Nome do catalog
        esquema (str): Nome do schema

    Raises:
        Exception: a original do Spark, se o catalog/schema nao puder ser configurado.
    """
    sessao_spark = obter_sessao_spark()
    # Tenta USE primeiro: o metastore nao tem storage root default, entao
    # CREATE CATALOG sem MANAGED LOCATION falha mesmo com IF NOT EXISTS.
    try:
        sessao_spark.sql(f"USE CATALOG {catalogo}")
    except Exception:
        sessao_spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
        sessao_spark.sql(f"USE CATALOG {catalogo}")
    sessao_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
    sessao_spark.sql(f"USE SCHEMA {esquema}")
    print(f"Schema {catalogo}.{esquema} configurado com sucesso")

# ============================================================================
# SECRETS
# ============================================================================
def obter_segredo(nome_segredo, valor_padrao=None, padroes_seguros_extras=None):
    """
    Obtém config: env var MTG_<NOME> > secret do scope "mtg-pipeline" > default.

    Args:
        nome_segredo (str): Nome do secret
        valor_padrao (str, optional): Valor padrão se secret não for encontrado
        padroes_seguros_extras (dict, optional): Defaults adicionais específicos da
            camada chamadora (ex.: {'s3_silver_prefix': '...'} ou {'s3_gold_prefix': '...'})

    Returns:
        str: Valor do secret

    Raises:
        Exception: Se secret obrigatório não for encontrado e sem default
    """
    valor_ambiente = config_do_ambiente(nome_segredo)
    if valor_ambiente:
        print(f"Config '{nome_segredo}' veio do ambiente: {valor_ambiente}")
        return _barra_catalogo_de_dev_em_producao(nome_segredo, _bucket_sem_esquema(nome_segredo, valor_ambiente))

    try:
        return _barra_catalogo_de_dev_em_producao(
            nome_segredo,
            _bucket_sem_esquema(nome_segredo, dbutils.secrets.get(scope="mtg-pipeline", key=nome_segredo)),
        )
    except Exception:
        if valor_padrao is not None:
            print(f"Secret '{nome_segredo}' não encontrado, usando valor padrão: {valor_padrao}")
            return valor_padrao

        # s3_bucket não tem default: sem ele o pipeline falha em vez de gravar num bucket errado.
        padroes_seguros = {'catalog_name': 'mtg_dev'}
        padroes_seguros.update(padroes_seguros_extras or {})

        if nome_segredo in padroes_seguros:
            print(f"Secret '{nome_segredo}' não encontrado, usando valor padrão: {padroes_seguros[nome_segredo]}")
            return _barra_catalogo_de_dev_em_producao(nome_segredo, padroes_seguros[nome_segredo])
        else:
            print(f"Secret '{nome_segredo}' não encontrado e sem valor padrão")
            print(f"Configure o secret no scope ou a env var MTG_<NOME>")
            raise Exception(f"Secret '{nome_segredo}' not configured and no default available")
