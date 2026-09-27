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


def config_override(secret_name):
    """Valor da env var MTG_<SECRET_NAME>, ou None.

    dev e prd dividem o mesmo secret scope (com a config de dev). O que prd
    muda (catalogo, bucket) chega como env var, injetada pelo deploy.py em
    spark_env_vars. Precedencia no get_secret: env var > secret > default.
    """
    return os.environ.get("MTG_" + secret_name.upper()) or None


def _bucket_sem_esquema(secret_name, value):
    """Remove o "s3://" do s3_bucket, venha de onde vier.

    Stage e Bronze montam f"s3://{bucket}/...", entao o valor nao pode trazer o esquema.
    """
    if secret_name == "s3_bucket" and value.startswith("s3://"):
        return value[len("s3://"):]
    return value


def _barra_catalogo_de_dev_em_producao(secret_name, value):
    """Falha se producao resolver o catalogo pra mtg_dev.

    dev e prd estao no mesmo workspace: sem MTG_CATALOG_NAME, o job de
    producao gravaria nas tabelas de dev.
    """
    if (
        secret_name == "catalog_name"
        and os.environ.get("MTG_ENVIRONMENT") == "production"
        and value == "mtg_dev"
    ):
        raise Exception(
            "catalog_name resolveu para mtg_dev com MTG_ENVIRONMENT=production - "
            "injete MTG_CATALOG_NAME no alvo de deploy"
        )
    return value

# ============================================================================
# INICIALIZAÇÃO PARA DATABRICKS
# ============================================================================
def get_spark_session():
    """Obtém SparkSession do contexto global do Databricks"""
    try:
        return spark  # Disponível globalmente no Databricks
    except:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

# ============================================================================
# UNITY CATALOG
# ============================================================================
def setup_unity_catalog(catalog, schema):
    """
    Configura Unity Catalog criando catalog e schema se necessário

    Args:
        catalog (str): Nome do catalog
        schema (str): Nome do schema

    Raises:
        Exception: a original do Spark, se o catalog/schema nao puder ser configurado.
    """
    spark_session = get_spark_session()
    # Tenta USE primeiro: o metastore nao tem storage root default, entao
    # CREATE CATALOG sem MANAGED LOCATION falha mesmo com IF NOT EXISTS.
    try:
        spark_session.sql(f"USE CATALOG {catalog}")
    except Exception:
        spark_session.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark_session.sql(f"USE CATALOG {catalog}")
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark_session.sql(f"USE SCHEMA {schema}")
    print(f"Schema {catalog}.{schema} configurado com sucesso")

# ============================================================================
# SECRETS
# ============================================================================
def get_secret(secret_name, default_value=None, extra_safe_defaults=None):
    """
    Obtém config: env var MTG_<NOME> > secret do scope "mtg-pipeline" > default.

    Args:
        secret_name (str): Nome do secret
        default_value (str, optional): Valor padrão se secret não for encontrado
        extra_safe_defaults (dict, optional): Defaults adicionais específicos da
            camada chamadora (ex.: {'s3_silver_prefix': '...'} ou {'s3_gold_prefix': '...'})

    Returns:
        str: Valor do secret

    Raises:
        Exception: Se secret obrigatório não for encontrado e sem default
    """
    override = config_override(secret_name)
    if override:
        print(f"Config '{secret_name}' veio do ambiente: {override}")
        return _barra_catalogo_de_dev_em_producao(secret_name, _bucket_sem_esquema(secret_name, override))

    try:
        return _barra_catalogo_de_dev_em_producao(
            secret_name,
            _bucket_sem_esquema(secret_name, dbutils.secrets.get(scope="mtg-pipeline", key=secret_name)),
        )
    except Exception:
        if default_value is not None:
            print(f"Secret '{secret_name}' não encontrado, usando valor padrão: {default_value}")
            return default_value

        # s3_bucket não tem default: sem ele o pipeline falha em vez de gravar num bucket errado.
        safe_defaults = {'catalog_name': 'mtg_dev'}
        safe_defaults.update(extra_safe_defaults or {})

        if secret_name in safe_defaults:
            print(f"Secret '{secret_name}' não encontrado, usando valor padrão: {safe_defaults[secret_name]}")
            return _barra_catalogo_de_dev_em_producao(secret_name, safe_defaults[secret_name])
        else:
            print(f"Secret '{secret_name}' não encontrado e sem valor padrão")
            print(f"Configure o secret no scope ou a env var MTG_<NOME>")
            raise Exception(f"Secret '{secret_name}' not configured and no default available")
