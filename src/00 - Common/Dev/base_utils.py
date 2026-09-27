# Databricks notebook source
# ============================================================================
# BASE UTILS - Funções compartilhadas entre as camadas Silver e Gold
# ============================================================================
"""
Módulo base com infraestrutura comum usada por silver_utils.py e gold_utils.py:
sessão Spark, Unity Catalog e leitura de secrets.

ADAPTADO PARA DATABRICKS NOTEBOOKS:
- dbutils e spark são disponíveis globalmente nos notebooks
- Importado por silver_utils.py / gold_utils.py via:
  %run ../../00 - Common/Dev/base_utils
"""

# ponytail: em Serverless + Git source, %run às vezes executa este arquivo num
# namespace que não herda o `dbutils` implícito do notebook. Puxa do IPython
# quando isso acontece; fora de um notebook Databricks (ex.: pytest local),
# get_ipython() é None e o bloco é ignorado, preservando o NameError esperado
# pelos testes locais (ver test_base_utils_get_secret.py).
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
    """Valor por ambiente, vindo de env var, ou None.

    dev e prd dividem workspace E scope de secret: o scope guarda so o que e
    igual nos dois. O que difere (catalogo, prefixos de S3) chega como env var
    injetada em spark_env_vars pelo deploy.py, entao a config de producao fica
    versionada no workflow em vez de invisivel num scope.

    Precedencia: env var > secret > default do codigo. Nenhuma dessas chaves e
    segredo de verdade - sao config - por isso duplicar o scope inteiro so pra
    mudar tres valores seria criar sete valores pra manter em sincronia na mao.
    """
    return os.environ.get("MTG_" + secret_name.upper()) or None


def _barra_catalogo_de_dev_em_producao(secret_name, value):
    """Producao nunca pode resolver o catalogo pra mtg_dev.

    ponytail: os dois ambientes vivem no mesmo workspace, entao esquecer de
    injetar MTG_CATALOG_NAME faria o job de producao gravar por cima das
    tabelas de desenvolvimento - task verde, dado destruido. Explode aqui.
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

    Levanta a excecao original se o catalog/schema nao puder ser configurado:
    nenhum call site checava o retorno antigo, entao engolir a falha aqui
    deixava o notebook seguir e a task fechar verde sem ter escrito nada.
    """
    spark_session = get_spark_session()
    # ponytail: tenta USE primeiro - este metastore não tem storage root
    # default, então CREATE CATALOG sem MANAGED LOCATION falha mesmo com
    # IF NOT EXISTS quando o catalog já existe (caso normal aqui).
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
    Obtém segredos do Databricks Secret Scope

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
        return _barra_catalogo_de_dev_em_producao(secret_name, override)

    try:
        return _barra_catalogo_de_dev_em_producao(
            secret_name, dbutils.secrets.get(scope="mtg-pipeline", key=secret_name)
        )
    except Exception:
        if default_value is not None:
            print(f"Secret '{secret_name}' não encontrado, usando valor padrão: {default_value}")
            return default_value

        # ponytail: s3_bucket não entra em safe_defaults de propósito - é o
        # destino real de escrita/leitura de todas as camadas, então preferimos
        # falhar alto a gravar silenciosamente num bucket placeholder inexistente.
        safe_defaults = {'catalog_name': 'mtg_dev'}
        safe_defaults.update(extra_safe_defaults or {})

        if secret_name in safe_defaults:
            print(f"Secret '{secret_name}' não encontrado, usando valor padrão: {safe_defaults[secret_name]}")
            return _barra_catalogo_de_dev_em_producao(secret_name, safe_defaults[secret_name])
        else:
            print(f"⚠️ Secret '{secret_name}' não encontrado e sem valor padrão")
            print(f"💡 Configure o secret ou use create_manual_config()")
            raise Exception(f"Secret '{secret_name}' not configured and no default available")
