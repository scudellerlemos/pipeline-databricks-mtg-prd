# Databricks notebook source
# =============================================================================
# SMOKE TEST DE DEPLOY
# =============================================================================
"""Prova que o ambiente deployado EXISTE e ACEITA ESCRITA - nada de negocio.

Existe porque o CI e 100% estatico (yaml, ruff, pytest de funcao pura) e o
`verify_deployment` do deploy.py so rele as settings do job pela API. Os dois
juntos provam que o job esta configurado certo; nenhum dos dois prova que o
cluster sobe, que MTG_CATALOG_NAME chegou no spark_env_vars de verdade, ou que
o Unity Catalog deixa gravar no storage do catalogo. Ate a primeira run
mensal, isso tudo e fe.

Roda em ~4min (instance pool) depois de todo deploy, em dev e em prd.

O que NAO faz: ler Scryfall, escrever em tabela de verdade, validar dado.
Isso e a run agendada - um smoke test que roda o pipeline nao e smoke test.
"""

# COMMAND ----------

# MAGIC %run ./base_utils

# COMMAND ----------

import os
import uuid

spark = get_spark_session()

# 1. A config do ambiente chegou no cluster?
#    Se o deploy esqueceu de injetar, get_secret cai no scope compartilhado e
#    prd grava em mtg_dev. Essa e a falha silenciosa que mais custa aqui.
env_vars = {k: v for k, v in os.environ.items() if k.startswith("MTG_")}
print(f"MTG_* no cluster: {env_vars or '(nenhuma - deploy sem config injetada)'}")

catalog = get_secret("catalog_name")
bucket = get_secret("s3_bucket")
ambiente = os.environ.get("MTG_ENVIRONMENT", "development")
print(f"ambiente={ambiente} catalog={catalog} bucket={bucket}")

esperado = os.environ.get("MTG_CATALOG_NAME")
if esperado and catalog != esperado:
    raise AssertionError(f"MTG_CATALOG_NAME={esperado} mas get_secret resolveu {catalog}")
if ambiente == "production" and catalog == "mtg_dev":
    raise AssertionError("producao resolveu o catalogo de dev")

# COMMAND ----------

# 2. O catalogo aceita escrita? Prova grant do UC + external location + S3
#    numa tacada so. Tabela managed em .default com nome unico: nao encosta em
#    nenhum schema do medalhao e nao colide com outro smoke rodando junto.
tabela = f"{catalog}.default.smoke_{uuid.uuid4().hex[:8]}"
try:
    spark.sql(f"CREATE TABLE {tabela} (n INT) USING DELTA")
    spark.sql(f"INSERT INTO {tabela} VALUES (1)")
    lido = spark.sql(f"SELECT n FROM {tabela}").collect()[0][0]
    if lido != 1:
        raise AssertionError(f"escreveu 1, leu {lido}")
    print(f"✅ escrita/leitura em {catalog} ok")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {tabela}")

# COMMAND ----------

# 3. Os schemas do medalhao existem? setup_unity_catalog cria se faltar, entao
#    isso tambem e o bootstrap de um catalogo novo (mtg_prod nasceu vazio).
for schema in ["stage", "bronze", "silver", "gold"]:
    setup_unity_catalog(catalog, schema)

print(f"SMOKE OK · {ambiente} · {catalog}")
