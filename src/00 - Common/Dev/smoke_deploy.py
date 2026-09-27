# Databricks notebook source
# =============================================================================
# SMOKE TEST DE DEPLOY
# =============================================================================
"""Confere que o ambiente deployado sobe e aceita escrita. Sem logica de negocio.

Disparado pelo .github/scripts/smoke.py depois de todo deploy (dev e prd), ~4min.
Verifica: config do ambiente no cluster, escrita/leitura no catalogo e schemas
do medalhao. Nao le Scryfall nem toca tabelas reais.
"""

# COMMAND ----------

# MAGIC %run ./base_utils

# COMMAND ----------

import os
import uuid

spark = get_spark_session()

# 1. A config do ambiente chegou no cluster? Sem ela, get_secret usa o scope
#    compartilhado e prd resolveria o catalogo mtg_dev.
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

# 2. O catalogo aceita escrita? Testa grant do UC, external location e S3.
#    Tabela temporaria em .default com nome unico, fora dos schemas do medalhao.
tabela = f"{catalog}.default.smoke_{uuid.uuid4().hex[:8]}"
try:
    spark.sql(f"CREATE TABLE {tabela} (n INT) USING DELTA")
    spark.sql(f"INSERT INTO {tabela} VALUES (1)")
    lido = spark.sql(f"SELECT n FROM {tabela}").collect()[0][0]
    if lido != 1:
        raise AssertionError(f"escreveu 1, leu {lido}")
    print(f"escrita/leitura em {catalog} ok")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {tabela}")

# COMMAND ----------

# 3. Garante os schemas do medalhao no UC (Stage fica so no S3). Em catalogo
#    novo, isso cria os schemas.
for schema in ["bronze", "silver", "gold"]:
    setup_unity_catalog(catalog, schema)

print(f"SMOKE OK · {ambiente} · {catalog}")
