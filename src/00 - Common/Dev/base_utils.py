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

# ============================================================================
# CONTRATO DE SCHEMA
# ============================================================================
class ErroContratoEsquema(Exception):
    """Lote quebra o contrato de schema da tabela - nada foi gravado.

    Não é RuntimeError: a Gold trata RuntimeError do salvar como FALHA_DQ_PK.
    """


def campos_do_esquema(esquema):
    """{coluna: tipo} de um StructType (df.schema), sem olhar nulabilidade."""
    return {f.name: f.dataType.simpleString() for f in esquema.fields}


def validar_contrato_esquema(nome_tabela, campos_atuais, campos_novos,
                             colunas_documentadas=None, permitir_quebra=False):
    """Compara o lote com a tabela antes de gravar. Aborta se quebrar o contrato.

    campos_atuais/campos_novos: {coluna: tipo}; campos_atuais vazio = primeira carga.
    - Coluna nova entra, com aviso (evolução aditiva).
    - Coluna removida ou com tipo alterado aborta, a não ser com
      permitir_quebra=True (mudança intencional, declarada no notebook).
    - colunas_documentadas (column_docs da camada): o lote tem que ter
      exatamente essas colunas. Vale mesmo com permitir_quebra.

    Returns:
        list: colunas novas no lote.
    """
    colunas_novas = sorted(set(campos_novos) - set(campos_atuais)) if campos_atuais else []
    removidas = sorted(set(campos_atuais) - set(campos_novos))
    tipo_alterado = sorted(
        f"{c} ({campos_atuais[c]} -> {campos_novos[c]})"
        for c in campos_novos if c in campos_atuais and campos_novos[c] != campos_atuais[c]
    )

    problemas = []
    if removidas and not permitir_quebra:
        problemas.append(f"colunas removidas {removidas}")
    if tipo_alterado and not permitir_quebra:
        problemas.append(f"tipo alterado {tipo_alterado}")
    if colunas_documentadas is not None:
        sem_doc = sorted(set(campos_novos) - set(colunas_documentadas))
        doc_sem_coluna = sorted(set(colunas_documentadas) - set(campos_novos))
        if sem_doc:
            problemas.append(f"colunas sem documentação no column_docs {sem_doc}")
        if doc_sem_coluna:
            problemas.append(f"colunas documentadas que o lote não tem {doc_sem_coluna}")

    if problemas:
        raise ErroContratoEsquema(f"Contrato de schema de {nome_tabela} quebrado: " + "; ".join(problemas))
    if removidas or tipo_alterado:
        print(f"[schema] {nome_tabela}: quebra permitida - removidas={removidas} tipo_alterado={tipo_alterado}")
    if colunas_novas:
        print(f"[schema] {nome_tabela}: colunas novas {colunas_novas}")
    return colunas_novas


# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG
# ============================================================================
def escapar_string_sql(valor):
    # Spark SQL não aceita '' (padrão ANSI) como aspa literal - dá
    # ParseException. O escape que funciona é com backslash.
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def comentarios_a_aplicar(comentarios_atuais, comentarios_desejados):
    """{coluna: comentario} do que precisa de ALTER: coluna existe na tabela e o
    comentário atual é diferente. Coluna que a tabela ainda não tem fica de fora."""
    return {
        coluna: comentario
        for coluna, comentario in comentarios_desejados.items()
        if coluna in comentarios_atuais and comentarios_atuais[coluna] != comentario
    }


def aplicar_documentacao_tabela(spark, nome_completo_tabela, comentario_tabela=None, comentarios_colunas=None):
    """Aplica COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog, só no
    que mudou. Cada ALTER é um commit Delta (4-20s com o driver ocupado):
    reaplicar tudo custava 5-7 min por camada em toda execução."""
    if comentario_tabela and spark.catalog.getTable(nome_completo_tabela).description != comentario_tabela:
        spark.sql(f"COMMENT ON TABLE {nome_completo_tabela} IS '{escapar_string_sql(comentario_tabela)}'")

    if comentarios_colunas:
        atuais = {f.name: f.metadata.get("comment") for f in spark.table(nome_completo_tabela).schema.fields}
        mudaram = comentarios_a_aplicar(atuais, comentarios_colunas)
        for nome_coluna, comentario in mudaram.items():
            spark.sql(
                f"ALTER TABLE {nome_completo_tabela} "
                f"ALTER COLUMN `{nome_coluna}` COMMENT '{escapar_string_sql(comentario)}'"
            )
        print(f"[doc] {nome_completo_tabela}: {len(mudaram)} comentário(s) de coluna atualizado(s)")
