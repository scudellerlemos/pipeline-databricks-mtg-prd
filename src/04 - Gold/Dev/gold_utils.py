# Databricks notebook source
# ============================================================================
# GOLD UTILS - Módulo de Funções Utilitárias para Camada Gold
# ============================================================================
"""
Funções utilitárias da camada Gold: config, extract, load e auditoria.
A transformação de negócio fica em SQL dentro de cada notebook; mesmo padrão
de silver_utils.py.

Requer base_utils carregado antes:
  %run "../../00 - Common/Dev/base_utils"
  %run ./gold_utils

EXEMPLO DE USO NO NOTEBOOK:

%run "../../00 - Common/Dev/base_utils"
%run ./gold_utils

config = criar_config_manual("meu_catalog", "s3://meu-bucket")
processador = GoldTableProcessor("TB_DIM_CARTAS", config)

df_cartas = processador.extrair_da_silver("TB_FATO_CARTAS")
df_dim = processador.transformar_dados(df_cartas, funcao_transformacao)
processador.salvar_tabela_gold(df_dim, coluna_chave="ID_CARTA")
"""

import uuid
from datetime import datetime

from delta.tables import DeltaTable

# ============================================================================
# INFRAESTRUTURA COMUM (Spark session, Unity Catalog, secrets)
# %run isola o namespace de cada arquivo, então as funções de base_utils.py
# são buscadas no user_ns do IPython (mesmo esquema de silver_utils.py).
# ============================================================================
try:
    obter_sessao_spark, obter_segredo, configurar_unity_catalog, validar_contrato_esquema, campos_do_esquema, aplicar_documentacao_tabela
except NameError:
    try:
        import IPython
        _namespace_usuario = IPython.get_ipython().user_ns
        obter_sessao_spark = _namespace_usuario["obter_sessao_spark"]
        obter_segredo = _namespace_usuario["obter_segredo"]
        configurar_unity_catalog = _namespace_usuario["configurar_unity_catalog"]
        validar_contrato_esquema = _namespace_usuario["validar_contrato_esquema"]
        campos_do_esquema = _namespace_usuario["campos_do_esquema"]
        aplicar_documentacao_tabela = _namespace_usuario["aplicar_documentacao_tabela"]
    except Exception:
        pass
# ============================================================================

# ============================================================================
# FUNÇÕES DE CONFIGURAÇÃO
# ============================================================================
def criar_config_manual(catalogo, bucket_s3, prefixo_s3_gold=None):
    """
    Cria configuração manual sem usar secrets (para testes/desenvolvimento)

    Example:
        config = criar_config_manual("meu_catalog", "s3://meu-bucket")
        processador = GoldTableProcessor("TB_FATO_MERCADO_CARTAS", config)
    """
    return {
        'catalog_name': catalogo,
        'schema_silver': "silver",
        'schema_gold': "gold",
        's3_bucket': bucket_s3,
        # Override por ambiente via MTG_S3_GOLD_PREFIX; o argumento explicito vence.
        's3_gold_prefix': prefixo_s3_gold or obter_segredo("s3_gold_prefix", "gold")
    }

# ============================================================================
# FUNÇÕES DE EXTRAÇÃO DA SILVER
# ============================================================================
def extrair_da_silver(catalogo, nome_tabela_silver):
    """EXTRACT: lê dados de uma tabela da camada Silver"""
    sessao_spark = obter_sessao_spark()
    tabela_silver = f"{catalogo}.silver.{nome_tabela_silver}"
    # Sem try/except: tabela inexistente deve falhar aqui, com a causa real.
    df = sessao_spark.table(tabela_silver)
    print(f"Extraídos {df.count()} registros da Silver: {tabela_silver}")
    return df

# ============================================================================
# DOCUMENTAÇÃO NO UNITY CATALOG (aplicar_documentacao_tabela vem da base_utils)
# ============================================================================
def _escapar_string_sql(valor):
    # Spark SQL não aceita '' como aspas escapada; usa backslash.
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def _declarar_chave_primaria(sessao_spark, nome_completo_tabela, nome_tabela, colunas_chave):
    """Declara a PRIMARY KEY de colunas_chave em nome_completo_tabela no Unity Catalog.

    Unity Catalog exige NOT NULL na PK mas não garante unicidade, então um
    SELECT valida nulos e duplicatas antes e levanta RuntimeError com a
    contagem se houver. A chave nunca é mascarada com valor artificial.
    """
    nome_pk = f"pk_{nome_tabela.lower()}"

    somas_nulos = ", ".join(f"sum(case when `{k}` is null then 1 else 0 end) as `{k}`" for k in colunas_chave)
    concat_chave = "concat_ws('', " + ", ".join(f"cast(`{k}` as string)" for k in colunas_chave) + ")"
    expr_qtd_duplicadas = f"count(*) - count(distinct {concat_chave}) as __dup_count"
    linha = sessao_spark.sql(
        f"SELECT {somas_nulos}, {expr_qtd_duplicadas} FROM {nome_completo_tabela}"
    ).collect()[0]

    for k in colunas_chave:
        qtd_nulos = linha[k] or 0
        if qtd_nulos > 0:
            raise RuntimeError(
                f"Coluna chave '{k}' de {nome_completo_tabela} tem {qtd_nulos} linha(s) "
                f"com valor NULO - viola a premissa de chave única desta tabela. "
                f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
            )

    qtd_duplicadas = linha["__dup_count"] or 0
    if qtd_duplicadas > 0:
        raise RuntimeError(
            f"Chave ({', '.join(colunas_chave)}) de {nome_completo_tabela} tem {qtd_duplicadas} "
            f"linha(s) duplicada(s) - viola a premissa de chave única desta "
            f"tabela (Unity Catalog não enforca unicidade de PRIMARY KEY). "
            f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
        )

    for k in colunas_chave:
        sessao_spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN `{k}` SET NOT NULL")

    sessao_spark.sql(f"ALTER TABLE {nome_completo_tabela} DROP CONSTRAINT IF EXISTS {nome_pk}")
    sessao_spark.sql(
        f"ALTER TABLE {nome_completo_tabela} ADD CONSTRAINT {nome_pk} "
        f"PRIMARY KEY ({', '.join(colunas_chave)})"
    )


# ============================================================================
# FUNÇÃO DE CARREGAMENTO DELTA/UNITY CATALOG
# ============================================================================
def salvar_na_gold(df_final, catalogo, esquema, nome_tabela, caminho_s3_gold,
                   colunas_particao=None, coluna_chave=None,
                   comentario_tabela=None, comentarios_colunas=None, permitir_quebra_esquema=False,
                   incremental=False):
    """
    LOAD: grava df_final na camada Gold (Delta + Unity Catalog).

    Overwrite por padrão (tabela recalculada inteira: o que sai da Silver sai
    da Gold). incremental=True com a tabela já existente faz MERGE por
    coluna_chave: df_final é só o que entrou ou mudou. Lote com chave
    duplicada aborta antes de gravar.

    Args:
        df_final (DataFrame): DataFrame final para salvar
        catalogo, esquema, nome_tabela (str): identificação da tabela no Unity Catalog
        caminho_s3_gold (str): caminho/bucket S3 base para Gold (com ou sem "s3://")
        colunas_particao (list, optional): colunas para particionamento
        coluna_chave (str or list, optional): chave única, declarada como PRIMARY KEY
        comentario_tabela (str, optional): descrição de negócio da tabela (ver
            gold_column_docs.py).
        comentarios_colunas (dict, optional): {nome_coluna: descrição de negócio}
            (ver gold_column_docs.py). As chaves são o contrato de nomes: o lote tem
            que ter exatamente essas colunas.
        permitir_quebra_esquema (bool): True só para remover coluna ou mudar
            tipo de propósito. Sem isso, o contrato aborta antes de gravar.
        incremental (bool): MERGE em vez de overwrite (exige coluna_chave).
    """
    if not caminho_s3_gold.startswith("s3://"):
        caminho_s3_gold = f"s3://{caminho_s3_gold}"
    caminho_delta = f"{caminho_s3_gold}/{nome_tabela}"
    nome_completo_tabela = f"{catalogo}.{esquema}.{nome_tabela}"
    sessao_spark = obter_sessao_spark()

    sessao_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema}")

    colunas_chave = None
    if coluna_chave:
        colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)
        # Duplicata no lote é bug a montante (PK da Silver ou join): aborta em
        # vez de dropDuplicates, que esconderia o problema.
        qtd_duplicadas = df_final.groupBy(*colunas_chave).count().filter("count > 1").count()
        if qtd_duplicadas > 0:
            raise RuntimeError(
                f"Lote de {nome_completo_tabela} tem {qtd_duplicadas} chave(s) "
                f"({', '.join(colunas_chave)}) duplicada(s) - nada foi gravado. "
                f"Corrija a fonte/transformação."
            )

    # Contrato de schema antes de qualquer escrita (ver base_utils).
    arquivos_existem = DeltaTable.isDeltaTable(sessao_spark, caminho_delta)
    campos_atuais = campos_do_esquema(DeltaTable.forPath(sessao_spark, caminho_delta).toDF().schema) if arquivos_existem else {}
    validar_contrato_esquema(
        nome_completo_tabela, campos_atuais, campos_do_esquema(df_final.schema),
        colunas_documentadas=list(comentarios_colunas) if comentarios_colunas else None,
        permitir_quebra=permitir_quebra_esquema,
    )

    fazer_merge = incremental and arquivos_existem
    if fazer_merge:
        # <=> (null-safe): com "=", chave nula nunca casa e seria reinserida a cada run.
        condicao_merge = " AND ".join(f"gold.{k} <=> novo.{k}" for k in colunas_chave)
        (
            DeltaTable.forPath(sessao_spark, caminho_delta).alias("gold")
            .merge(df_final.alias("novo"), condicao_merge)
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # mergeSchema: coluna nova (já validada pelo contrato) entra; NOT NULL e PK
        # da tabela ficam. overwriteSchema trocaria o schema e derrubaria o NOT NULL.
        escritor = df_final.write.format("delta").mode("overwrite").option("mergeSchema", "true")
        if colunas_particao:
            escritor = escritor.partitionBy(*colunas_particao)
        escritor.save(caminho_delta)

    sessao_spark.sql(
        f"CREATE TABLE IF NOT EXISTS {nome_completo_tabela} USING DELTA LOCATION '{caminho_delta}'"
    )

    comentario_final_tabela = comentario_tabela
    if colunas_chave:
        nota_chave = f"Chave única: {', '.join(colunas_chave)}."
        comentario_final_tabela = f"{comentario_tabela} {nota_chave}" if comentario_tabela else nota_chave

    aplicar_documentacao_tabela(sessao_spark, nome_completo_tabela, comentario_final_tabela, comentarios_colunas)

    # No MERGE a PK já foi declarada na carga completa e o lote foi checado
    # acima; revalidar varreria a tabela inteira a cada run.
    if colunas_chave and not fazer_merge:
        _declarar_chave_primaria(sessao_spark, nome_completo_tabela, nome_tabela, colunas_chave)

    print("Dados salvos com sucesso na camada Gold!")


# ============================================================================
# DATA QUALITY E AUDITORIA
# ============================================================================
class DataQualityError(RuntimeError):
    """DQ estourou o limite. Carrega .resultados pra auditoria ainda ser gravada."""

    def __init__(self, mensagem, resultados):
        super().__init__(mensagem)
        self.resultados = resultados


def executar_checagens_dq(sessao_spark, rotulo, checagens):
    """
    Roda checagens de DQ (cada uma um SELECT que retorna uma contagem), loga
    e aborta se alguma passar do limite. Roda todas antes de abortar.

        "nome": consulta                 -> limite 0: qualquer ocorrencia aborta.
        "nome": (consulta, 12000)        -> aborta acima de 12000.
        "nome": (consulta, None)         -> so loga (contagem esperada > 0).

    Args:
        rotulo (str): de onde vem a checagem, so pro log (nome da tabela,
            "pré-join", etc).
        checagens (dict): {nome: consulta} ou {nome: (consulta, limite)}

    Returns:
        dict: {nome_da_checagem: contagem} - usado no resumo de auditoria.

    Raises:
        DataQualityError: se alguma contagem passou do limite.
    """
    resultados = {}
    estourados = []
    for nome, checagem in checagens.items():
        consulta, limite = checagem if isinstance(checagem, tuple) else (checagem, 0)
        contagem = sessao_spark.sql(consulta).collect()[0][0] or 0
        resultados[nome] = contagem

        if limite is None:
            nivel = "INFO"
        elif contagem > limite:
            nivel = "FALHA"
            estourados.append(f"{nome}={contagem} (limite {limite})")
        else:
            nivel = "OK"
        print(f"{nivel} DQ [{rotulo}] {nome}: {contagem}")

    if estourados:
        raise DataQualityError(
            f"DQ [{rotulo}] estourou o limite: " + "; ".join(estourados), resultados
        )
    return resultados


def iniciar_execucao_auditoria():
    """Abre um run de auditoria: id único (uuid4) + timestamp de início."""
    return {"id_execucao": str(uuid.uuid4()), "dt_inicio": datetime.now()}


def registrar_auditoria_gold(sessao_spark, catalogo, esquema, nome_tabela, execucao_auditoria,
                             qtd_lidos, qtd_processados, qtd_inseridos_atualizados,
                             dq_resultados, status):
    """
    Fecha o run e grava 1 linha em `{catalogo}.{esquema}.TB_AUDITORIA_GOLD`
    (criada na 1a chamada). O notebook chama isto num finally, então toda
    execução é registrada, inclusive as que abortam. Linhas são só inseridas.
    """
    dt_fim = datetime.now()
    duracao_segundos = (dt_fim - execucao_auditoria["dt_inicio"]).total_seconds()
    tabela_auditoria = f"{catalogo}.{esquema}.TB_AUDITORIA_GOLD"

    sessao_spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_auditoria} (
            ID_EXECUCAO STRING,
            NME_TABELA STRING,
            DT_INICIO TIMESTAMP,
            DT_FIM TIMESTAMP,
            QTD_SEGUNDOS_DURACAO DOUBLE,
            QTD_LIDOS BIGINT,
            QTD_PROCESSADOS BIGINT,
            QTD_INSERIDOS_ATUALIZADOS BIGINT,
            DESC_DQ_RESULTADO STRING,
            DESC_STATUS STRING
        ) USING DELTA
    """)

    dq_resumo = ", ".join(f"{k}={v}" for k, v in dq_resultados.items()) if dq_resultados else "sem checagens"

    sessao_spark.sql(f"""
        INSERT INTO {tabela_auditoria} VALUES (
            '{execucao_auditoria["id_execucao"]}',
            '{_escapar_string_sql(nome_tabela)}',
            '{execucao_auditoria["dt_inicio"].isoformat()}',
            '{dt_fim.isoformat()}',
            {duracao_segundos},
            {qtd_lidos},
            {qtd_processados},
            {qtd_inseridos_atualizados},
            '{_escapar_string_sql(dq_resumo)}',
            '{_escapar_string_sql(status)}'
        )
    """)

    print(f"Auditoria registrada em {tabela_auditoria} (run {execucao_auditoria['id_execucao']}, {duracao_segundos:.1f}s).")


# ============================================================================
# CLASSE AUXILIAR
# ============================================================================
class GoldTableProcessor:
    """Classe para processar tabelas Gold com padrões comuns"""

    def __init__(self, nome_tabela, config):
        self.nome_tabela = nome_tabela
        self.config = config
        self.spark = obter_sessao_spark()
        self.caminho_s3_gold = f"{self.config['s3_bucket']}/{self.config['s3_gold_prefix']}"

        configurar_unity_catalog(self.config['catalog_name'], self.config['schema_gold'])

    def extrair_da_silver(self, nome_tabela_silver):
        """Extrai dados de uma tabela Silver"""
        return extrair_da_silver(self.config['catalog_name'], nome_tabela_silver)

    def transformar_dados(self, df, funcao_transformacao, **kwargs):
        """Aplica funcao_transformacao ao df (a lógica fica no notebook)."""
        if funcao_transformacao:
            return funcao_transformacao(df, **kwargs)
        return df

    def salvar_tabela_gold(self, df, colunas_particao=None, coluna_chave=None,
                           comentario_tabela=None, comentarios_colunas=None,
                           permitir_quebra_esquema=False, incremental=False):
        """Salva tabela na Gold com configurações padrão"""
        salvar_na_gold(
            df_final=df,
            catalogo=self.config['catalog_name'],
            esquema=self.config['schema_gold'],
            nome_tabela=self.nome_tabela,
            caminho_s3_gold=self.caminho_s3_gold,
            colunas_particao=colunas_particao,
            coluna_chave=coluna_chave,
            comentario_tabela=comentario_tabela,
            comentarios_colunas=comentarios_colunas,
            permitir_quebra_esquema=permitir_quebra_esquema,
            incremental=incremental
        )

        print(f"{self.nome_tabela} criada com sucesso!")
        print(f"Tabela criada: {self.config['catalog_name']}.{self.config['schema_gold']}.{self.nome_tabela}")
