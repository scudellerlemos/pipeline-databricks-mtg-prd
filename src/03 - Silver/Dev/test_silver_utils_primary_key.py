# Testa _declarar_chave_primaria() de silver_utils.py. O módulo exige
# pyspark/Databricks, então a função é copiada aqui; manter em sincronia.


class FakeSpark:
    """Registra as chamadas a spark.sql(). O SELECT de validação devolve
    {coluna: qtd_nulos, "__dup_count": n}; o resto (DDL) não faz nada."""

    def __init__(self, qtd_nulos_por_coluna=None, qtd_duplicadas=0):
        self.linha = dict(qtd_nulos_por_coluna or {})
        self.linha["__dup_count"] = qtd_duplicadas
        self.chamadas = []

    def sql(self, consulta):
        self.chamadas.append(consulta)
        if "sum(case when" in consulta:
            return _FakeResult(self.linha)
        return _FakeResult(None)


class _FakeResult:
    def __init__(self, linha):
        self._linha = linha

    def collect(self):
        return [self._linha]


def declarar_chave_primaria(spark, nome_completo_tabela, nome_tabela, colunas_chave):
    """Cópia de silver_utils._declarar_chave_primaria."""
    nome_pk = f"pk_{nome_tabela.lower()}"

    somas_nulos = ", ".join(f"sum(case when `{k}` is null then 1 else 0 end) as `{k}`" for k in colunas_chave)
    concat_chave = "concat_ws('', " + ", ".join(f"cast(`{k}` as string)" for k in colunas_chave) + ")"
    expr_qtd_duplicadas = f"count(*) - count(distinct {concat_chave}) as __dup_count"
    linha = spark.sql(f"SELECT {somas_nulos}, {expr_qtd_duplicadas} FROM {nome_completo_tabela}").collect()[0]

    for k in colunas_chave:
        qtd_nulos = linha.get(k) or 0
        if qtd_nulos > 0:
            raise RuntimeError(
                f"Coluna chave '{k}' de {nome_completo_tabela} tem {qtd_nulos} linha(s) "
                f"com valor NULO - viola a premissa de chave única desta tabela. "
                f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
            )

    qtd_duplicadas = linha.get("__dup_count") or 0
    if qtd_duplicadas > 0:
        raise RuntimeError(
            f"Chave ({', '.join(colunas_chave)}) de {nome_completo_tabela} tem {qtd_duplicadas} "
            f"linha(s) duplicada(s) - viola a premissa de chave única desta "
            f"tabela (Unity Catalog não enforca unicidade de PRIMARY KEY). "
            f"Corrija a fonte/transformação antes de declarar PRIMARY KEY."
        )

    for k in colunas_chave:
        spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN `{k}` SET NOT NULL")

    spark.sql(f"ALTER TABLE {nome_completo_tabela} DROP CONSTRAINT IF EXISTS {nome_pk}")
    spark.sql(
        f"ALTER TABLE {nome_completo_tabela} ADD CONSTRAINT {nome_pk} "
        f"PRIMARY KEY ({', '.join(colunas_chave)})"
    )


def test_chave_nula_falha_com_contagem_exata_e_pula_constraint():
    spark = FakeSpark(qtd_nulos_por_coluna={"ID_CARTA": 3})
    try:
        declarar_chave_primaria(spark, "cat.silver.TB_FATO_CARTAS", "TB_FATO_CARTAS", ["ID_CARTA"])
        assert False, "esperava RuntimeError"
    except RuntimeError as e:
        assert "3 linha(s)" in str(e)
        assert "ID_CARTA" in str(e)
    # Nenhum DDL roda depois da falha.
    assert len(spark.chamadas) == 1
    assert "sum(case when" in spark.chamadas[0]


def test_sem_nulo_declara_constraint_em_ordem():
    spark = FakeSpark(qtd_nulos_por_coluna={"COD_COLECAO": 0})
    declarar_chave_primaria(spark, "cat.silver.TB_DIM_COLECOES", "TB_DIM_COLECOES", ["COD_COLECAO"])
    assert len(spark.chamadas) == 4
    assert "sum(case when" in spark.chamadas[0]
    assert "SET NOT NULL" in spark.chamadas[1]
    assert "DROP CONSTRAINT IF EXISTS pk_tb_dim_colecoes" in spark.chamadas[2]
    assert "ADD CONSTRAINT pk_tb_dim_colecoes" in spark.chamadas[3]
    assert "PRIMARY KEY (COD_COLECAO)" in spark.chamadas[3]


def test_chave_composta_checa_todas_colunas_em_uma_leitura():
    spark = FakeSpark(qtd_nulos_por_coluna={"ID_CARTA": 0, "DT_INGESTAO": 0})
    declarar_chave_primaria(
        spark, "cat.silver.TB_FATO_PRECOS_CARTAS", "TB_FATO_PRECOS_CARTAS",
        ["ID_CARTA", "DT_INGESTAO"],
    )
    # 1 SELECT + 2 SET NOT NULL + DROP + ADD
    assert len(spark.chamadas) == 5
    assert spark.chamadas[0].count("sum(case when") == 2
    assert "ID_CARTA" in spark.chamadas[0] and "DT_INGESTAO" in spark.chamadas[0]


def test_chave_composta_segunda_coluna_nula_para_antes_de_ddl():
    spark = FakeSpark(qtd_nulos_por_coluna={"ID_CARTA": 0, "DT_INGESTAO": 1})
    try:
        declarar_chave_primaria(
            spark, "cat.silver.TB_FATO_PRECOS_CARTAS", "TB_FATO_PRECOS_CARTAS",
            ["ID_CARTA", "DT_INGESTAO"],
        )
        assert False, "esperava RuntimeError"
    except RuntimeError as e:
        assert "DT_INGESTAO" in str(e)
    assert len(spark.chamadas) == 1


def test_chave_duplicada_falha_com_contagem_exata_e_pula_constraint():
    spark = FakeSpark(qtd_nulos_por_coluna={"COD_COLECAO": 0}, qtd_duplicadas=2)
    try:
        declarar_chave_primaria(spark, "cat.silver.TB_DIM_COLECOES", "TB_DIM_COLECOES", ["COD_COLECAO"])
        assert False, "esperava RuntimeError"
    except RuntimeError as e:
        assert "2 linha(s) duplicada(s)" in str(e)
        assert "COD_COLECAO" in str(e)
    assert len(spark.chamadas) == 1


def test_expr_qtd_duplicadas_usa_distinct_concat_chave_na_mesma_leitura():
    spark = FakeSpark(qtd_nulos_por_coluna={"COD_COLECAO": 0}, qtd_duplicadas=0)
    declarar_chave_primaria(spark, "cat.silver.TB_DIM_COLECOES", "TB_DIM_COLECOES", ["COD_COLECAO"])
    assert len(spark.chamadas) == 4
    assert "count(distinct concat_ws(" in spark.chamadas[0]
    assert "__dup_count" in spark.chamadas[0]


if __name__ == "__main__":
    test_chave_nula_falha_com_contagem_exata_e_pula_constraint()
    test_sem_nulo_declara_constraint_em_ordem()
    test_chave_composta_checa_todas_colunas_em_uma_leitura()
    test_chave_composta_segunda_coluna_nula_para_antes_de_ddl()
    test_chave_duplicada_falha_com_contagem_exata_e_pula_constraint()
    test_expr_qtd_duplicadas_usa_distinct_concat_chave_na_mesma_leitura()
    print("OK")
