# Testa a condição de merge por coluna_chave de salvar_na_gold (gold_utils.py).
# O módulo precisa de pyspark/Databricks, então a lógica é espelhada aqui.


def montar_plano_merge(coluna_chave):
    colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)
    # <=> (null-safe): com "=", chave NULL nunca casa e seria reinserida a cada execução.
    condicao_merge = " AND ".join(f"gold.{k} <=> novo.{k}" for k in colunas_chave)
    return colunas_chave, condicao_merge


def test_chave_composta_mercado_cartas():
    colunas_chave, condicao = montar_plano_merge(["ID_CARTA", "DT_COTACAO"])
    assert colunas_chave == ["ID_CARTA", "DT_COTACAO"]
    assert condicao == "gold.ID_CARTA <=> novo.ID_CARTA AND gold.DT_COTACAO <=> novo.DT_COTACAO"


def test_coluna_chave_unica_string():
    colunas_chave, condicao = montar_plano_merge("ID_CARTA")
    assert colunas_chave == ["ID_CARTA"]
    assert condicao == "gold.ID_CARTA <=> novo.ID_CARTA"


if __name__ == "__main__":
    test_chave_composta_mercado_cartas()
    test_coluna_chave_unica_string()
    print("OK")
