# Testa a montagem da chave de merge e do desempate de salvar_na_silver()
# (silver_utils.py). O módulo exige pyspark/Databricks, então a lógica é copiada
# aqui; manter em sincronia.


def montar_plano_merge(coluna_chave):
    colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)
    condicao_merge = " AND ".join(f"silver.{k} <=> novo.{k}" for k in colunas_chave)
    return colunas_chave, condicao_merge


def test_coluna_chave_unica_string():
    colunas_chave, condicao = montar_plano_merge("ID_CARTA")
    assert colunas_chave == ["ID_CARTA"]
    assert condicao == "silver.ID_CARTA <=> novo.ID_CARTA"


def test_chave_composta_precos_preserva_historico():
    colunas_chave, condicao = montar_plano_merge(["ID_CARTA", "DT_INGESTAO"])
    assert colunas_chave == ["ID_CARTA", "DT_INGESTAO"]
    assert condicao == "silver.ID_CARTA <=> novo.ID_CARTA AND silver.DT_INGESTAO <=> novo.DT_INGESTAO"


def montar_colunas_desempate(colunas, colunas_chave, coluna_ordenacao):
    return [c for c in colunas if c not in colunas_chave and c != coluna_ordenacao]


def test_colunas_desempate_exclui_chave_e_ordenacao():
    colunas = montar_colunas_desempate(
        ["NME_CARTA", "COD_COLECAO", "DESC_CARTA", "DT_INGESTAO"], ["NME_CARTA", "COD_COLECAO"], "DT_INGESTAO"
    )
    assert colunas == ["DESC_CARTA"]


def test_colunas_desempate_vazia_quando_chave_e_ordenacao_cobrem_tudo():
    colunas = montar_colunas_desempate(["ID_CARTA", "DT_INGESTAO"], ["ID_CARTA"], "DT_INGESTAO")
    assert colunas == []


if __name__ == "__main__":
    test_coluna_chave_unica_string()
    test_chave_composta_precos_preserva_historico()
    test_colunas_desempate_exclui_chave_e_ordenacao()
    test_colunas_desempate_vazia_quando_chave_e_ordenacao_cobrem_tudo()
    print("OK")
