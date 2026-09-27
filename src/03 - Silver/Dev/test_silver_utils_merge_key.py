# Testa a montagem da chave de merge e do desempate de save_to_silver()
# (silver_utils.py). O módulo exige pyspark/Databricks, então a lógica é copiada
# aqui; manter em sincronia.


def build_merge_plan(key_column):
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
    merge_condition = " AND ".join(f"silver.{k} <=> novo.{k}" for k in key_cols)
    return key_cols, merge_condition


def test_single_key_column_string():
    key_cols, condition = build_merge_plan("ID_CARTA")
    assert key_cols == ["ID_CARTA"]
    assert condition == "silver.ID_CARTA <=> novo.ID_CARTA"


def test_composite_key_cardprices_preserves_history():
    key_cols, condition = build_merge_plan(["ID_CARTA", "DT_INGESTAO"])
    assert key_cols == ["ID_CARTA", "DT_INGESTAO"]
    assert condition == "silver.ID_CARTA <=> novo.ID_CARTA AND silver.DT_INGESTAO <=> novo.DT_INGESTAO"


def build_tie_break_cols(columns, key_cols, order_by_col):
    return [c for c in columns if c not in key_cols and c != order_by_col]


def test_tie_break_cols_excludes_key_and_order_by():
    cols = build_tie_break_cols(
        ["NME_CARTA", "COD_COLECAO", "DESC_CARTA", "DT_INGESTAO"], ["NME_CARTA", "COD_COLECAO"], "DT_INGESTAO"
    )
    assert cols == ["DESC_CARTA"]


def test_tie_break_cols_empty_when_key_and_order_by_cover_all():
    cols = build_tie_break_cols(["ID_CARTA", "DT_INGESTAO"], ["ID_CARTA"], "DT_INGESTAO")
    assert cols == []


if __name__ == "__main__":
    test_single_key_column_string()
    test_composite_key_cardprices_preserves_history()
    test_tie_break_cols_excludes_key_and_order_by()
    test_tie_break_cols_empty_when_key_and_order_by_cover_all()
    print("OK")
