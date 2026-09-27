# ponytail: self-check de lógica pura pro branch key_column/dedup em save_to_silver()
# (silver_utils.py). Não dá pra importar esse módulo diretamente aqui (precisa de pyspark
# e uma sessão spark/dbutils viva do Databricks, nenhuma disponível fora de um cluster),
# então isto espelha só a lógica de merge-condition sob teste.


def build_merge_plan(key_column):
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
    # <=> (igualdade null-safe): um "=" simples nunca casa quando uma coluna
    # de chave é NULL, o que reinseriria essa linha a cada execução.
    merge_condition = " AND ".join(f"silver.{k} <=> novo.{k}" for k in key_cols)
    return key_cols, merge_condition


def test_single_key_column_string():
    key_cols, condition = build_merge_plan("ID_CARD")
    assert key_cols == ["ID_CARD"]
    assert condition == "silver.ID_CARD <=> novo.ID_CARD"


def test_composite_key_cardprices_preserves_history():
    key_cols, condition = build_merge_plan(["ID_CARD", "DT_INGESTION"])
    assert key_cols == ["ID_CARD", "DT_INGESTION"]
    assert condition == "silver.ID_CARD <=> novo.ID_CARD AND silver.DT_INGESTION <=> novo.DT_INGESTION"


def build_tie_break_cols(columns, key_cols, order_by_col):
    return [c for c in columns if c not in key_cols and c != order_by_col]


def test_tie_break_cols_excludes_key_and_order_by():
    cols = build_tie_break_cols(
        ["NME_CARD", "COD_SET", "DESC_CARD", "DT_INGESTION"], ["NME_CARD", "COD_SET"], "DT_INGESTION"
    )
    assert cols == ["DESC_CARD"]


def test_tie_break_cols_empty_when_key_and_order_by_cover_all():
    cols = build_tie_break_cols(["ID_CARD", "DT_INGESTION"], ["ID_CARD"], "DT_INGESTION")
    assert cols == []


if __name__ == "__main__":
    test_single_key_column_string()
    test_composite_key_cardprices_preserves_history()
    test_tie_break_cols_excludes_key_and_order_by()
    test_tie_break_cols_empty_when_key_and_order_by_cover_all()
    print("OK")
