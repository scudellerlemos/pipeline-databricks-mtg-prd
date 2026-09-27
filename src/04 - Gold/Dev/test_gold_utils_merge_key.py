# Testa a condição de merge por key_column de save_to_gold (gold_utils.py).
# O módulo precisa de pyspark/Databricks, então a lógica é espelhada aqui.


def build_merge_plan(key_column):
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
    # <=> (null-safe): com "=", chave NULL nunca casa e seria reinserida a cada execução.
    merge_condition = " AND ".join(f"gold.{k} <=> novo.{k}" for k in key_cols)
    return key_cols, merge_condition


def test_composite_key_mercado_cartas():
    key_cols, condition = build_merge_plan(["ID_CARTA", "DT_COTACAO"])
    assert key_cols == ["ID_CARTA", "DT_COTACAO"]
    assert condition == "gold.ID_CARTA <=> novo.ID_CARTA AND gold.DT_COTACAO <=> novo.DT_COTACAO"


def test_single_key_column_string():
    key_cols, condition = build_merge_plan("ID_CARTA")
    assert key_cols == ["ID_CARTA"]
    assert condition == "gold.ID_CARTA <=> novo.ID_CARTA"


if __name__ == "__main__":
    test_composite_key_mercado_cartas()
    test_single_key_column_string()
    print("OK")
