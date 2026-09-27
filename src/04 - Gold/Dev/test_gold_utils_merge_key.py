# ponytail: self-check de lógica pura pro branch key_column/dedup em save_to_gold()
# (gold_utils.py). Não dá pra importar esse módulo diretamente aqui (precisa de pyspark
# e uma sessão spark/dbutils viva do Databricks, nenhuma disponível fora de um cluster),
# então isto espelha só a lógica de merge-condition sob teste. Mesma convenção de
# test_silver_utils_merge_key.py.


def build_merge_plan(key_column):
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)
    # <=> (igualdade null-safe): um "=" simples nunca casa quando uma coluna
    # de chave é NULL, o que reinseriria essa linha a cada execução.
    merge_condition = " AND ".join(f"gold.{k} <=> novo.{k}" for k in key_cols)
    return key_cols, merge_condition


def test_composite_key_mercado_cartas():
    # chave real de TB_FATO_MERCADO_CARTAS: (ID_CARTA, DT_COTACAO)
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
