# Testa _escapar_string_sql() de silver_utils.py. O módulo exige pyspark/delta,
# então a função é copiada aqui; manter em sincronia.


def escapar_string_sql(valor):
    """Cópia de silver_utils._escapar_string_sql."""
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def test_apostrofo_escapado_com_backslash_nao_duplicado():
    # Spark SQL não aceita '' como aspas literal; só backslash.
    assert escapar_string_sql("carta do jogador") == "carta do jogador"
    assert escapar_string_sql("o que e essa carta") == "o que e essa carta"
    assert escapar_string_sql("it's a trap") == "it\\'s a trap"


def test_backslash_literal_escapado_antes_da_aspa():
    # Backslash é escapado antes da aspa, senão o \' gerado seria escapado de novo.
    assert escapar_string_sql("a\\b") == "a\\\\b"
    assert escapar_string_sql("a\\'b") == "a\\\\\\'b"


if __name__ == "__main__":
    test_apostrofo_escapado_com_backslash_nao_duplicado()
    test_backslash_literal_escapado_antes_da_aspa()
    print("OK")
