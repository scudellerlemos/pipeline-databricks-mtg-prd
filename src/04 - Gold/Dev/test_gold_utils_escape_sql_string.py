# Testa a lógica de gold_utils._escapar_string_sql. O módulo precisa de
# pyspark/delta, então a função é espelhada aqui.


def escapar_string_sql(valor):
    """Espelho de gold_utils._escapar_string_sql."""
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def test_apostrofo_escapado_com_backslash_nao_duplicado():
    # Spark SQL nao aceita '' (ANSI) como aspas escapada.
    assert escapar_string_sql("carta do jogador") == "carta do jogador"
    assert escapar_string_sql("it's a trap") == "it\\'s a trap"


def test_backslash_literal_escapado_antes_do_apostrofo():
    # backslash e escapado antes do apostrofo, senao o \' gerado seria reprocessado.
    assert escapar_string_sql("a\\b") == "a\\\\b"
    assert escapar_string_sql("a\\'b") == "a\\\\\\'b"


if __name__ == "__main__":
    test_apostrofo_escapado_com_backslash_nao_duplicado()
    test_backslash_literal_escapado_antes_do_apostrofo()
    print("OK")
