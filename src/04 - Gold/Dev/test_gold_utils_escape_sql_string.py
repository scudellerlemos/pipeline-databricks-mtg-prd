# Testa a lógica de gold_utils._escape_sql_string. O módulo precisa de
# pyspark/delta, então a função é espelhada aqui.


def escape_sql_string(value):
    """Espelho de gold_utils._escape_sql_string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def test_apostrophe_is_backslash_escaped_not_doubled():
    # Spark SQL nao aceita '' (ANSI) como aspas escapada.
    assert escape_sql_string("carta do jogador") == "carta do jogador"
    assert escape_sql_string("it's a trap") == "it\\'s a trap"


def test_literal_backslash_is_escaped_before_the_quote_pass():
    # backslash e escapado antes do apostrofo, senao o \' gerado seria reprocessado.
    assert escape_sql_string("a\\b") == "a\\\\b"
    assert escape_sql_string("a\\'b") == "a\\\\\\'b"


if __name__ == "__main__":
    test_apostrophe_is_backslash_escaped_not_doubled()
    test_literal_backslash_is_escaped_before_the_quote_pass()
    print("OK")
