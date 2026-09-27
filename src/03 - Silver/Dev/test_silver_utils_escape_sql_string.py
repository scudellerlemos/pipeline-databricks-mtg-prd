# Testa _escape_sql_string() de silver_utils.py. O módulo exige pyspark/delta,
# então a função é copiada aqui; manter em sincronia.


def escape_sql_string(value):
    """Cópia de silver_utils._escape_sql_string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def test_apostrophe_is_backslash_escaped_not_doubled():
    # Spark SQL não aceita '' como aspas literal; só backslash.
    assert escape_sql_string("carta do jogador") == "carta do jogador"
    assert escape_sql_string("o que e essa carta") == "o que e essa carta"
    assert escape_sql_string("it's a trap") == "it\\'s a trap"


def test_literal_backslash_is_escaped_before_the_quote_pass():
    # Backslash é escapado antes da aspa, senão o \' gerado seria escapado de novo.
    assert escape_sql_string("a\\b") == "a\\\\b"
    assert escape_sql_string("a\\'b") == "a\\\\\\'b"


if __name__ == "__main__":
    test_apostrophe_is_backslash_escaped_not_doubled()
    test_literal_backslash_is_escaped_before_the_quote_pass()
    print("OK")
