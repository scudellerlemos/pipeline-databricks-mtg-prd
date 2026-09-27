# ponytail: self-check de lógica pura pra _escape_sql_string() (silver_utils.py).
# Não dá pra importar esse módulo diretamente aqui (precisa de pyspark/delta,
# indisponível fora de um cluster Databricks), então isto espelha só a lógica de escaping.


def escape_sql_string(value):
    """Espelho de silver_utils._escape_sql_string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def test_apostrophe_is_backslash_escaped_not_doubled():
    # Spark SQL rejeita '' (dobrar aspas, convencao ANSI) dentro de um
    # single-quoted string literal - so backslash funciona aqui.
    assert escape_sql_string("carta do jogador") == "carta do jogador"
    assert escape_sql_string("o que e essa carta") == "o que e essa carta"
    assert escape_sql_string("it's a trap") == "it\\'s a trap"


def test_literal_backslash_is_escaped_before_the_quote_pass():
    # backslash precisa ser escapado primeiro, senao o \' virado por um
    # apostrofo anterior seria re-processado como se fosse um escape novo.
    assert escape_sql_string("a\\b") == "a\\\\b"
    assert escape_sql_string("a\\'b") == "a\\\\\\'b"


if __name__ == "__main__":
    test_apostrophe_is_backslash_escaped_not_doubled()
    test_literal_backslash_is_escaped_before_the_quote_pass()
    print("OK")
