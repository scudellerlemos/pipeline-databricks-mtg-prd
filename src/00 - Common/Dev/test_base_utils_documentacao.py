# Testa escapar_string_sql() e comentarios_a_aplicar() reais de base_utils.py.

import importlib.util
import os

_CAMINHO = os.path.join(os.path.dirname(__file__), "base_utils.py")
_ESPEC = importlib.util.spec_from_file_location("base_utils", _CAMINHO)
base_utils = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(base_utils)

escapar_string_sql = base_utils.escapar_string_sql
a_aplicar = base_utils.comentarios_a_aplicar


def test_apostrofo_escapado_com_backslash_nao_duplicado():
    # Spark SQL não aceita '' como aspas literal; só backslash.
    assert escapar_string_sql("carta do jogador") == "carta do jogador"
    assert escapar_string_sql("it's a trap") == "it\\'s a trap"


def test_backslash_literal_escapado_antes_da_aspa():
    # Backslash é escapado antes da aspa, senão o \' gerado seria escapado de novo.
    assert escapar_string_sql("a\\b") == "a\\\\b"
    assert escapar_string_sql("a\\'b") == "a\\\\\\'b"


def test_so_reaplica_comentario_que_mudou():
    atuais = {"id": "Id da carta", "nome": None, "cmc": "velho"}
    desejados = {"id": "Id da carta", "nome": "Nome", "cmc": "novo", "nao_existe": "x"}
    # igual -> fica de fora; sem comentário ou diferente -> entra; coluna ausente -> fica de fora
    assert a_aplicar(atuais, desejados) == {"nome": "Nome", "cmc": "novo"}
    assert a_aplicar(atuais, {"id": "Id da carta"}) == {}
