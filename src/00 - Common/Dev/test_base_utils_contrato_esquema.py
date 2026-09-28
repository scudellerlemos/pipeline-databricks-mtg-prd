# Testa o validar_contrato_esquema() real de base_utils.py com dicts {coluna: tipo}.

import importlib.util
import os

_CAMINHO = os.path.join(os.path.dirname(__file__), "base_utils.py")
_ESPEC = importlib.util.spec_from_file_location("base_utils", _CAMINHO)
base_utils = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(base_utils)

validar = base_utils.validar_contrato_esquema
ATUAIS = {"id": "string", "cmc": "double"}


def _falha(*args, **kwargs):
    try:
        validar(*args, **kwargs)
    except base_utils.ErroContratoEsquema as e:
        return str(e)
    raise AssertionError("deveria ter quebrado o contrato")


def test_primeira_carga_passa_sem_colunas_novas():
    assert validar("t", {}, ATUAIS) == []


def test_coluna_nova_passa_e_e_devolvida():
    assert validar("t", ATUAIS, {**ATUAIS, "set": "string"}) == ["set"]


def test_coluna_removida_aborta():
    assert "removidas ['cmc']" in _falha("t", ATUAIS, {"id": "string"})


def test_tipo_alterado_aborta():
    assert "cmc (double -> string)" in _falha("t", ATUAIS, {"id": "string", "cmc": "string"})


def test_permitir_quebra_libera_remocao_e_tipo():
    assert validar("t", ATUAIS, {"id": "int"}, permitir_quebra=True) == []


def test_lote_tem_que_bater_com_column_docs_mesmo_com_permitir_quebra():
    msg = _falha("t", {}, ATUAIS, colunas_documentadas=["id", "nme"], permitir_quebra=True)
    assert "sem documentação no column_docs ['cmc']" in msg
    assert "documentadas que o lote não tem ['nme']" in msg


if __name__ == "__main__":
    for nome, f in list(globals().items()):
        if nome.startswith("test_"):
            f()
    print("contrato de schema: OK")
