# Testa o regex de extração de símbolos de TB_PONTE_CARTA_SIMBOLOS.py com `re`,
# no lugar de regexp_extract_all/posexplode do Spark.
import re

PADRAO_SIMBOLO = r"\[[^\]]*\]"


def extrair_simbolos(desc_custo_mana):
    if desc_custo_mana is None or desc_custo_mana == "NA":
        return []
    return re.findall(PADRAO_SIMBOLO, desc_custo_mana)


def test_custo_simples():
    assert extrair_simbolos("[R]") == ["[R]"]


def test_custo_com_generico_e_repeticao():
    assert extrair_simbolos("[2][U][U]") == ["[2]", "[U]", "[U]"]


def test_carta_sem_custo_de_mana_nao_gera_simbolo():
    assert extrair_simbolos("NA") == []


def test_custo_nulo_nao_gera_simbolo():
    assert extrair_simbolos(None) == []


def test_posicao_comeca_em_1_apos_enumerar():
    simbolos = extrair_simbolos("[2][U][U]")
    posicoes = list(enumerate(simbolos, start=1))
    assert posicoes == [(1, "[2]"), (2, "[U]"), (3, "[U]")]
