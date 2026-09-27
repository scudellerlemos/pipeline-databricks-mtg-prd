# ponytail: self-check de lógica pura pra transform_ponte_carta_simbolos() em
# TB_PONTE_CARTA_SIMBOLOS.py. Não dá pra rodar regexp_extract_all/posexplode
# do Spark localmente, então isto espelha a extração de símbolos via re
# (mesmo padrão de test_migration_chain.py).
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


def test_posicao_e_1_based_apos_enumerar():
    simbolos = extrair_simbolos("[2][U][U]")
    posicoes = list(enumerate(simbolos, start=1))
    assert posicoes == [(1, "[2]"), (2, "[U]"), (3, "[U]")]
