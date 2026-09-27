# Testa normalizar_valor() de silver_utils.py com um equivalente em Python puro
# (o módulo exige pyspark/Databricks). Manter em sincronia.

import unicodedata


def _remover_acentos(texto):
    """Cópia de _remover_acentos em silver_utils.py."""
    if texto is None:
        return None
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')


def normalizar_valor_str(valor):
    """Equivalente de normalizar_valor() para str/None."""
    if valor is None or valor.strip() == '' or valor == 'NA':
        return valor
    sem_acento = _remover_acentos(valor.strip())
    return '_'.join(p.capitalize() for p in sem_acento.split(' '))


def test_remove_acentos_cobre_qualquer_caractere_acentuado():
    assert _remover_acentos("São Paulo Ação") == "Sao Paulo Acao"
    assert _remover_acentos("café ïnça") == "cafe inca"
    assert _remover_acentos(None) is None


def test_normalizar_valor_capitaliza_palavras_com_underscore():
    assert normalizar_valor_str("mana vermelha") == "Mana_Vermelha"
    assert normalizar_valor_str("MANA VERMELHA") == "Mana_Vermelha"
    assert normalizar_valor_str("Água") == "Agua"


def test_normalizar_valor_passa_direto_null_vazio_e_na():
    assert normalizar_valor_str(None) is None
    assert normalizar_valor_str("") == ""
    assert normalizar_valor_str("   ") == "   "
    assert normalizar_valor_str("NA") == "NA"


if __name__ == "__main__":
    test_remove_acentos_cobre_qualquer_caractere_acentuado()
    test_normalizar_valor_capitaliza_palavras_com_underscore()
    test_normalizar_valor_passa_direto_null_vazio_e_na()
    print("OK")
