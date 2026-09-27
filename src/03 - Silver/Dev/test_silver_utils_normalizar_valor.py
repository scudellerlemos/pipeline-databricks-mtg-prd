# ponytail: self-check para normalizar_valor() (silver_utils.py). Mesmo
# problema de import dos outros test_silver_utils_*.py: aquele módulo exige
# pyspark + sessão spark/dbutils ao vivo, e o pyspark local aqui não sobe uma
# JVM (ambiente sem JDK compatível) - então mirror só da parte que não é
# Column API: o núcleo de remoção de acento (unicodedata), que é puro Python
# e é a peça que o usuário pediu pra simplificar (era um translate() com mapa
# de caracteres digitado à mão).

import unicodedata


def remover_acentos(texto):
    """Mirror de _remover_acentos em silver_utils.py."""
    if texto is None:
        return None
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')


def normalizar_valor_str(valor):
    """Mirror do comportamento de normalizar_valor() operando em str/None
    (equivalente Python puro do Column expression: mesmo passa-direto pra
    NULL/vazio/'NA', mesmo Title_Case com '_' no lugar de espaço)."""
    if valor is None or valor.strip() == '' or valor == 'NA':
        return valor
    sem_acento = remover_acentos(valor.strip())
    return '_'.join(p.capitalize() for p in sem_acento.split(' '))


def test_remove_acentos_cobre_qualquer_caractere_acentuado():
    # cobre caracteres fora do mapa manual antigo (ex.: ï, ñ em contexto novo)
    assert remover_acentos("São Paulo Ação") == "Sao Paulo Acao"
    assert remover_acentos("café ïnça") == "cafe inca"
    assert remover_acentos(None) is None


def test_normalizar_valor_title_case_com_underscore():
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
    test_normalizar_valor_title_case_com_underscore()
    test_normalizar_valor_passa_direto_null_vazio_e_na()
    print("OK")
