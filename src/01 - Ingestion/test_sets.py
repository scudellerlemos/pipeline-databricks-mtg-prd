# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (resposta única do Scryfall /sets).

import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "sets.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE SETS"


def _carregar_funcoes(requisicao_falsa):
    celulas = open(_CAMINHO_NOTEBOOK, encoding="utf-8").read().split("# COMMAND ----------")
    codigo_celula = next(c for c in celulas if _MARCADOR in c)

    escopo = {
        "json": json,
        "obter_http_com_retentativa": lambda url, cabecalhos=None, tempo_limite=30, tentativas=3: requisicao_falsa(url, cabecalhos=cabecalhos, tempo_limite=tempo_limite),
        "StructType": lambda campos: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "IntegerType": lambda: None,
        "BooleanType": lambda: None,
        "URL_API_SCRYFALL": "https://api.scryfall.test",
        "CABECALHOS_SCRYFALL": {},
        "MAX_TENTATIVAS": 3,
        "configurar_armazenamento_s3": lambda *a, **k: True,
        "CAMINHO_S3_STAGE": "s3://test-bucket/stage",
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_colecao"], escopo["buscar_todas_colecoes"], escopo["limpar_dados_colecoes"]


class _Resp:
    def __init__(self, dados_json):
        self._dados_json = dados_json

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        pass


def _requisicao_falsa_para(dados_colecoes):
    def requisicao_falsa(url, cabecalhos=None, tempo_limite=None):
        assert url.endswith("/sets")
        return _Resp({"object": "list", "has_more": False, "data": dados_colecoes})
    return requisicao_falsa


def test_buscar_todas_colecoes_mapeia_campos_em_um_request():
    dados_colecoes = [
        {"code": "lea", "name": "Limited Edition Alpha", "set_type": "core",
         "released_at": "1993-08-05", "digital": False},
        {"code": "trc", "name": "Star Trek Commander", "set_type": "commander",
         "released_at": "2026-11-13", "digital": False},
    ]

    _, buscar_todas_colecoes, _ = _carregar_funcoes(_requisicao_falsa_para(dados_colecoes))
    registros = buscar_todas_colecoes()

    assert len(registros) == 2
    assert registros[0]["code"] == "lea"
    assert registros[0]["type"] == "core"
    assert registros[0]["releaseDate"] == "1993-08-05"
    assert registros[0]["onlineOnly"] is False


def test_buscar_todas_colecoes_sem_paginacao():
    dados_colecoes = [{"code": f"s{i}", "name": f"Set {i}", "set_type": "expansion",
                  "released_at": "2020-01-01", "digital": False} for i in range(1049)]

    _, buscar_todas_colecoes, _ = _carregar_funcoes(_requisicao_falsa_para(dados_colecoes))
    registros = buscar_todas_colecoes()

    assert len(registros) == 1049


def test_buscar_todas_colecoes_mapeia_campos_nativos_da_scryfall():
    dados_colecoes = [{
        "code": "dmc", "name": "Duskmourn Commander", "set_type": "commander",
        "released_at": "2024-09-27", "digital": False,
        "card_count": 240, "parent_set_code": "dmu", "block": "Commander",
        "icon_svg_uri": "https://svgs.scryfall.io/sets/dmc.svg?1234",
    }]

    _, buscar_todas_colecoes, limpar_dados_colecoes = _carregar_funcoes(_requisicao_falsa_para(dados_colecoes))
    registros = buscar_todas_colecoes()
    limpo = limpar_dados_colecoes(registros)[0]

    assert limpo["card_count"] == 240
    assert limpo["parent_set_code"] == "dmu"
    assert limpo["block"] == "Commander"
    assert limpo["icon_svg_uri"] == "https://svgs.scryfall.io/sets/dmc.svg?1234"


def test_campos_legados_viram_none_apos_limpar():
    # Campos legados sem equivalente na Scryfall ficam None.
    para_registro_colecao, _, limpar_dados_colecoes = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_colecao({"code": "lea", "name": "Alpha", "set_type": "core",
                             "released_at": "1993-08-05", "digital": False})

    limpo = limpar_dados_colecoes([registro])[0]
    assert limpo["border"] is None
    assert limpo["mkm_id"] is None
    assert limpo["gathererCode"] is None
    assert limpo["oldCode"] is None
    assert limpo["booster"] is None
    assert limpo["code"] == "lea"
    assert limpo["onlineOnly"] is False


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_todas_colecoes_mapeia_campos_em_um_request()
    test_buscar_todas_colecoes_sem_paginacao()
    test_buscar_todas_colecoes_mapeia_campos_nativos_da_scryfall()
    test_campos_legados_viram_none_apos_limpar()
    print("OK")
