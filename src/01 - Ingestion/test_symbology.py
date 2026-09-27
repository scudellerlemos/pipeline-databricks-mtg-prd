# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (resposta única do Scryfall /symbology).

import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "symbology.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE SYMBOLOGY"

def _carregar_funcoes(requisicao_falsa):
    celulas = open(_CAMINHO_NOTEBOOK, encoding="utf-8").read().split("# COMMAND ----------")
    codigo_celula = next(c for c in celulas if _MARCADOR in c)

    escopo = {
        "json": json,
        # no notebook vem do %run ./ingestion_utils
        "como_float": lambda v: float(v) if v is not None else None,
        "obter_http_com_retentativa": lambda url, cabecalhos=None, tempo_limite=30, tentativas=3: requisicao_falsa(url, cabecalhos=cabecalhos, tempo_limite=tempo_limite),
        "StructType": lambda campos: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "BooleanType": lambda: None,
        "DoubleType": lambda: None,
        "URL_API_SCRYFALL": "https://api.scryfall.test",
        "CABECALHOS_SCRYFALL": {},
        "MAX_TENTATIVAS": 3,
        "configurar_armazenamento_s3": lambda *a, **k: True,
        "CAMINHO_S3_STAGE": "s3://test-bucket/stage",
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_simbolo"], escopo["buscar_todos_simbolos"]

class _Resp:
    def __init__(self, dados_json):
        self._dados_json = dados_json

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        pass

def _requisicao_falsa_para(dados_simbolos):
    def requisicao_falsa(url, cabecalhos=None, tempo_limite=None):
        assert url.endswith("/symbology")
        return _Resp({"object": "list", "has_more": False, "data": dados_simbolos})
    return requisicao_falsa

def test_buscar_todos_simbolos_mapeia_campos_em_um_request():
    dados_simbolos = [
        {"symbol": "{T}", "svg_uri": "https://svgs.scryfall.io/card-symbols/T.svg",
         "loose_variant": None, "english": "tap this permanent", "transposable": False,
         "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
         "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
         "colors": [], "gatherer_alternates": ["ocT", "oT"]},
    ]

    _, buscar_todos_simbolos = _carregar_funcoes(_requisicao_falsa_para(dados_simbolos))
    registros = buscar_todos_simbolos()

    assert len(registros) == 1
    assert registros[0]["symbol"] == "{T}"
    assert registros[0]["english"] == "tap this permanent"
    assert registros[0]["mana_value"] == 0.0
    assert registros[0]["colors"] == "[]"
    assert registros[0]["gatherer_alternates"] == json.dumps(["ocT", "oT"])

def test_listas_nulas_continuam_none():
    # null tem que continuar None, não virar a string "null" do json.dumps.
    dados_simbolos = [
        {"symbol": "{CHAOS}", "svg_uri": "https://svgs.scryfall.io/card-symbols/CHAOS.svg",
         "loose_variant": None, "english": "chaos", "transposable": False,
         "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
         "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
         "colors": [], "gatherer_alternates": None},
    ]

    para_registro_simbolo, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_simbolo(dados_simbolos[0])

    assert registro["gatherer_alternates"] is None
    assert registro["colors"] == "[]"

def test_buscar_todos_simbolos_sem_paginacao():
    dados_simbolos = [{"symbol": f"{{S{i}}}", "svg_uri": f"https://x/{i}.svg",
                      "loose_variant": None, "english": f"symbol {i}", "transposable": False,
                      "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
                      "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
                      "colors": [], "gatherer_alternates": None} for i in range(84)]

    _, buscar_todos_simbolos = _carregar_funcoes(_requisicao_falsa_para(dados_simbolos))
    registros = buscar_todos_simbolos()

    assert len(registros) == 84

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_todos_simbolos_mapeia_campos_em_um_request()
    test_listas_nulas_continuam_none()
    test_buscar_todos_simbolos_sem_paginacao()
    print("OK")
