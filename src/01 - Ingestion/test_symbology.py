# ponytail: mesma abordagem de test_sets.py - a célula "FUNÇÕES ESPECÍFICAS"
# do notebook não é um módulo importável por si só, então carrega seu
# código-fonte direto do notebook .py e executa com um `requests` fake
# (resposta única do Scryfall /symbology).

import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "symbology.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE SYMBOLOGY"

def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "json": json,
        # vem do %run ./ingestion_utils no notebook - comportamento real
        # coberto por test_ingestion_utils.test_as_float_converte_int_e_preserva_none
        "as_float": lambda v: float(v) if v is not None else None,
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "BooleanType": lambda: None,
        "DoubleType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "MAX_RETRIES": 3,
        "setup_s3_storage": lambda *a, **k: True,
        "S3_BASE_PATH": "s3://test-bucket/stage",
    }
    exec(cell_source, ns)
    return ns["_to_symbol_record"], ns["fetch_all_symbols"]

class _Resp:
    def __init__(self, json_data):
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        pass

def _fake_get_for(symbols_data):
    def fake_get(url, headers=None, timeout=None):
        assert url.endswith("/symbology")
        return _Resp({"object": "list", "has_more": False, "data": symbols_data})
    return fake_get

def test_fetch_all_symbols_maps_fields_in_single_request():
    symbols_data = [
        {"symbol": "{T}", "svg_uri": "https://svgs.scryfall.io/card-symbols/T.svg",
         "loose_variant": None, "english": "tap this permanent", "transposable": False,
         "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
         "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
         "colors": [], "gatherer_alternates": ["ocT", "oT"]},
    ]

    _, fetch_all_symbols = _load_functions(_fake_get_for(symbols_data))
    records = fetch_all_symbols()

    assert len(records) == 1
    assert records[0]["symbol"] == "{T}"
    assert records[0]["english"] == "tap this permanent"
    assert records[0]["mana_value"] == 0.0
    assert records[0]["colors"] == "[]"
    assert records[0]["gatherer_alternates"] == json.dumps(["ocT", "oT"])

def test_null_list_fields_stay_none():
    # gatherer_alternates vem null pra muitos símbolos (não tem alternativa no
    # Gatherer) - json.dumps(None) viraria a string "null", então o mapeamento
    # preserva None de verdade em vez de serializar o null.
    symbols_data = [
        {"symbol": "{CHAOS}", "svg_uri": "https://svgs.scryfall.io/card-symbols/CHAOS.svg",
         "loose_variant": None, "english": "chaos", "transposable": False,
         "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
         "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
         "colors": [], "gatherer_alternates": None},
    ]

    to_symbol_record, _ = _load_functions(_fake_get_for([]))
    record = to_symbol_record(symbols_data[0])

    assert record["gatherer_alternates"] is None
    assert record["colors"] == "[]"

def test_fetch_all_symbols_no_pagination_needed():
    # mesmo padrão de sets.ipynb: /symbology devolve o catálogo inteiro em 1
    # request só (has_more: false) - sem loop de paginação necessário.
    symbols_data = [{"symbol": f"{{S{i}}}", "svg_uri": f"https://x/{i}.svg",
                      "loose_variant": None, "english": f"symbol {i}", "transposable": False,
                      "represents_mana": False, "appears_in_mana_costs": False, "mana_value": 0.0,
                      "hybrid": False, "phyrexian": False, "cmc": 0.0, "funny": False,
                      "colors": [], "gatherer_alternates": None} for i in range(84)]

    _, fetch_all_symbols = _load_functions(_fake_get_for(symbols_data))
    records = fetch_all_symbols()

    assert len(records) == 84

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_all_symbols_maps_fields_in_single_request()
    test_null_list_fields_stay_none()
    test_fetch_all_symbols_no_pagination_needed()
    print("OK")
