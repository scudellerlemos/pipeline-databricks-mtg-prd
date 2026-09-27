# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (resposta única do Scryfall /sets).

import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "sets.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE SETS"


def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "json": json,
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "IntegerType": lambda: None,
        "BooleanType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "MAX_RETRIES": 3,
        "setup_s3_storage": lambda *a, **k: True,
        "S3_BASE_PATH": "s3://test-bucket/stage",
    }
    exec(cell_source, ns)
    return ns["_to_set_record"], ns["fetch_all_sets"], ns["clean_sets_data"]


class _Resp:
    def __init__(self, json_data):
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _fake_get_for(sets_data):
    def fake_get(url, headers=None, timeout=None):
        assert url.endswith("/sets")
        return _Resp({"object": "list", "has_more": False, "data": sets_data})
    return fake_get


def test_fetch_all_sets_maps_fields_in_single_request():
    sets_data = [
        {"code": "lea", "name": "Limited Edition Alpha", "set_type": "core",
         "released_at": "1993-08-05", "digital": False},
        {"code": "trc", "name": "Star Trek Commander", "set_type": "commander",
         "released_at": "2026-11-13", "digital": False},
    ]

    _, fetch_all_sets, _ = _load_functions(_fake_get_for(sets_data))
    records = fetch_all_sets()

    assert len(records) == 2
    assert records[0]["code"] == "lea"
    assert records[0]["type"] == "core"
    assert records[0]["releaseDate"] == "1993-08-05"
    assert records[0]["onlineOnly"] is False


def test_fetch_all_sets_no_pagination_needed():
    sets_data = [{"code": f"s{i}", "name": f"Set {i}", "set_type": "expansion",
                  "released_at": "2020-01-01", "digital": False} for i in range(1049)]

    _, fetch_all_sets, _ = _load_functions(_fake_get_for(sets_data))
    records = fetch_all_sets()

    assert len(records) == 1049


def test_fetch_all_sets_maps_new_scryfall_only_fields():
    sets_data = [{
        "code": "dmc", "name": "Duskmourn Commander", "set_type": "commander",
        "released_at": "2024-09-27", "digital": False,
        "card_count": 240, "parent_set_code": "dmu", "block": "Commander",
        "icon_svg_uri": "https://svgs.scryfall.io/sets/dmc.svg?1234",
    }]

    _, fetch_all_sets, clean_sets_data = _load_functions(_fake_get_for(sets_data))
    records = fetch_all_sets()
    cleaned = clean_sets_data(records)[0]

    assert cleaned["card_count"] == 240
    assert cleaned["parent_set_code"] == "dmu"
    assert cleaned["block"] == "Commander"
    assert cleaned["icon_svg_uri"] == "https://svgs.scryfall.io/sets/dmc.svg?1234"


def test_magicthegathering_only_fields_become_none_after_clean():
    # Campos legados sem equivalente na Scryfall ficam None.
    to_set_record, _, clean_sets_data = _load_functions(_fake_get_for([]))
    record = to_set_record({"code": "lea", "name": "Alpha", "set_type": "core",
                             "released_at": "1993-08-05", "digital": False})

    cleaned = clean_sets_data([record])[0]
    assert cleaned["border"] is None
    assert cleaned["mkm_id"] is None
    assert cleaned["gathererCode"] is None
    assert cleaned["oldCode"] is None
    assert cleaned["booster"] is None
    assert cleaned["code"] == "lea"
    assert cleaned["onlineOnly"] is False


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_all_sets_maps_fields_in_single_request()
    test_fetch_all_sets_no_pagination_needed()
    test_fetch_all_sets_maps_new_scryfall_only_fields()
    test_magicthegathering_only_fields_become_none_after_clean()
    print("OK")
