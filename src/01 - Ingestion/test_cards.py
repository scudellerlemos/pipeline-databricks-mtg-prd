# ponytail: mesma abordagem de test_card_prices.py - a célula do notebook não
# é um módulo importável por si só, então carrega o código-fonte da célula
# "FUNÇÕES ESPECÍFICAS" direto do notebook .py e executa com um `requests`
# fake (índice de bulk-data + payload jsonl gzipado, ou /sets).

import gzip
import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "cards.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE CARDS"


def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "json": json,
        # vem do %run ./ingestion_utils no notebook - comportamento real
        # coberto por test_ingestion_utils.test_as_float_converte_int_e_preserva_none
        "as_float": lambda v: float(v) if v is not None else None,
        "gzip": gzip,
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "IntegerType": lambda: None,
        "FloatType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "SCRYFALL_BULK_TYPE": "default_cards",
        "MAX_RETRIES": 3,
    }
    exec(cell_source, ns)
    return ns["_to_card_record"], ns["fetch_cards_by_sets"]


class _Resp:
    def __init__(self, json_data=None, content=None):
        self._json = json_data
        self.content = content

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _fake_get_for(cards):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/bulk-data"):
            return _Resp(json_data={"data": [
                {"type": "default_cards", "jsonl_download_uri": "https://data.test/default.jsonl.gz"}
            ]})
        body = "\n".join(json.dumps(c) for c in cards).encode("utf-8")
        return _Resp(content=gzip.compress(body))
    return fake_get


def test_fetch_cards_by_sets_filters_by_set_and_maps_fields():
    cards = [
        {
            "name": "Lightning Bolt", "mana_cost": "{R}", "cmc": 1.0,
            "colors": ["R"], "color_identity": ["R"], "type_line": "Instant",
            "rarity": "common", "set": "lea", "set_name": "Limited Edition Alpha",
            "oracle_text": "Deal 3 damage.", "artist": "Christopher Rush",
            "collector_number": "161", "power": None, "toughness": None,
            "layout": "normal", "image_uris": {"normal": "https://img/bolt.jpg"},
            "legalities": {"standard": "not_legal"}, "id": "abc-123",
        },
        {
            "name": "Some Other Card", "set": "not-in-window", "type_line": "Creature",
            "rarity": "common", "set_name": "Other", "collector_number": "1",
            "layout": "normal", "id": "zzz",
        },
    ]

    _, fetch_cards_by_sets = _load_functions(_fake_get_for(cards))
    records = fetch_cards_by_sets(["lea"])

    assert len(records) == 1
    assert records[0]["name"] == "Lightning Bolt"
    assert records[0]["manaCost"] == "{R}"
    assert records[0]["type"] == "Instant"
    assert records[0]["setName"] == "Limited Edition Alpha"
    assert records[0]["imageUrl"] == "https://img/bolt.jpg"


def test_double_faced_card_falls_back_to_front_face():
    dfc_card = {
        "name": "Delver of Secrets // Insectile Aberration",
        "cmc": 1.0, "color_identity": ["U"], "type_line": "Creature — Human Wizard // Creature — Human Insect",
        "rarity": "common", "set": "isd", "set_name": "Innistrad", "collector_number": "51",
        "layout": "transform", "legalities": {"standard": "not_legal"}, "id": "dfc-1",
        "card_faces": [
            {
                "name": "Delver of Secrets", "mana_cost": "{U}", "colors": ["U"],
                "oracle_text": "At the beginning of your upkeep...", "artist": "Nils Hamm",
                "power": "1", "toughness": "1", "image_uris": {"normal": "https://img/delver.jpg"},
            },
            {"name": "Insectile Aberration"},
        ],
    }

    to_card_record, _ = _load_functions(_fake_get_for([]))
    record = to_card_record(dfc_card)

    assert record["manaCost"] == "{U}"
    assert record["colors"] == json.dumps(["U"])
    assert record["power"] == "1"
    assert record["imageUrl"] == "https://img/delver.jpg"


def test_double_faced_card_empty_colors_not_treated_as_missing():
    # colors: [] no nível raiz (ex.: carta incolor) é um valor válido - não
    # deve cair no fallback pra card_faces (`is not None`, não `or`).
    card = {"name": "X", "colors": [], "card_faces": [{"colors": ["R"]}]}

    to_card_record, _ = _load_functions(_fake_get_for([]))
    record = to_card_record(card)

    assert record["colors"] == json.dumps([])


def test_legalities_dict_is_serialized_as_valid_json():
    # legalities na Scryfall é um dict (não list) - _to_card_record precisa
    # serializar via json.dumps pra gravar JSON válido, não um repr Python.
    card = {"name": "X", "legalities": {"standard": "legal", "modern": "legal"}}

    to_card_record, _ = _load_functions(_fake_get_for([]))
    record = to_card_record(card)

    assert json.loads(record["legalities"]) == {"standard": "legal", "modern": "legal"}


def test_missing_scryfall_only_fields_are_none():
    # foreignNames/printings/originalText/originalType/types/subtypes/
    # multiverseid/variations não têm equivalente na Scryfall.
    to_card_record, _ = _load_functions(_fake_get_for([]))
    card = {"name": "X", "set": "lea", "rarity": "common", "id": "1"}
    record = to_card_record(card)

    assert record["foreignNames"] is None
    assert record["printings"] is None
    assert record["multiverseid"] is None
    assert record["types"] is None


# fetch_valid_set_codes ficou em ingestion_utils.py como get_scryfall_set_codes_since
# (usa http_get_with_retry) - coberto por
# test_get_scryfall_set_codes_since_filters_by_date_and_lowercases em test_ingestion_utils.py.


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_cards_by_sets_filters_by_set_and_maps_fields()
    test_double_faced_card_falls_back_to_front_face()
    test_double_faced_card_empty_colors_not_treated_as_missing()
    test_legalities_dict_is_serialized_as_valid_json()
    test_missing_scryfall_only_fields_are_none()
    print("OK")
