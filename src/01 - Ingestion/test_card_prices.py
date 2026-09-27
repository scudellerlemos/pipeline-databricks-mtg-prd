# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (índice de bulk-data + jsonl gzipado).

import gzip
import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "card_prices.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE CARD_PRICES"


def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "gzip": gzip,
        "json": json,
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "SCRYFALL_BULK_TYPE": "default_cards",
        "MAX_RETRIES": 3,
    }
    exec(cell_source, ns)
    return ns["_to_price_record"], ns["fetch_price_records"]


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


def test_fetch_price_records_maps_fields():
    cards = [{
        "id": "aaaa-1111",
        "name": "Nissa, Worldsoul Speaker", "set": "drc", "rarity": "rare",
        "released_at": "2025-01-31",
        "prices": {"usd": "0.25", "usd_foil": "1.90", "usd_etched": None,
                   "eur": "0.21", "eur_foil": "1.55", "tix": "1.04"},
        "scryfall_uri": "https://scryfall.com/x",
        "image_uris": {"normal": "https://img/x.jpg"},
    }]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert len(records) == 1
    assert records[0]["id"] == "aaaa-1111"
    assert records[0]["name"] == "Nissa, Worldsoul Speaker"
    assert records[0]["set"] == "drc"
    assert records[0]["usd"] == "0.25"
    assert records[0]["eur"] == "0.21"
    assert records[0]["tix"] == "1.04"
    assert records[0]["usd_foil"] == "1.90"
    assert records[0]["eur_foil"] == "1.55"
    assert records[0]["usd_etched"] is None
    assert records[0]["scryfall_uri"] == "https://scryfall.com/x"
    assert records[0]["image_url"] == "https://img/x.jpg"
    assert records[0]["releaseDate"] == "2025-01-31"


def test_double_faced_card_keeps_combined_name_as_is():
    # Nome combinado "A // B" é gravado como veio; join com cards fica na Gold.
    cards = [{
        "name": "Brightglass Gearhulk // Brightglass Gearhulk", "set": "eoe", "rarity": "mythic",
        "released_at": "2025-07-25",
        "prices": {"usd": "3.50", "eur": None, "tix": None},
        "scryfall_uri": "https://scryfall.com/y",
        "card_faces": [{"image_uris": {"normal": "https://img/frente.jpg"}}, {}],
    }]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert records[0]["name"] == "Brightglass Gearhulk // Brightglass Gearhulk"
    assert records[0]["usd"] == "3.50"
    # DFC não tem image_uris na raiz: cai pra frente (card_faces[0])
    assert records[0]["image_url"] == "https://img/frente.jpg"


def test_reimpressoes_do_mesmo_nome_viram_linhas_com_precos_proprios():
    # O preco varia por impressao, entao cada impressao vira uma linha.
    cards = [
        {"id": "bolt-lea", "name": "Lightning Bolt", "set": "lea",
         "released_at": "1993-08-05", "prices": {"usd": "412.00"}},
        {"id": "bolt-sos", "name": "Lightning Bolt", "set": "sos",
         "released_at": "2026-04-24", "prices": {"usd": "1.35"}},
    ]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert [r["id"] for r in records] == ["bolt-lea", "bolt-sos"]
    assert [r["usd"] for r in records] == ["412.00", "1.35"]
    # mesmo nome nas duas - por isso a chave de join e o id, nao o nome
    assert len({r["name"] for r in records}) == 1


def test_variantes_foil_sao_capturadas_separadamente():
    # Foil e outra cotacao da mesma impressao, com valor bem diferente do nao-foil.
    cards = [{"id": "bolt-msc", "name": "Lightning Bolt", "set": "msc",
              "released_at": "2026-06-26",
              "prices": {"usd": "0.74", "usd_foil": "3.73", "tix": "0.02"}}]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    r = fetch_price_records()[0]

    assert r["usd"] == "0.74"
    assert r["usd_foil"] == "3.73"
    # ausente na fonte continua None, nunca 0 - "sem cotacao" != "vale zero"
    assert r["usd_etched"] is None
    assert r["eur_foil"] is None


def test_fetch_price_records_returns_one_row_per_catalog_entry():
    cards = [
        {"name": "A", "released_at": "2020-01-01", "prices": {}},
        {"name": "B", "released_at": "2021-01-01", "prices": {}},
        {"name": "C", "released_at": "2022-01-01", "prices": {}},
    ]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert [r["name"] for r in records] == ["A", "B", "C"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_price_records_maps_fields()
    test_double_faced_card_keeps_combined_name_as_is()
    test_reimpressoes_do_mesmo_nome_viram_linhas_com_precos_proprios()
    test_variantes_foil_sao_capturadas_separadamente()
    test_fetch_price_records_returns_one_row_per_catalog_entry()
    print("OK")
