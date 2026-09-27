# ponytail: mesma abordagem de test_card_prices.py - _to_ruling_record/
# fetch_ruling_records vivem dentro da célula "FUNÇÕES ESPECÍFICAS" do
# notebook (não é um módulo importável por si só), então isto carrega o
# código-fonte da célula direto do notebook .py e executa com um `requests`
# fake (índice de bulk-data + payload jsonl gzipado).

import gzip
import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "rulings.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE RULINGS"


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
        "SCRYFALL_BULK_TYPE": "rulings",
        "MAX_RETRIES": 3,
    }
    exec(cell_source, ns)
    return ns["_to_ruling_record"], ns["fetch_ruling_records"]


class _Resp:
    def __init__(self, json_data=None, content=None):
        self._json = json_data
        self.content = content

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _fake_get_for(rulings):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/bulk-data"):
            return _Resp(json_data={"data": [
                {"type": "rulings", "jsonl_download_uri": "https://data.test/rulings.jsonl.gz"}
            ]})
        body = "\n".join(json.dumps(r) for r in rulings).encode("utf-8")
        return _Resp(content=gzip.compress(body))
    return fake_get


def test_fetch_ruling_records_maps_fields():
    rulings = [{
        "oracle_id": "00037840-6089-42ec-8c5c-281f9f474504",
        "source": "wotc",
        "published_at": "2025-02-07",
        "comment": "Energy counters are a kind of counter that a player may have.",
    }]

    _, fetch_ruling_records = _load_functions(_fake_get_for(rulings))
    records = fetch_ruling_records()

    assert len(records) == 1
    assert records[0]["oracle_id"] == "00037840-6089-42ec-8c5c-281f9f474504"
    assert records[0]["source"] == "wotc"
    assert records[0]["published_at"] == "2025-02-07"
    assert records[0]["comment"] == "Energy counters are a kind of counter that a player may have."


def test_fetch_ruling_records_returns_one_row_per_ruling():
    # 1 oracle_id pode ter varias rulings - grao e 1 linha por ruling, nao 1
    # por carta (join fica pra Bronze/Silver).
    rulings = [
        {"oracle_id": "abc", "source": "wotc", "published_at": "2020-01-01", "comment": "A"},
        {"oracle_id": "abc", "source": "wotc", "published_at": "2020-02-01", "comment": "B"},
        {"oracle_id": "def", "source": "scryfall", "published_at": "2021-01-01", "comment": "C"},
    ]

    _, fetch_ruling_records = _load_functions(_fake_get_for(rulings))
    records = fetch_ruling_records()

    assert [r["comment"] for r in records] == ["A", "B", "C"]
    assert sum(1 for r in records if r["oracle_id"] == "abc") == 2


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_ruling_records_maps_fields()
    test_fetch_ruling_records_returns_one_row_per_ruling()
    print("OK")
