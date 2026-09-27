# ponytail: mesma abordagem de test_sets.py/test_symbology.py - a célula
# "FUNÇÕES ESPECÍFICAS" do notebook não é um módulo importável por si só,
# então carrega seu código-fonte direto do notebook .py e executa com um
# `requests` fake (respostas paginadas de /migrations).

import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "migrations.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE MIGRATIONS"


def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "MAX_RETRIES": 3,
        "setup_s3_storage": lambda *a, **k: True,
        "S3_BASE_PATH": "s3://test-bucket/stage",
    }
    exec(cell_source, ns)
    return ns["_to_migration_record"], ns["fetch_all_migrations"]


class _Resp:
    def __init__(self, json_data):
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _fake_get_for(pages):
    # pages: lista de listas de migration-dicts, 1 lista por página
    calls = {"n": 0}

    def fake_get(url, headers=None, timeout=None):
        i = calls["n"]
        calls["n"] += 1
        has_more = i < len(pages) - 1
        return _Resp({
            "object": "list",
            "has_more": has_more,
            "next_page": f"https://api.scryfall.test/migrations?page={i + 2}" if has_more else None,
            "data": pages[i],
        })
    return fake_get


def test_fetch_all_migrations_maps_delete_and_merge_fields():
    migrations_data = [
        {
            "id": "18045879-f920-4fc9-95e5-58cc58d6c6c1",
            "uri": "https://api.scryfall.com/migrations/18045879-f920-4fc9-95e5-58cc58d6c6c1",
            "performed_at": "2026-09-09",
            "migration_strategy": "delete",
            "old_scryfall_id": "466a7198-d022-47af-a7c5-351a54eb71de",
            "note": "Not actually printed",
            "metadata": {
                "id": "466a7198-d022-47af-a7c5-351a54eb71de",
                "lang": "en",
                "name": "Xyris, the Writhing Storm",
                "set_code": "plst",
                "oracle_id": "8687948c-456b-495b-9b31-f818c65624b6",
                "collector_number": "DMC-175",
            },
        },
        {
            "id": "4f1387c2-5398-40b3-8f95-e934958001b1",
            "uri": "https://api.scryfall.com/migrations/4f1387c2-5398-40b3-8f95-e934958001b1",
            "performed_at": "2026-07-23",
            "migration_strategy": "merge",
            "old_scryfall_id": "c927d327-ac57-4b6f-a2e9-2e407e26a4a7",
            "new_scryfall_id": "d2b59872-0e11-41c3-9858-3e2dd5a1c3c3",
            "note": "Duplicate oracle card entry",
            "metadata": {
                "id": "c927d327-ac57-4b6f-a2e9-2e407e26a4a7",
                "lang": "en",
                "name": "TEMP WILL MERGE",
                "set_code": "hob",
                "oracle_id": "0bb04342-7ec3-48ba-b23a-d2f8a37e3526",
                "collector_number": "1",
            },
        },
    ]

    _, fetch_all_migrations = _load_functions(_fake_get_for([migrations_data]))
    records = fetch_all_migrations()

    assert len(records) == 2

    delete_rec = records[0]
    assert delete_rec["migration_strategy"] == "delete"
    assert delete_rec["old_scryfall_id"] == "466a7198-d022-47af-a7c5-351a54eb71de"
    assert delete_rec["new_scryfall_id"] is None  # delete não tem new_scryfall_id
    assert delete_rec["metadata_name"] == "Xyris, the Writhing Storm"
    assert delete_rec["metadata_oracle_id"] == "8687948c-456b-495b-9b31-f818c65624b6"

    merge_rec = records[1]
    assert merge_rec["migration_strategy"] == "merge"
    assert merge_rec["new_scryfall_id"] == "d2b59872-0e11-41c3-9858-3e2dd5a1c3c3"
    assert merge_rec["metadata_set_code"] == "hob"


def test_fetch_all_migrations_follows_pagination():
    # /migrations é o único endpoint da Stage que pagina de verdade
    # (has_more/next_page) - confirma que o loop segue até has_more=false.
    def _fake_migration(i):
        return {
            "id": f"id-{i}", "uri": f"https://x/{i}", "performed_at": "2026-01-01",
            "migration_strategy": "delete", "old_scryfall_id": f"old-{i}", "note": None,
            "metadata": {"id": f"old-{i}", "lang": "en", "name": f"Card {i}",
                         "set_code": "abc", "oracle_id": f"oracle-{i}", "collector_number": str(i)},
        }

    pages = [[_fake_migration(i) for i in range(3)], [_fake_migration(i) for i in range(3, 5)]]

    _, fetch_all_migrations = _load_functions(_fake_get_for(pages))
    records = fetch_all_migrations()

    assert len(records) == 5
    assert [r["id"] for r in records] == ["id-0", "id-1", "id-2", "id-3", "id-4"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_all_migrations_maps_delete_and_merge_fields()
    test_fetch_all_migrations_follows_pagination()
    print("OK")
