# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (respostas paginadas de /migrations).

import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "migrations.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE MIGRATIONS"


def _carregar_funcoes(requisicao_falsa):
    celulas = open(_CAMINHO_NOTEBOOK, encoding="utf-8").read().split("# COMMAND ----------")
    codigo_celula = next(c for c in celulas if _MARCADOR in c)

    escopo = {
        "obter_http_com_retentativa": lambda url, cabecalhos=None, tempo_limite=30, tentativas=3: requisicao_falsa(url, cabecalhos=cabecalhos, tempo_limite=tempo_limite),
        "StructType": lambda campos: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "URL_API_SCRYFALL": "https://api.scryfall.test",
        "CABECALHOS_SCRYFALL": {},
        "MAX_TENTATIVAS": 3,
        "configurar_armazenamento_s3": lambda *a, **k: True,
        "CAMINHO_S3_STAGE": "s3://test-bucket/stage",
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_migracao"], escopo["buscar_todas_migracoes"]


class _Resp:
    def __init__(self, dados_json):
        self._dados_json = dados_json

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        pass


def _requisicao_falsa_para(paginas):
    # paginas: lista de listas de migration-dicts, 1 lista por página
    chamadas = {"n": 0}

    def requisicao_falsa(url, cabecalhos=None, tempo_limite=None):
        i = chamadas["n"]
        chamadas["n"] += 1
        tem_mais = i < len(paginas) - 1
        return _Resp({
            "object": "list",
            "has_more": tem_mais,
            "next_page": f"https://api.scryfall.test/migrations?page={i + 2}" if tem_mais else None,
            "data": paginas[i],
        })
    return requisicao_falsa


def test_buscar_todas_migracoes_mapeia_campos_de_delete_e_merge():
    dados_migracoes = [
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

    _, buscar_todas_migracoes = _carregar_funcoes(_requisicao_falsa_para([dados_migracoes]))
    registros = buscar_todas_migracoes()

    assert len(registros) == 2

    registro_remocao = registros[0]
    assert registro_remocao["migration_strategy"] == "delete"
    assert registro_remocao["old_scryfall_id"] == "466a7198-d022-47af-a7c5-351a54eb71de"
    assert registro_remocao["new_scryfall_id"] is None  # delete não tem new_scryfall_id
    assert registro_remocao["metadata_name"] == "Xyris, the Writhing Storm"
    assert registro_remocao["metadata_oracle_id"] == "8687948c-456b-495b-9b31-f818c65624b6"

    registro_fusao = registros[1]
    assert registro_fusao["migration_strategy"] == "merge"
    assert registro_fusao["new_scryfall_id"] == "d2b59872-0e11-41c3-9858-3e2dd5a1c3c3"
    assert registro_fusao["metadata_set_code"] == "hob"


def test_buscar_todas_migracoes_segue_a_paginacao():
    def _migracao_falsa(i):
        return {
            "id": f"id-{i}", "uri": f"https://x/{i}", "performed_at": "2026-01-01",
            "migration_strategy": "delete", "old_scryfall_id": f"old-{i}", "note": None,
            "metadata": {"id": f"old-{i}", "lang": "en", "name": f"Card {i}",
                         "set_code": "abc", "oracle_id": f"oracle-{i}", "collector_number": str(i)},
        }

    paginas = [[_migracao_falsa(i) for i in range(3)], [_migracao_falsa(i) for i in range(3, 5)]]

    _, buscar_todas_migracoes = _carregar_funcoes(_requisicao_falsa_para(paginas))
    registros = buscar_todas_migracoes()

    assert len(registros) == 5
    assert [r["id"] for r in registros] == ["id-0", "id-1", "id-2", "id-3", "id-4"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_todas_migracoes_mapeia_campos_de_delete_e_merge()
    test_buscar_todas_migracoes_segue_a_paginacao()
    print("OK")
