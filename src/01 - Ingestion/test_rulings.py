# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (índice de bulk-data + jsonl gzipado).

import gzip
import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "rulings.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE RULINGS"


def _carregar_funcoes(requisicao_falsa):
    celulas = open(_CAMINHO_NOTEBOOK, encoding="utf-8").read().split("# COMMAND ----------")
    codigo_celula = next(c for c in celulas if _MARCADOR in c)

    escopo = {
        "obter_http_com_retentativa": lambda url, cabecalhos=None, tempo_limite=30, tentativas=3: requisicao_falsa(url, cabecalhos=cabecalhos, tempo_limite=tempo_limite),
        "gzip": gzip,
        "json": json,
        "StructType": lambda campos: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "URL_API_SCRYFALL": "https://api.scryfall.test",
        "CABECALHOS_SCRYFALL": {},
        "TIPO_BULK_SCRYFALL": "rulings",
        "MAX_TENTATIVAS": 3,
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_esclarecimento"], escopo["buscar_registros_esclarecimentos"]


class _Resp:
    def __init__(self, dados_json=None, conteudo=None):
        self._dados_json = dados_json
        self.content = conteudo

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        pass


def _requisicao_falsa_para(esclarecimentos):
    def requisicao_falsa(url, cabecalhos=None, tempo_limite=None):
        if url.endswith("/bulk-data"):
            return _Resp(dados_json={"data": [
                {"type": "rulings", "jsonl_download_uri": "https://data.test/rulings.jsonl.gz"}
            ]})
        corpo = "\n".join(json.dumps(r) for r in esclarecimentos).encode("utf-8")
        return _Resp(conteudo=gzip.compress(corpo))
    return requisicao_falsa


def test_buscar_registros_esclarecimentos_mapeia_campos():
    esclarecimentos = [{
        "oracle_id": "00037840-6089-42ec-8c5c-281f9f474504",
        "source": "wotc",
        "published_at": "2025-02-07",
        "comment": "Energy counters are a kind of counter that a player may have.",
    }]

    _, buscar_registros_esclarecimentos = _carregar_funcoes(_requisicao_falsa_para(esclarecimentos))
    registros = buscar_registros_esclarecimentos()

    assert len(registros) == 1
    assert registros[0]["oracle_id"] == "00037840-6089-42ec-8c5c-281f9f474504"
    assert registros[0]["source"] == "wotc"
    assert registros[0]["published_at"] == "2025-02-07"
    assert registros[0]["comment"] == "Energy counters are a kind of counter that a player may have."


def test_buscar_registros_esclarecimentos_devolve_uma_linha_por_ruling():
    # Grao: 1 linha por ruling, nao por carta.
    esclarecimentos = [
        {"oracle_id": "abc", "source": "wotc", "published_at": "2020-01-01", "comment": "A"},
        {"oracle_id": "abc", "source": "wotc", "published_at": "2020-02-01", "comment": "B"},
        {"oracle_id": "def", "source": "scryfall", "published_at": "2021-01-01", "comment": "C"},
    ]

    _, buscar_registros_esclarecimentos = _carregar_funcoes(_requisicao_falsa_para(esclarecimentos))
    registros = buscar_registros_esclarecimentos()

    assert [r["comment"] for r in registros] == ["A", "B", "C"]
    assert sum(1 for r in registros if r["oracle_id"] == "abc") == 2


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_registros_esclarecimentos_mapeia_campos()
    test_buscar_registros_esclarecimentos_devolve_uma_linha_por_ruling()
    print("OK")
