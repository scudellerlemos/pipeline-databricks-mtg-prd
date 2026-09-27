# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (índice de bulk-data + jsonl gzipado).

import gzip
import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "card_prices.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE CARD_PRICES"


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
        "TIPO_BULK_SCRYFALL": "default_cards",
        "MAX_TENTATIVAS": 3,
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_preco"], escopo["buscar_registros_precos"]


class _Resp:
    def __init__(self, dados_json=None, conteudo=None):
        self._dados_json = dados_json
        self.content = conteudo

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        pass


def _requisicao_falsa_para(cartas):
    def requisicao_falsa(url, cabecalhos=None, tempo_limite=None):
        if url.endswith("/bulk-data"):
            return _Resp(dados_json={"data": [
                {"type": "default_cards", "jsonl_download_uri": "https://data.test/default.jsonl.gz"}
            ]})
        corpo = "\n".join(json.dumps(c) for c in cartas).encode("utf-8")
        return _Resp(conteudo=gzip.compress(corpo))
    return requisicao_falsa


def test_buscar_registros_precos_mapeia_campos():
    cartas = [{
        "id": "aaaa-1111",
        "name": "Nissa, Worldsoul Speaker", "set": "drc", "rarity": "rare",
        "released_at": "2025-01-31",
        "prices": {"usd": "0.25", "usd_foil": "1.90", "usd_etched": None,
                   "eur": "0.21", "eur_foil": "1.55", "tix": "1.04"},
        "scryfall_uri": "https://scryfall.com/x",
        "image_uris": {"normal": "https://img/x.jpg"},
    }]

    _, buscar_registros_precos = _carregar_funcoes(_requisicao_falsa_para(cartas))
    registros = buscar_registros_precos()

    assert len(registros) == 1
    assert registros[0]["id"] == "aaaa-1111"
    assert registros[0]["name"] == "Nissa, Worldsoul Speaker"
    assert registros[0]["set"] == "drc"
    assert registros[0]["usd"] == "0.25"
    assert registros[0]["eur"] == "0.21"
    assert registros[0]["tix"] == "1.04"
    assert registros[0]["usd_foil"] == "1.90"
    assert registros[0]["eur_foil"] == "1.55"
    assert registros[0]["usd_etched"] is None
    assert registros[0]["scryfall_uri"] == "https://scryfall.com/x"
    assert registros[0]["image_url"] == "https://img/x.jpg"
    assert registros[0]["releaseDate"] == "2025-01-31"


def test_carta_dupla_face_mantem_o_nome_combinado():
    # Nome combinado "A // B" é gravado como veio; join com cards fica na Gold.
    cartas = [{
        "name": "Brightglass Gearhulk // Brightglass Gearhulk", "set": "eoe", "rarity": "mythic",
        "released_at": "2025-07-25",
        "prices": {"usd": "3.50", "eur": None, "tix": None},
        "scryfall_uri": "https://scryfall.com/y",
        "card_faces": [{"image_uris": {"normal": "https://img/frente.jpg"}}, {}],
    }]

    _, buscar_registros_precos = _carregar_funcoes(_requisicao_falsa_para(cartas))
    registros = buscar_registros_precos()

    assert registros[0]["name"] == "Brightglass Gearhulk // Brightglass Gearhulk"
    assert registros[0]["usd"] == "3.50"
    # DFC não tem image_uris na raiz: cai pra frente (card_faces[0])
    assert registros[0]["image_url"] == "https://img/frente.jpg"


def test_reimpressoes_do_mesmo_nome_viram_linhas_com_precos_proprios():
    # O preco varia por impressao, entao cada impressao vira uma linha.
    cartas = [
        {"id": "bolt-lea", "name": "Lightning Bolt", "set": "lea",
         "released_at": "1993-08-05", "prices": {"usd": "412.00"}},
        {"id": "bolt-sos", "name": "Lightning Bolt", "set": "sos",
         "released_at": "2026-04-24", "prices": {"usd": "1.35"}},
    ]

    _, buscar_registros_precos = _carregar_funcoes(_requisicao_falsa_para(cartas))
    registros = buscar_registros_precos()

    assert [r["id"] for r in registros] == ["bolt-lea", "bolt-sos"]
    assert [r["usd"] for r in registros] == ["412.00", "1.35"]
    # mesmo nome nas duas - por isso a chave de join e o id, nao o nome
    assert len({r["name"] for r in registros}) == 1


def test_variantes_foil_sao_capturadas_separadamente():
    # Foil e outra cotacao da mesma impressao, com valor bem diferente do nao-foil.
    cartas = [{"id": "bolt-msc", "name": "Lightning Bolt", "set": "msc",
              "released_at": "2026-06-26",
              "prices": {"usd": "0.74", "usd_foil": "3.73", "tix": "0.02"}}]

    _, buscar_registros_precos = _carregar_funcoes(_requisicao_falsa_para(cartas))
    r = buscar_registros_precos()[0]

    assert r["usd"] == "0.74"
    assert r["usd_foil"] == "3.73"
    # ausente na fonte continua None, nunca 0 - "sem cotacao" != "vale zero"
    assert r["usd_etched"] is None
    assert r["eur_foil"] is None


def test_buscar_registros_precos_devolve_uma_linha_por_entrada_do_catalogo():
    cartas = [
        {"name": "A", "released_at": "2020-01-01", "prices": {}},
        {"name": "B", "released_at": "2021-01-01", "prices": {}},
        {"name": "C", "released_at": "2022-01-01", "prices": {}},
    ]

    _, buscar_registros_precos = _carregar_funcoes(_requisicao_falsa_para(cartas))
    registros = buscar_registros_precos()

    assert [r["name"] for r in registros] == ["A", "B", "C"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_registros_precos_mapeia_campos()
    test_carta_dupla_face_mantem_o_nome_combinado()
    test_reimpressoes_do_mesmo_nome_viram_linhas_com_precos_proprios()
    test_variantes_foil_sao_capturadas_separadamente()
    test_buscar_registros_precos_devolve_uma_linha_por_entrada_do_catalogo()
    print("OK")
