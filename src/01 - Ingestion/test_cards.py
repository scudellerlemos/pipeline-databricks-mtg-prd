# O notebook não é importável: carrega o código da célula "FUNÇÕES ESPECÍFICAS"
# e executa com um `requests` fake (índice de bulk-data + jsonl gzipado).

import gzip
import json
import os
import sys

_CAMINHO_NOTEBOOK = os.path.join(os.path.dirname(__file__), "cards.py")
_MARCADOR = "FUNÇÕES ESPECÍFICAS DE CARDS"


def _carregar_funcoes(requisicao_falsa):
    celulas = open(_CAMINHO_NOTEBOOK, encoding="utf-8").read().split("# COMMAND ----------")
    codigo_celula = next(c for c in celulas if _MARCADOR in c)

    escopo = {
        "json": json,
        # no notebook vem do %run ./ingestion_utils
        "como_float": lambda v: float(v) if v is not None else None,
        "gzip": gzip,
        "obter_http_com_retentativa": lambda url, cabecalhos=None, tempo_limite=30, tentativas=3: requisicao_falsa(url, cabecalhos=cabecalhos, tempo_limite=tempo_limite),
        "StructType": lambda campos: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "IntegerType": lambda: None,
        "FloatType": lambda: None,
        "URL_API_SCRYFALL": "https://api.scryfall.test",
        "CABECALHOS_SCRYFALL": {},
        "TIPO_BULK_SCRYFALL": "default_cards",
        "MAX_TENTATIVAS": 3,
    }
    exec(codigo_celula, escopo)
    return escopo["_para_registro_carta"], escopo["buscar_cartas_por_colecoes"]


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


def test_buscar_cartas_por_colecoes_filtra_por_colecao_e_mapeia_campos():
    cartas = [
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

    _, buscar_cartas_por_colecoes = _carregar_funcoes(_requisicao_falsa_para(cartas))
    registros = buscar_cartas_por_colecoes(["lea"])

    assert len(registros) == 1
    assert registros[0]["name"] == "Lightning Bolt"
    assert registros[0]["manaCost"] == "{R}"
    assert registros[0]["type"] == "Instant"
    assert registros[0]["setName"] == "Limited Edition Alpha"
    assert registros[0]["imageUrl"] == "https://img/bolt.jpg"


def test_carta_dupla_face_usa_a_face_da_frente():
    carta_dfc = {
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

    para_registro_carta, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_carta(carta_dfc)

    assert registro["manaCost"] == "{U}"
    assert registro["colors"] == json.dumps(["U"])
    assert registro["power"] == "1"
    assert registro["imageUrl"] == "https://img/delver.jpg"
    # type_line existe na raiz aqui - não é trocado pela frente
    assert registro["type"] == "Creature — Human Wizard // Creature — Human Insect"


def test_reversible_card_sem_type_line_e_oracle_id_na_raiz_usa_a_frente():
    # type ou oracle_id nulo quebra o DQ da Gold (NME_TIPO_CARTA, ID_ORACLE).
    face = {"type_line": "Legendary Creature — Elf", "oracle_id": "orc-1"}
    carta = {"name": "X // X", "layout": "reversible_card", "id": "rev-1", "card_faces": [face, dict(face)]}

    para_registro_carta, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_carta(carta)
    assert registro["type"] == "Legendary Creature — Elf"
    assert registro["oracle_id"] == "orc-1"


def test_carta_dupla_face_cores_vazias_nao_contam_como_ausentes():
    # colors: [] na raiz (carta incolor) é válido - não cai no fallback pra card_faces.
    carta = {"name": "X", "colors": [], "card_faces": [{"colors": ["R"]}]}

    para_registro_carta, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_carta(carta)

    assert registro["colors"] == json.dumps([])


def test_legalidades_viram_json_valido():
    # legalities é dict: tem que sair como JSON válido, não repr Python.
    carta = {"name": "X", "legalities": {"standard": "legal", "modern": "legal"}}

    para_registro_carta, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    registro = para_registro_carta(carta)

    assert json.loads(registro["legalities"]) == {"standard": "legal", "modern": "legal"}


def test_campos_legados_sem_equivalente_ficam_none():
    # foreignNames/printings/originalText/originalType/types/subtypes/
    # multiverseid/variations ficam nulos (legado; multiverse_ids da Scryfall não é mapeado).
    para_registro_carta, _ = _carregar_funcoes(_requisicao_falsa_para([]))
    carta = {"name": "X", "set": "lea", "rarity": "common", "id": "1"}
    registro = para_registro_carta(carta)

    assert registro["foreignNames"] is None
    assert registro["printings"] is None
    assert registro["multiverseid"] is None
    assert registro["types"] is None


# obter_codigos_colecoes_scryfall_desde é testado em test_ingestion_utils.py.


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_buscar_cartas_por_colecoes_filtra_por_colecao_e_mapeia_campos()
    test_carta_dupla_face_usa_a_face_da_frente()
    test_reversible_card_sem_type_line_na_raiz_usa_a_frente()
    test_carta_dupla_face_cores_vazias_nao_contam_como_ausentes()
    test_legalidades_viram_json_valido()
    test_campos_legados_sem_equivalente_ficam_none()
    print("OK")
