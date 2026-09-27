# Importa ingestion_utils.py pelo caminho. Sem cluster não há dbutils, então a
# escrita do controle em finalizar_execucao falha em silêncio, a menos que um dbutils
# fake seja injetado no módulo.

import importlib.util
import json
import os
import sys
import types
from contextlib import contextmanager

# pyspark não está instalado no CI: stub mínimo só pro import de nível de
# módulo (salvar_em_parquet não é testado aqui).
if "pyspark" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_types = types.ModuleType("pyspark.sql.types")
    for nome in ("coalesce", "col", "lit", "current_timestamp", "year", "month", "when"):
        setattr(pyspark_sql_functions, nome, lambda *a, **k: None)
    for nome in ("StructType", "StructField", "StringType", "IntegerType", "FloatType"):
        setattr(pyspark_sql_types, nome, lambda *a, **k: None)
    pyspark.sql = pyspark_sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions
    sys.modules["pyspark.sql.types"] = pyspark_sql_types

_CAMINHO = os.path.join(os.path.dirname(__file__), "ingestion_utils.py")
_ESPEC = importlib.util.spec_from_file_location("ingestion_utils", _CAMINHO)
ingestion_utils = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(ingestion_utils)


class _Resp:
    def __init__(self, codigo_status=200, dados_json=None):
        self.status_code = codigo_status
        self._dados_json = dados_json

    def json(self):
        return self._dados_json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


@contextmanager
def _substituir_requests(requisicao_falsa):
    """Troca ingestion_utils.requests.get por requisicao_falsa e time.sleep por um no-op."""
    requisicao_original, espera_original = ingestion_utils.requests.get, ingestion_utils.time.sleep
    ingestion_utils.requests.get = requisicao_falsa
    ingestion_utils.time.sleep = lambda *_: None
    try:
        yield
    finally:
        ingestion_utils.requests.get = requisicao_original
        ingestion_utils.time.sleep = espera_original


def test_obter_http_com_retentativa_devolve_resposta_no_sucesso():
    with _substituir_requests(lambda *a, **k: _Resp(200, {"ok": True})):
        resposta = ingestion_utils.obter_http_com_retentativa("https://x.test")
    assert resposta.json() == {"ok": True}


def test_obter_http_com_retentativa_repete_no_5xx_e_depois_passa():
    chamadas = {"n": 0}

    def requisicao_falsa(*a, **k):
        chamadas["n"] += 1
        return _Resp(503) if chamadas["n"] < 2 else _Resp(200, {"ok": True})

    with _substituir_requests(requisicao_falsa):
        resposta = ingestion_utils.obter_http_com_retentativa("https://x.test", tentativas=3)

    assert resposta.json() == {"ok": True}
    assert chamadas["n"] == 2


def test_obter_http_com_retentativa_falha_na_hora_no_4xx():
    chamadas = {"n": 0}

    def requisicao_falsa(*a, **k):
        chamadas["n"] += 1
        return _Resp(404)

    try:
        with _substituir_requests(requisicao_falsa):
            ingestion_utils.obter_http_com_retentativa("https://x.test", tentativas=3)
    except Exception:
        pass
    else:
        raise AssertionError("expected exception for 404")
    assert chamadas["n"] == 1  # 4xx nao tenta de novo


def test_obter_http_com_retentativa_levanta_apos_esgotar_tentativas():
    try:
        with _substituir_requests(lambda *a, **k: _Resp(500)):
            ingestion_utils.obter_http_com_retentativa("https://x.test", tentativas=2)
    except Exception:
        pass
    else:
        raise AssertionError("expected exception after exhausting retries")


def test_obter_codigos_colecoes_filtra_por_data_e_poe_em_minusculas():
    dados_colecoes = {"data": [
        {"code": "LEA", "released_at": "1993-08-05"},
        {"code": "TRC", "released_at": "2026-11-13"},
        {"code": "old", "released_at": "1990-01-01"},
    ]}
    with _substituir_requests(lambda *a, **k: _Resp(200, dados_colecoes)):
        codigos = ingestion_utils.obter_codigos_colecoes_scryfall_desde("https://api.scryfall.test", {}, "2000-01-01")

    # lea (1993) e old (1990) ficam fora da janela (cutoff 2000-01-01); trc
    # (2026) entra e o code vem normalizado pra minúsculo.
    assert codigos == ["trc"]


def test_iniciar_execucao_tem_o_formato_esperado():
    execucao = ingestion_utils.iniciar_execucao("cards", endpoint="bulk-data/default_cards", parametros={"years_back": 5})
    assert execucao["table_name"] == "cards"
    assert execucao["status"] == "RUNNING"
    assert execucao["origem"] == "scryfall"
    assert len(execucao["run_id"]) == 12


def test_finalizar_execucao_sem_dbutils_nao_levanta():
    # Sem dbutils o write do controle falha em silêncio, sem mudar o status.
    execucao = ingestion_utils.iniciar_execucao("cards", endpoint="bulk-data/default_cards")
    execucao["files_written"] = 3
    finalizada = ingestion_utils.finalizar_execucao(execucao, "s3://test-bucket/stage", "SUCCESS")
    assert finalizada["status"] == "SUCCESS"
    assert finalizada["files_written"] == 3
    assert "duration_seconds" in finalizada


def test_finalizar_execucao_grava_json_de_controle_com_dbutils():
    gravado = {}

    class _FakeFs:
        def mkdirs(self, caminho):
            gravado["dir"] = caminho

        def put(self, caminho, conteudo, overwrite=True):
            gravado["caminho"] = caminho
            gravado["conteudo"] = conteudo

    ingestion_utils.dbutils = types.SimpleNamespace(fs=_FakeFs())
    try:
        execucao = ingestion_utils.iniciar_execucao("sets", endpoint="sets")
        ingestion_utils.finalizar_execucao(execucao, "s3://test-bucket/stage", "FAILED", erro="boom")

        assert gravado["dir"] == "s3://test-bucket/stage/_control/sets"
        assert gravado["caminho"] == f"s3://test-bucket/stage/_control/sets/{execucao['run_id']}.json"
        conteudo_json = json.loads(gravado["conteudo"])
        assert conteudo_json["status"] == "FAILED"
        assert conteudo_json["error"] == "boom"
    finally:
        del ingestion_utils.dbutils


def test_executar_ingestao_stage_sucesso_devolve_df_e_status_success():
    execucao_vista = {}

    def funcao_ingestao(execucao):
        execucao_vista["execucao"] = execucao
        return "fake-df"

    df, execucao = ingestion_utils.executar_ingestao_stage("sets", "sets", funcao_ingestao, "s3://test-bucket/stage")

    assert df == "fake-df"
    assert execucao is execucao_vista["execucao"]  # mesmo dict passado pra funcao_ingestao (execucao mutavel via finalizar_execucao)
    assert execucao["status"] == "SUCCESS"


def test_executar_ingestao_stage_df_none_levanta():
    # Tem que levantar para a task do job falhar.
    try:
        ingestion_utils.executar_ingestao_stage("sets", "sets", lambda execucao: None, "s3://test-bucket/stage")
    except Exception as e:
        assert "nao gravou nada" in str(e), str(e)
    else:
        raise AssertionError("esperava excecao quando funcao_ingestao nao grava nada")


def test_como_float_converte_int_e_preserva_none():
    # Scryfall pode devolver int (ex.: mana_value 0) e o schema e DoubleType;
    # createDataFrame rejeita int nesse caso.
    assert ingestion_utils.como_float(0) == 0.0
    assert isinstance(ingestion_utils.como_float(0), float)
    assert isinstance(ingestion_utils.como_float(3), float)
    assert ingestion_utils.como_float(1.5) == 1.5
    assert ingestion_utils.como_float(None) is None


def test_executar_ingestao_stage_df_none_propaga_erro_do_salvar():
    # salvar_em_parquet so registra o erro em execucao["error"]; a mensagem tem que
    # chegar na excecao do job.
    def funcao_ingestao(execucao):
        execucao["error"] = "S3 timeout"
        return None

    try:
        ingestion_utils.executar_ingestao_stage("sets", "sets", funcao_ingestao, "s3://test-bucket/stage")
    except Exception as e:
        assert "S3 timeout" in str(e), str(e)
    else:
        raise AssertionError("esperava excecao")


def test_executar_ingestao_stage_excecao_marca_failed_e_propaga():
    def funcao_ingestao(execucao):
        raise ValueError("boom")

    try:
        ingestion_utils.executar_ingestao_stage("sets", "sets", funcao_ingestao, "s3://test-bucket/stage")
    except ValueError as e:
        assert str(e) == "boom"
    else:
        raise AssertionError("expected ValueError to propagate")


def test_carimbo_da_execucao_e_constante_entre_chamadas():
    # salvar_em_parquet faz um .write por particao; o carimbo tem que ser o mesmo
    # em todas (DT_INGESTAO faz parte da chave de merge da Silver).
    execucao = ingestion_utils.iniciar_execucao("card_prices", "bulk-data/default_cards")

    assert ingestion_utils._carimbo_da_execucao(execucao) == ingestion_utils._carimbo_da_execucao(execucao)
    assert ingestion_utils._carimbo_da_execucao(execucao).isoformat() == execucao["started_at"]
    # execucao e opcional em salvar_em_parquet - sem ele ainda devolve um carimbo
    assert ingestion_utils._carimbo_da_execucao(None) is not None


def test_variavel_ambiente_sobrescreve_o_prefixo_de_stage():
    # Stage escreve fora do Unity Catalog: so o caminho no S3 (bucket/prefixo,
    # via env var) separa os ambientes.
    assert ingestion_utils.config_do_ambiente("s3_stage_prefix") is None
    os.environ["MTG_S3_STAGE_PREFIX"] = "prod/stage"
    try:
        assert ingestion_utils.obter_segredo("s3_stage_prefix", "stage") == "prod/stage"
    finally:
        del os.environ["MTG_S3_STAGE_PREFIX"]


def test_nome_do_parquet_nao_colide_entre_meses_no_mesmo_dia():
    # sets/card_prices particionam por releaseDate; o nome precisa da data
    # completa da run pra nao colidir entre meses.
    setembro = ingestion_utils.nome_arquivo_parquet(2021, 3, "20260905", "card_prices")
    outubro = ingestion_utils.nome_arquivo_parquet(2021, 3, "20261005", "card_prices")
    assert setembro == "2021_03_20260905_card_prices.parquet"
    assert setembro != outubro


def test_s3_bucket_do_ambiente_perde_o_esquema():
    # Stage monta f"s3://{bucket}/..."; com esquema no valor vira "s3://s3://".
    os.environ["MTG_S3_BUCKET"] = "s3://magicthegatheringdev/prd"
    try:
        assert ingestion_utils.obter_segredo("s3_bucket") == "magicthegatheringdev/prd"
    finally:
        del os.environ["MTG_S3_BUCKET"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_obter_http_com_retentativa_devolve_resposta_no_sucesso()
    test_obter_http_com_retentativa_repete_no_5xx_e_depois_passa()
    test_obter_http_com_retentativa_falha_na_hora_no_4xx()
    test_obter_http_com_retentativa_levanta_apos_esgotar_tentativas()
    test_obter_codigos_colecoes_filtra_por_data_e_poe_em_minusculas()
    test_iniciar_execucao_tem_o_formato_esperado()
    test_finalizar_execucao_sem_dbutils_nao_levanta()
    test_finalizar_execucao_grava_json_de_controle_com_dbutils()
    test_executar_ingestao_stage_sucesso_devolve_df_e_status_success()
    test_executar_ingestao_stage_df_none_levanta()
    test_como_float_converte_int_e_preserva_none()
    test_executar_ingestao_stage_df_none_propaga_erro_do_salvar()
    test_executar_ingestao_stage_excecao_marca_failed_e_propaga()
    test_carimbo_da_execucao_e_constante_entre_chamadas()
    test_variavel_ambiente_sobrescreve_o_prefixo_de_stage()
    test_nome_do_parquet_nao_colide_entre_meses_no_mesmo_dia()
    test_s3_bucket_do_ambiente_perde_o_esquema()
    print("OK")
