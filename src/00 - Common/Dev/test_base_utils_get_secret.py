# ponytail: exercita o get_secret() real de base_utils.py. dbutils não existe
# fora de um cluster Databricks, então a chamada dentro do try de get_secret
# levanta NameError - capturado pelo except genérico, exatamente o caminho de
# fallback que este teste quer cobrir.

import importlib.util
import os
import sys

_PATH = os.path.join(os.path.dirname(__file__), "base_utils.py")
_SPEC = importlib.util.spec_from_file_location("base_utils", _PATH)
base_utils = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(base_utils)


def test_explicit_default_wins():
    assert base_utils.get_secret("s3_bucket", default_value="s3://explicit") == "s3://explicit"


def test_falls_back_to_common_safe_default():
    assert base_utils.get_secret("catalog_name") == "mtg_dev"


def test_falls_back_to_layer_specific_default():
    assert base_utils.get_secret(
        "s3_gold_prefix", extra_safe_defaults={"s3_gold_prefix": "magic_the_gathering/gold"}
    ) == "magic_the_gathering/gold"


def test_raises_when_no_default_available():
    try:
        base_utils.get_secret("unknown_secret")
    except Exception as e:
        assert "unknown_secret" in str(e)
    else:
        raise AssertionError("expected Exception for secret with no default")


def test_s3_bucket_has_no_silent_fallback():
    # s3_bucket é o destino real de leitura/escrita de todas as camadas - não
    # pode cair silenciosamente num bucket placeholder quando o secret falha.
    try:
        base_utils.get_secret("s3_bucket")
    except Exception as e:
        assert "s3_bucket" in str(e)
    else:
        raise AssertionError("expected Exception for s3_bucket with no default")


def test_env_var_vence_o_secret_e_o_default():
    # Precedencia env var > secret > default: e assim que prd sobrescreve so o
    # que difere, em vez de duplicar o scope inteiro. Nenhuma dessas chaves e
    # segredo - sao config (bucket, prefixo, URL publica).
    os.environ["MTG_CATALOG_NAME"] = "mtg_prod"
    try:
        assert base_utils.get_secret("catalog_name") == "mtg_prod"
    finally:
        del os.environ["MTG_CATALOG_NAME"]


def test_producao_sem_catalogo_injetado_explode():
    # O desastre que isso trava: dev e prd dividem o mesmo workspace, entao
    # esquecer de injetar MTG_CATALOG_NAME faria o job de producao gravar por
    # cima das tabelas de mtg_dev - task verde, dado destruido.
    os.environ["MTG_ENVIRONMENT"] = "production"
    try:
        base_utils.get_secret("catalog_name")
    except Exception as e:
        assert "mtg_dev" in str(e)
    else:
        raise AssertionError("producao nao pode resolver o catalogo pra mtg_dev")
    finally:
        del os.environ["MTG_ENVIRONMENT"]


def test_producao_com_catalogo_injetado_passa():
    os.environ["MTG_ENVIRONMENT"] = "production"
    os.environ["MTG_CATALOG_NAME"] = "mtg_prod"
    try:
        assert base_utils.get_secret("catalog_name") == "mtg_prod"
    finally:
        del os.environ["MTG_ENVIRONMENT"]
        del os.environ["MTG_CATALOG_NAME"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_explicit_default_wins()
    test_falls_back_to_common_safe_default()
    test_falls_back_to_layer_specific_default()
    test_raises_when_no_default_available()
    test_s3_bucket_has_no_silent_fallback()
    test_env_var_vence_o_secret_e_o_default()
    test_producao_sem_catalogo_injetado_explode()
    test_producao_com_catalogo_injetado_passa()
    print("OK")
