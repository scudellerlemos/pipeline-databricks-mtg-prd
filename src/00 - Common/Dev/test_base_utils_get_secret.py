# Testa o get_secret() real de base_utils.py. Fora do Databricks dbutils não
# existe, então a leitura do secret levanta NameError e cai no caminho de fallback.

import importlib.util
import os
import sys

_PATH = os.path.join(os.path.dirname(__file__), "base_utils.py")
_SPEC = importlib.util.spec_from_file_location("base_utils", _PATH)
base_utils = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(base_utils)


def test_explicit_default_wins():
    assert base_utils.get_secret("s3_bucket", default_value="s3://explicit") == "s3://explicit"


def test_s3_bucket_from_env_drops_scheme():
    # Stage/Bronze prefixam "s3://"; com o esquema no valor o caminho viraria "s3://s3://...".
    os.environ["MTG_S3_BUCKET"] = "s3://magicthegatheringdev/prd"
    try:
        assert base_utils.get_secret("s3_bucket") == "magicthegatheringdev/prd"
    finally:
        del os.environ["MTG_S3_BUCKET"]


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
    # s3_bucket não tem default: falha em vez de usar um bucket placeholder.
    try:
        base_utils.get_secret("s3_bucket")
    except Exception as e:
        assert "s3_bucket" in str(e)
    else:
        raise AssertionError("expected Exception for s3_bucket with no default")


def test_env_var_vence_o_secret_e_o_default():
    # Precedencia env var > secret > default: prd sobrescreve so o que difere.
    os.environ["MTG_CATALOG_NAME"] = "mtg_prod"
    try:
        assert base_utils.get_secret("catalog_name") == "mtg_prod"
    finally:
        del os.environ["MTG_CATALOG_NAME"]


def test_producao_sem_catalogo_injetado_explode():
    # Sem MTG_CATALOG_NAME, producao resolveria mtg_dev (mesmo workspace) e gravaria nas tabelas de dev.
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
    test_s3_bucket_from_env_drops_scheme()
    test_falls_back_to_common_safe_default()
    test_falls_back_to_layer_specific_default()
    test_raises_when_no_default_available()
    test_s3_bucket_has_no_silent_fallback()
    test_env_var_vence_o_secret_e_o_default()
    test_producao_sem_catalogo_injetado_explode()
    test_producao_com_catalogo_injetado_passa()
    print("OK")
