# Testa o obter_segredo() real de base_utils.py. Fora do Databricks dbutils não
# existe, então a leitura do secret levanta NameError e cai no caminho de fallback.

import importlib.util
import os
import sys

_CAMINHO = os.path.join(os.path.dirname(__file__), "base_utils.py")
_ESPEC = importlib.util.spec_from_file_location("base_utils", _CAMINHO)
base_utils = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(base_utils)


def test_padrao_explicito_vence():
    assert base_utils.obter_segredo("s3_bucket", valor_padrao="s3://explicit") == "s3://explicit"


def test_s3_bucket_do_ambiente_perde_esquema():
    # Stage/Bronze prefixam "s3://"; com o esquema no valor o caminho viraria "s3://s3://...".
    os.environ["MTG_S3_BUCKET"] = "s3://magicthegatheringdev/prd"
    try:
        assert base_utils.obter_segredo("s3_bucket") == "magicthegatheringdev/prd"
    finally:
        del os.environ["MTG_S3_BUCKET"]


def test_cai_no_padrao_seguro_comum():
    assert base_utils.obter_segredo("catalog_name") == "mtg_dev"


def test_cai_no_padrao_da_camada():
    assert base_utils.obter_segredo(
        "s3_gold_prefix", padroes_seguros_extras={"s3_gold_prefix": "magic_the_gathering/gold"}
    ) == "magic_the_gathering/gold"


def test_falha_sem_padrao_disponivel():
    try:
        base_utils.obter_segredo("unknown_secret")
    except Exception as e:
        assert "unknown_secret" in str(e)
    else:
        raise AssertionError("expected Exception for secret with no default")


def test_s3_bucket_sem_fallback_silencioso():
    # s3_bucket não tem default: falha em vez de usar um bucket placeholder.
    try:
        base_utils.obter_segredo("s3_bucket")
    except Exception as e:
        assert "s3_bucket" in str(e)
    else:
        raise AssertionError("expected Exception for s3_bucket with no default")


def test_variavel_ambiente_vence_o_segredo_e_o_padrao():
    # Precedencia env var > secret > default: prd sobrescreve so o que difere.
    os.environ["MTG_CATALOG_NAME"] = "mtg_prod"
    try:
        assert base_utils.obter_segredo("catalog_name") == "mtg_prod"
    finally:
        del os.environ["MTG_CATALOG_NAME"]


def test_producao_sem_catalogo_injetado_explode():
    # Sem MTG_CATALOG_NAME, producao resolveria mtg_dev (mesmo workspace) e gravaria nas tabelas de dev.
    os.environ["MTG_ENVIRONMENT"] = "production"
    try:
        base_utils.obter_segredo("catalog_name")
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
        assert base_utils.obter_segredo("catalog_name") == "mtg_prod"
    finally:
        del os.environ["MTG_ENVIRONMENT"]
        del os.environ["MTG_CATALOG_NAME"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_padrao_explicito_vence()
    test_s3_bucket_do_ambiente_perde_esquema()
    test_cai_no_padrao_seguro_comum()
    test_cai_no_padrao_da_camada()
    test_falha_sem_padrao_disponivel()
    test_s3_bucket_sem_fallback_silencioso()
    test_variavel_ambiente_vence_o_segredo_e_o_padrao()
    test_producao_sem_catalogo_injetado_explode()
    test_producao_com_catalogo_injetado_passa()
    print("OK")
