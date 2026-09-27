# ponytail: o retorno bool antigo era ignorado pelos 22 call sites, entao uma
# falha de USE/CREATE SCHEMA virava print + notebook seguindo em frente e task
# verde sem escrita. Estes testes travam a propagacao da excecao.

import importlib.util
import os
import sys

_PATH = os.path.join(os.path.dirname(__file__), "base_utils.py")
_SPEC = importlib.util.spec_from_file_location("base_utils", _PATH)
base_utils = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(base_utils)


class _FakeSpark:
    """Grava os SQLs executados e falha nos que casarem com fail_on."""

    def __init__(self, fail_on=()):
        self.fail_on = fail_on
        self.executed = []

    def sql(self, statement):
        self.executed.append(statement)
        for fragment in self.fail_on:
            if fragment in statement:
                raise RuntimeError(f"boom: {fragment}")


def _with_spark(spark):
    base_utils.get_spark_session = lambda: spark
    return spark


def test_schema_failure_propagates():
    spark = _with_spark(_FakeSpark(fail_on=("CREATE SCHEMA",)))
    try:
        base_utils.setup_unity_catalog("mtg_dev", "bronze")
    except RuntimeError as e:
        assert "CREATE SCHEMA" in str(e)
    else:
        raise AssertionError("falha de CREATE SCHEMA precisa estourar, nao virar return False")


def test_catalog_failure_propagates():
    # USE CATALOG falha e o CREATE CATALOG de fallback tambem - nada a fazer,
    # tem que estourar em vez de seguir pro CREATE SCHEMA.
    spark = _with_spark(_FakeSpark(fail_on=("CATALOG",)))
    try:
        base_utils.setup_unity_catalog("mtg_dev", "bronze")
    except RuntimeError:
        assert not any("SCHEMA" in s for s in spark.executed), \
            "nao pode seguir pro schema com o catalog quebrado"
    else:
        raise AssertionError("falha de catalog precisa estourar")


def test_creates_catalog_when_use_fails():
    # Caminho normal de primeira carga: USE CATALOG falha porque o catalog nao
    # existe, o fallback cria e da USE de novo. Nao pode estourar.
    spark = _with_spark(_FakeSpark(fail_on=("USE CATALOG mtg_dev",)))
    try:
        base_utils.setup_unity_catalog("mtg_dev", "bronze")
    except RuntimeError:
        pass  # o fake falha em todo USE CATALOG, inclusive o do fallback
    assert "CREATE CATALOG IF NOT EXISTS mtg_dev" in spark.executed


def test_happy_path_runs_all_statements():
    spark = _with_spark(_FakeSpark())
    base_utils.setup_unity_catalog("mtg_dev", "bronze")
    assert spark.executed == [
        "USE CATALOG mtg_dev",
        "CREATE SCHEMA IF NOT EXISTS bronze",
        "USE SCHEMA bronze",
    ]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_schema_failure_propagates()
    test_catalog_failure_propagates()
    test_creates_catalog_when_use_fails()
    test_happy_path_runs_all_statements()
    print("OK")
