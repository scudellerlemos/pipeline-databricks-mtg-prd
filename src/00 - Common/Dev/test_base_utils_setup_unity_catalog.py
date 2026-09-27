# configurar_unity_catalog precisa propagar a excecao: se engolir, o notebook segue
# e a task termina verde sem ter escrito nada.

import importlib.util
import os
import sys

_CAMINHO = os.path.join(os.path.dirname(__file__), "base_utils.py")
_ESPEC = importlib.util.spec_from_file_location("base_utils", _CAMINHO)
base_utils = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(base_utils)


class _FakeSpark:
    """Grava os SQLs executados e falha nos que casarem com falhar_em."""

    def __init__(self, falhar_em=()):
        self.falhar_em = falhar_em
        self.executados = []

    def sql(self, comando):
        self.executados.append(comando)
        for trecho in self.falhar_em:
            if trecho in comando:
                raise RuntimeError(f"boom: {trecho}")


def _com_spark(spark):
    base_utils.obter_sessao_spark = lambda: spark
    return spark


def test_falha_de_esquema_propaga():
    spark = _com_spark(_FakeSpark(falhar_em=("CREATE SCHEMA",)))
    try:
        base_utils.configurar_unity_catalog("mtg_dev", "bronze")
    except RuntimeError as e:
        assert "CREATE SCHEMA" in str(e)
    else:
        raise AssertionError("falha de CREATE SCHEMA precisa estourar, nao virar return False")


def test_falha_de_catalogo_propaga():
    # USE e CREATE CATALOG falham: estoura sem chegar no CREATE SCHEMA.
    spark = _com_spark(_FakeSpark(falhar_em=("CATALOG",)))
    try:
        base_utils.configurar_unity_catalog("mtg_dev", "bronze")
    except RuntimeError:
        assert not any("SCHEMA" in s for s in spark.executados), \
            "nao pode seguir pro schema com o catalog quebrado"
    else:
        raise AssertionError("falha de catalog precisa estourar")


def test_cria_catalogo_quando_use_falha():
    # Catalog inexistente: USE CATALOG falha e o fallback tenta CREATE CATALOG IF NOT EXISTS.
    spark = _com_spark(_FakeSpark(falhar_em=("USE CATALOG mtg_dev",)))
    try:
        base_utils.configurar_unity_catalog("mtg_dev", "bronze")
    except RuntimeError:
        pass  # o fake falha em todo USE CATALOG, inclusive o do fallback
    assert "CREATE CATALOG IF NOT EXISTS mtg_dev" in spark.executados


def test_caminho_feliz_roda_todos_os_comandos():
    spark = _com_spark(_FakeSpark())
    base_utils.configurar_unity_catalog("mtg_dev", "bronze")
    assert spark.executados == [
        "USE CATALOG mtg_dev",
        "CREATE SCHEMA IF NOT EXISTS bronze",
        "USE SCHEMA bronze",
    ]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_falha_de_esquema_propaga()
    test_falha_de_catalogo_propaga()
    test_cria_catalogo_quando_use_falha()
    test_caminho_feliz_roda_todos_os_comandos()
    print("OK")
