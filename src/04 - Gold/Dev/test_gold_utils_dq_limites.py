# ponytail: DQ que so imprime e DQ que ninguem le - a task fica verde e o
# numero so existe no log do cluster. Este self-check cobre a decisao de
# abortar: limite implicito 0, tripwire com numero, e None pra contagem que
# e esperada ser > 0 (orfaos, ids migrados).
#
# gold_utils nao importa aqui (precisa de spark/dbutils vivos do Databricks),
# entao a funcao sob teste e extraida do arquivo real por exec - se a
# assinatura ou o contrato mudarem, este teste quebra junto.

import io
import os
import re

_GOLD_UTILS = os.path.join(os.path.dirname(__file__), "gold_utils.py")


def _carregar():
    fonte = io.open(_GOLD_UTILS, encoding="utf-8").read()
    trecho = re.search(
        r"^class DataQualityError.*?^    return resultados$", fonte, re.S | re.M
    )
    assert trecho, "class DataQualityError / run_data_quality_checks sumiram do gold_utils"
    ns = {}
    exec(trecho.group(0), ns)
    return ns["run_data_quality_checks"], ns["DataQualityError"]


run_data_quality_checks, DataQualityError = _carregar()


class FakeSpark:
    """Devolve a contagem que a query pedir: o SQL nao esta sob teste aqui."""

    def __init__(self, contagens):
        self.contagens = contagens

    def sql(self, query):
        valor = self.contagens[query.strip()]
        return type("R", (), {"collect": lambda _s: [[valor]]})()


def test_limite_implicito_e_zero_e_qualquer_ocorrencia_aborta():
    spark = FakeSpark({"q": 1})
    try:
        run_data_quality_checks(spark, "t", {"valor_negativo_preco": "q"})
    except DataQualityError as e:
        assert "valor_negativo_preco=1 (limite 0)" in str(e)
        # a auditoria precisa da contagem mesmo quando a run aborta
        assert e.resultados == {"valor_negativo_preco": 1}
        return
    raise AssertionError("contagem 1 com limite 0 tinha que abortar")


def test_zero_passa():
    spark = FakeSpark({"q": 0})
    assert run_data_quality_checks(spark, "t", {"fk_null_id_oracle": "q"}) == {
        "fk_null_id_oracle": 0
    }


def test_tripwire_passa_no_limite_e_aborta_acima():
    # a baseline medida (7476) tem que passar; a mudanca de regime, nao.
    assert run_data_quality_checks(
        FakeSpark({"q": 7476}), "pré-join", {"precos_excluidos_sem_carta": ("q", 12000)}
    ) == {"precos_excluidos_sem_carta": 7476}

    try:
        run_data_quality_checks(
            FakeSpark({"q": 40000}), "pré-join", {"precos_excluidos_sem_carta": ("q", 12000)}
        )
    except DataQualityError as e:
        assert "40000 (limite 12000)" in str(e)
        return
    raise AssertionError("40 mil orfaos tinham que abortar")


def test_limite_none_nunca_aborta():
    # cartas_com_id_migrado so cresce - limite nenhum faria sentido.
    assert run_data_quality_checks(
        FakeSpark({"q": 999999}), "t", {"cartas_com_id_migrado": ("q", None)}
    ) == {"cartas_com_id_migrado": 999999}


def test_roda_todas_antes_de_abortar():
    # descobrir os tres problemas de uma vez vale uma run; um por run, nao.
    spark = FakeSpark({"a": 5, "b": 0, "c": 7})
    try:
        run_data_quality_checks(spark, "t", {"a": "a", "b": "b", "c": "c"})
    except DataQualityError as e:
        assert "a=5" in str(e) and "c=7" in str(e)
        assert e.resultados == {"a": 5, "b": 0, "c": 7}
        return
    raise AssertionError("tinha que abortar")


def test_none_da_query_vira_zero():
    # COUNT(DISTINCT ...) devolve NULL quando nao ha linha nenhuma.
    assert run_data_quality_checks(FakeSpark({"q": None}), "t", {"x": "q"}) == {"x": 0}


if __name__ == "__main__":
    import sys

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_limite_implicito_e_zero_e_qualquer_ocorrencia_aborta()
    test_zero_passa()
    test_tripwire_passa_no_limite_e_aborta_acima()
    test_limite_none_nunca_aborta()
    test_roda_todas_antes_de_abortar()
    test_none_da_query_vira_zero()
    print("OK")
