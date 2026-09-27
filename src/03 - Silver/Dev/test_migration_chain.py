# Testa _resolver_cadeia_ids() de TB_MOV_MIGRACOES_CARTAS.py. O notebook não é
# importável fora do Databricks, então a função é copiada aqui; manter em sincronia.


def _resolver_cadeia_ids(mapa_direto):
    resolvidos = {}
    for inicio in mapa_direto:
        atual = inicio
        vistos = {inicio}
        saltos = 0
        while atual in mapa_direto and saltos < 10:
            proximo = mapa_direto[atual]
            if proximo in vistos:
                break
            atual = proximo
            vistos.add(atual)
            saltos += 1
        resolvidos[inicio] = atual
    return resolvidos


def test_sem_migracoes():
    assert _resolver_cadeia_ids({}) == {}


def test_unificacao_simples():
    assert _resolver_cadeia_ids({"A": "B"}) == {"A": "B"}


def test_unificacao_encadeada_resolve_para_id_final():
    mapa_direto = {"A": "B", "B": "C"}
    assert _resolver_cadeia_ids(mapa_direto) == {"A": "C", "B": "C"}


def test_ciclo_para_em_vez_de_loop_infinito():
    mapa_direto = {"A": "B", "B": "A"}
    resolvidos = _resolver_cadeia_ids(mapa_direto)
    assert resolvidos["A"] in ("A", "B")
    assert resolvidos["B"] in ("A", "B")


def test_cadeias_independentes_nao_interferem():
    mapa_direto = {"A": "B", "B": "C", "X": "Y"}
    resolvidos = _resolver_cadeia_ids(mapa_direto)
    assert resolvidos["A"] == "C"
    assert resolvidos["X"] == "Y"


def _montar_mapa_direto(linhas_ordenadas_por_dt_e_id):
    """Cópia do loop de anexar_id_canonico(); espera linhas já ordenadas por
    (ID_CARTA_ANTIGO, DT_EXECUCAO, ID_MIGRACAO), como o orderBy do notebook."""
    mapa_direto = {}
    for r in linhas_ordenadas_por_dt_e_id:
        mapa_direto[r["ID_CARTA_ANTIGO"]] = r["ID_CARTA_NOVO"]
    return mapa_direto


def test_mapa_direto_mantem_migracao_mais_recente_quando_carta_reunifica():
    linhas_ordenadas = [
        {"ID_CARTA_ANTIGO": "A", "ID_CARTA_NOVO": "B", "DT_EXECUCAO": "2024-01-01"},
        {"ID_CARTA_ANTIGO": "A", "ID_CARTA_NOVO": "C", "DT_EXECUCAO": "2024-06-01"},
    ]
    assert _montar_mapa_direto(linhas_ordenadas) == {"A": "C"}
    # Sem a ordenação, venceria a migração errada.
    assert _montar_mapa_direto(list(reversed(linhas_ordenadas))) == {"A": "B"}


if __name__ == "__main__":
    test_sem_migracoes()
    test_unificacao_simples()
    test_unificacao_encadeada_resolve_para_id_final()
    test_ciclo_para_em_vez_de_loop_infinito()
    test_cadeias_independentes_nao_interferem()
    test_mapa_direto_mantem_migracao_mais_recente_quando_carta_reunifica()
    print("OK")
