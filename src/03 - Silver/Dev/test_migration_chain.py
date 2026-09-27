# ponytail: self-check de lógica pura pra _resolve_id_chain() em TB_MOV_MIGRACOES_CARTAS.ipynb.
# Não dá pra importar o notebook diretamente (não é um módulo .py, e suas outras funções
# precisam de uma sessão spark viva do Databricks), então isto espelha só a função de
# resolução de cadeia sob teste.


def _resolve_id_chain(direct_map):
    resolved = {}
    for start in direct_map:
        current = start
        seen = {start}
        hops = 0
        while current in direct_map and hops < 10:
            nxt = direct_map[current]
            if nxt in seen:
                break
            current = nxt
            seen.add(current)
            hops += 1
        resolved[start] = current
    return resolved


def test_no_migrations():
    assert _resolve_id_chain({}) == {}


def test_single_merge():
    # A mergeou em B
    assert _resolve_id_chain({"A": "B"}) == {"A": "B"}


def test_chained_merge_resolves_to_final_id():
    # A mergeou em B, B mergeou em C -> A deve resolver direto pra C
    direct_map = {"A": "B", "B": "C"}
    assert _resolve_id_chain(direct_map) == {"A": "C", "B": "C"}


def test_cycle_stops_instead_of_looping_forever():
    # não deveria acontecer em dado real da Scryfall, mas não pode travar
    direct_map = {"A": "B", "B": "A"}
    resolved = _resolve_id_chain(direct_map)
    assert resolved["A"] in ("A", "B")
    assert resolved["B"] in ("A", "B")


def test_independent_chains_dont_interfere():
    direct_map = {"A": "B", "B": "C", "X": "Y"}
    resolved = _resolve_id_chain(direct_map)
    assert resolved["A"] == "C"
    assert resolved["X"] == "Y"


def _build_direct_map(rows_sorted_by_dt_and_id):
    """Espelho do loop de construção do direct_map em attach_canonical_id(): as linhas devem
    chegar pré-ordenadas por (ID_CARTA_ANTIGO, DT_EXECUCAO, ID_MIGRACAO) - igual ao
    .orderBy() antes do .collect() no notebook."""
    direct_map = {}
    for r in rows_sorted_by_dt_and_id:
        direct_map[r["ID_CARTA_ANTIGO"]] = r["ID_CARTA_NOVO"]
    return direct_map


def test_direct_map_keeps_most_recent_migration_when_a_card_remerges():
    # ID_CARTA_ANTIGO "A" migrou duas vezes (re-mesclada depois de já ter
    # mesclado antes) - ordenado por DT_EXECUCAO, o loop deve ficar com a
    # migração mais recente (pra "C"), não a mais antiga (pra "B").
    rows_sorted = [
        {"ID_CARTA_ANTIGO": "A", "ID_CARTA_NOVO": "B", "DT_EXECUCAO": "2024-01-01"},
        {"ID_CARTA_ANTIGO": "A", "ID_CARTA_NOVO": "C", "DT_EXECUCAO": "2024-06-01"},
    ]
    assert _build_direct_map(rows_sorted) == {"A": "C"}
    # ordem invertida na entrada (simula collect() sem orderBy) daria "B" -
    # por isso quem chama precisa garantir a ordenação antes.
    assert _build_direct_map(list(reversed(rows_sorted))) == {"A": "B"}


if __name__ == "__main__":
    test_no_migrations()
    test_single_merge()
    test_chained_merge_resolves_to_final_id()
    test_cycle_stops_instead_of_looping_forever()
    test_independent_chains_dont_interfere()
    test_direct_map_keeps_most_recent_migration_when_a_card_remerges()
    print("OK")
