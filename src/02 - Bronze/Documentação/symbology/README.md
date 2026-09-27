# symbology (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Catálogo de referência dos símbolos de mana e custo que aparecem no texto
das cartas (ex.: `{W}`, `{2/U}`, `{T}`). Serve pra traduzir/renderizar
corretamente esses símbolos e pra calcular quanto cada um vale em custo de
mana - tabela de apoio, praticamente estática (raramente ganha símbolo
novo).

- **Tabela Unity Catalog:** `{catalog}.bronze.symbology`.
- **Origem (Stage):** tabela `symbology`, gravada por [`src/01 - Ingestion/symbology.py`](<../../../01 - Ingestion/symbology.py>) a partir da API Scryfall (`/symbology`).
- **Notebook Bronze:** [`../../Dev/symbology.py`](../../Dev/symbology.py).
- **Histórico:** catálogo de referência estático - mesmo assim, sem `MERGE`: cada execução é um snapshot append-only do catálogo.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `symbol` | Símbolo de mana/custo em notação textual (ex.: `{W}`, `{2/U}`). |
| `svg_uri` | URL da imagem SVG do símbolo. |
| `loose_variant` | Variante alternativa de escrita do símbolo em texto livre, quando existe. |
| `english` | Descrição do símbolo em inglês. |
| `transposable` | `true` se o símbolo pode aparecer em ordem trocada em textos de regra. |
| `represents_mana` | `true` se o símbolo representa mana (nem todo símbolo representa - ex.: `{T}` de tap). |
| `appears_in_mana_costs` | `true` se o símbolo pode aparecer em custos de mana de cartas. |
| `mana_value` | Valor de mana que este símbolo contribui ao custo convertido de mana. |
| `hybrid` | `true` se é um símbolo de mana híbrida (ex.: `{W/U}`). |
| `phyrexian` | `true` se é um símbolo de mana phyrexiana (ex.: `{U/P}`). |
| `cmc` | Custo de mana convertido equivalente deste símbolo (pode diferir de `mana_value` em casos especiais). |
| `funny` | `true` se o símbolo só aparece em cartas não-oficiais/humorísticas (Un-sets). |
| `colors` | Cor(es) de mana associada(s) ao símbolo. |
| `gatherer_alternates` | Grafias alternativas do símbolo usadas no Gatherer. |
