# card_prices (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Histórico de cotações de preço de cartas em dólar, euro e MTGO ticket - cada
linha é o preço de uma carta em um momento coletado. Serve pra acompanhar
valorização/desvalorização de uma carta ao longo do tempo, comparar preço
entre cartas/sets ou montar um indicador de valor de coleção. Uma mesma
carta tem várias linhas (uma por coleta) de propósito - é histórico, não é
a cotação "atual".

- **Tabela Unity Catalog:** `{catalog}.bronze.card_prices`.
- **Origem (Stage):** tabela `card_prices`, gravada por [`src/01 - Ingestion/card_prices.py`](<../../../01 - Ingestion/card_prices.py>) a partir da API Scryfall.
- **Notebook Bronze:** [`../../Dev/card_prices.py`](../../Dev/card_prices.py).
- **Histórico:** o preço de uma mesma carta em runs/dias diferentes gera linhas diferentes, todas preservadas - não há filtro por data/período nem `dropDuplicates` por carta. Nenhuma checagem de consistência contra `cards` acontece aqui (a antiga limpeza cruzada que apagava preços de cartas "ausentes" era regra de negócio e foi removida desta camada).

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `name` | Nome da carta. |
| `set` | Código do set/edição desta impressão. |
| `rarity` | Raridade da impressão. |
| `usd` | Preço em dólares americanos, como veio da fonte. |
| `eur` | Preço em euros, como veio da fonte. |
| `tix` | Preço em MTGO tickets, como veio da fonte. |
| `scryfall_uri` | URL da página da carta na Scryfall. |
| `image_url` | URL da imagem da carta. |
| `releaseDate` | Data de lançamento da impressão/set desta carta. |
