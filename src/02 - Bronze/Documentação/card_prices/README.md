# card_prices (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Histórico de cotações de preço de cartas em dólar, euro e MTGO ticket - cada
linha é o preço de uma IMPRESSÃO de carta em um momento coletado. Preço em
Magic varia por impressão: a mesma carta reimpressa em outro set tem cotação
própria, e cada uma aparece aqui com seu `id`. Serve pra acompanhar
valorização/desvalorização ao longo do tempo, comparar preço entre
impressões/sets ou montar um indicador de valor de coleção. A mesma impressão
tem várias linhas (uma por coleta) de propósito - é histórico, não é a
cotação "atual".

- **Tabela Unity Catalog:** `{catalog}.bronze.card_prices`.
- **Origem (Stage):** tabela `card_prices`, gravada por [`src/01 - Ingestion/card_prices.py`](<../../../01 - Ingestion/card_prices.py>) a partir da API Scryfall (`/bulk-data` → `default_cards`).
- **Notebook Bronze:** [`../../Dev/card_prices.py`](../../Dev/card_prices.py).
- **Histórico:** o preço de uma mesma impressão em runs/dias diferentes gera linhas diferentes, todas preservadas - não há filtro por data/período nem `dropDuplicates` por impressão. Nenhuma checagem de consistência contra `cards` acontece aqui (é regra de negócio, fora da Bronze).

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `id` | Id da impressão cotada - mesmo id da tabela `cards`. Chave de join entre preço e carta. |
| `name` | Nome da carta. |
| `set` | Código do set/edição desta impressão. |
| `rarity` | Raridade da impressão. |
| `usd` | Preço em dólares americanos da variante normal (não-foil) desta impressão, como veio da fonte. |
| `usd_foil` | Preço em dólares da variante foil da MESMA impressão - a fonte cota foil separado e costuma valer múltiplos do não-foil. Nulo quando a impressão não tem foil. |
| `usd_etched` | Preço em dólares da variante etched foil da mesma impressão. Nulo na esmagadora maioria - só alguns sets tiveram etched. |
| `eur` | Preço em euros da variante normal (não-foil) desta impressão, como veio da fonte. |
| `eur_foil` | Preço em euros da variante foil da mesma impressão. Nulo quando a impressão não tem foil. |
| `tix` | Preço em MTGO tickets, como veio da fonte. |
| `scryfall_uri` | URL da página da carta na Scryfall. |
| `image_url` | URL da imagem da carta. Em dupla face usa a imagem da frente (`card_faces[0]`), mesmo fallback de `cards.imageUrl`. |
| `releaseDate` | Data de lançamento da impressão/set desta carta. |
