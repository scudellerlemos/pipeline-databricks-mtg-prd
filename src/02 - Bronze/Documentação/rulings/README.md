# rulings (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Esclarecimentos oficiais de regras (rulings) publicados pela Wizards/Scryfall
pra cartas específicas, ligados por `oracle_id`. Serve pra responder dúvida
de interação entre cartas ou interpretação de regra que o texto da carta
sozinho não deixa claro - uma carta pode acumular várias rulings ao longo do
tempo.

- **Tabela Unity Catalog:** `{catalog}.bronze.rulings`.
- **Origem (Stage):** tabela `rulings`, gravada por [`src/01 - Ingestion/rulings.py`](<../../../01 - Ingestion/rulings.py>) a partir da API Scryfall (`/bulk-data` → `rulings`).
- **Notebook Bronze:** [`../../Dev/rulings.py`](../../Dev/rulings.py).
- **Relação com `cards`:** ligação é por `oracle_id` (1 oracle_id → N impressões em `cards`) - o join acontece na Gold, não nesta camada.
- **Histórico:** EL puro, sem deduplicação.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns) -
**`source` repetida abaixo por causa de um bug conhecido nesta tabela**:

| Coluna | Descrição |
|---|---|
| `oracle_id` | Oracle id da carta a que esta ruling se aplica (mesmo valor para todas as impressões da carta). |
| `source` | Sempre `scryfall`. **Bug conhecido:** a ruling de origem traz quem a emitiu (`wotc` ou `scryfall`), mas o `save_to_parquet` da Stage sobrescreve a coluna com `scryfall` e o valor original se perde. |
| `published_at` | Data de publicação da ruling. |
| `comment` | Texto da ruling / esclarecimento de regras. |
