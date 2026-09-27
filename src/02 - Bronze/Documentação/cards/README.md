# cards (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Catálogo de cartas de Magic: The Gathering - uma linha por impressão/edição
de carta. Responde "o que é essa carta": texto de regras, custo de mana,
tipo, raridade, artista, em qual set saiu e em quais formatos de jogo
(Standard, Commander, etc.) ela é legal. Base pra qualquer análise de deck,
coleção ou busca de carta.

- **Tabela Unity Catalog:** `{catalog}.bronze.cards`.
- **Origem (Stage):** tabela `cards`, gravada por [`src/01 - Ingestion/cards.py`](<../../../01 - Ingestion/cards.py>) a partir da API Scryfall (`/bulk-data` → `default_cards`).
- **Notebook Bronze:** [`../../Dev/cards.py`](../../Dev/cards.py).
- **Histórico:** uma mesma carta (mesmo `id`) pode aparecer em runs diferentes com dados diferentes (ex.: `legalities` mudou) - cada run é preservada, sem deduplicação.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `id` | Id único da carta na Scryfall. |
| `name` | Nome da carta. |
| `manaCost` | Custo de mana em notação simbólica (ex.: `{2}{U}{U}`). |
| `cmc` | Custo de mana convertido (soma numérica do custo de mana). |
| `colors` | Cores da carta. |
| `colorIdentity` | Identidade de cor da carta (usada em formatos como Commander). |
| `type` | Linha de tipo completa da carta (ex.: `Creature — Human Wizard`). |
| `types` | Tipos principais da carta (ex.: Creature, Instant). |
| `subtypes` | Subtipos da carta (ex.: Human, Wizard). |
| `rarity` | Raridade da impressão (common/uncommon/rare/mythic). |
| `set` | Código do set/edição desta impressão. |
| `setName` | Nome completo do set/edição. |
| `text` | Texto de regras (oracle text) impresso na carta. |
| `artist` | Nome do ilustrador. |
| `number` | Número de colecionador dentro do set. |
| `power` | Força da criatura (texto, pode ser `*`). |
| `toughness` | Resistência da criatura (texto, pode ser `*`). |
| `layout` | Layout físico da carta (normal, split, transform, etc.). |
| `multiverseid` | Id da carta no Gatherer (banco oficial de cartas da Wizards) - usado pra linkar a carta na fonte oficial. |
| `imageUrl` | URL da imagem da carta. |
| `variations` | Ids de outras impressões/variações visuais da mesma carta. |
| `foreignNames` | Nomes/textos traduzidos em outros idiomas. |
| `printings` | Códigos de todos os sets em que a carta já foi impressa. |
| `originalText` | Texto de regras como impresso originalmente (antes de errata). |
| `originalType` | Linha de tipo original antes de reclassificações. |
| `legalities` | Legalidade da carta por formato de jogo. |
