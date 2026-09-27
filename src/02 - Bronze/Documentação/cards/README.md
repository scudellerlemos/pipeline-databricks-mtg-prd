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
| `id` | Id da impressão na Scryfall (cada reimpressão da mesma carta tem o seu). |
| `name` | Nome da carta. |
| `manaCost` | Custo de mana em notação simbólica (ex.: `{2}{U}{U}`). |
| `cmc` | Custo de mana convertido (soma numérica do custo de mana). |
| `colors` | Cores da carta. |
| `colorIdentity` | Identidade de cor da carta (usada em formatos como Commander). |
| `type` | Linha de tipo completa da carta (ex.: `Creature — Human Wizard`). |
| `types` | Tipos principais da carta (ex.: Creature, Instant). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `subtypes` | Subtipos da carta (ex.: Human, Wizard). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `rarity` | Raridade da impressão (common/uncommon/rare/mythic). |
| `set` | Código do set/edição desta impressão. |
| `setName` | Nome completo do set/edição. |
| `text` | Texto de regras oficial atual (Oracle text, com erratas) - não necessariamente o impresso nesta edição. Em dupla face, só a frente. |
| `artist` | Nome do ilustrador. |
| `number` | Número de colecionador dentro do set. |
| `power` | Força da criatura (texto, pode ser `*`). |
| `toughness` | Resistência da criatura (texto, pode ser `*`). |
| `layout` | Layout físico da carta (normal, split, transform, etc.). |
| `multiverseid` | Id da carta no Gatherer (banco oficial de cartas da Wizards) - usado pra linkar a carta na fonte oficial. Sempre nulo (campo legado da magicthegathering.io; a Scryfall tem `multiverse_ids`, lista, mas a Stage não mapeia). |
| `imageUrl` | URL da imagem da carta. |
| `variations` | Ids de outras impressões/variações visuais da mesma carta. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `foreignNames` | Nomes/textos traduzidos em outros idiomas. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `printings` | Códigos de todos os sets em que a carta já foi impressa. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `originalText` | Texto de regras como impresso originalmente (antes de errata). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `originalType` | Linha de tipo original antes de reclassificações. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `legalities` | Legalidade da carta por formato de jogo. |
| `oracle_id` | Oracle id da carta na Scryfall - estável entre impressões (printings) da mesma carta, ao contrário de `id` (que identifica só esta impressão). Usado na Gold para ligar a carta aos esclarecimentos (`rulings`). |
