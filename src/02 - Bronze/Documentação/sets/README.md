# sets (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Catálogo dos sets/edições de Magic: The Gathering já lançados (incluindo
edições só digitais). Responde "quando saiu, quantas cartas tem, a que
bloco pertence e o que vem num pacote de booster" - útil pra organizar
coleção por edição ou situar uma carta na linha do tempo do jogo.

- **Tabela Unity Catalog:** `{catalog}.bronze.sets`.
- **Origem (Stage):** tabela `sets`, gravada por [`src/01 - Ingestion/sets.py`](<../../../01 - Ingestion/sets.py>) a partir da API Scryfall (`/sets`).
- **Notebook Bronze:** [`../../Dev/sets.py`](../../Dev/sets.py).
- **Histórico:** um mesmo set pode aparecer em runs diferentes com dados diferentes; cada run é preservada, sem deduplicação.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `code` | Código curto do set/edição (ex.: `M19`). |
| `name` | Nome completo do set/edição. |
| `type` | Tipo de set (core, expansion, masters, promo, etc.). |
| `border` | Cor de borda padrão das cartas do set (black/white/silver). |
| `mkm_id` | Id do set na Cardmarket (MKM) - usado pra cruzar com dado de preço/mercado da Cardmarket. |
| `mkm_name` | Nome do set na Cardmarket (MKM) - pode diferir do nome oficial usado na Scryfall. |
| `releaseDate` | Data de lançamento do set. |
| `gathererCode` | Código do set usado no Gatherer (Wizards). |
| `magicCardsInfoCode` | Código do set usado no site magiccards.info. |
| `oldCode` | Código antigo do set, se já foi renomeado. |
| `onlineOnly` | `true` se o set só existe em ambiente digital (Arena/MTGO). |
| `card_count` | Quantidade de cartas no set. |
| `parent_set_code` | Código do set "pai", quando este é um sub-set (ex.: promos de um set principal). |
| `block` | Bloco de expansão ao qual o set pertence. |
| `icon_svg_uri` | URL do ícone SVG do set. |
| `booster` | Campo legado da magicthegathering.io (lista de booster serializada) sem equivalente na Scryfall - sempre nulo desde a migração pra Scryfall, mantido só por imutabilidade de schema. Use `booster_0`…`booster_19` pro dado de booster atual. |
| `booster_0` … `booster_19` | Slot N do pacote de booster deste set (tipo de carta possível nessa posição) - a fonte devolve `booster` como lista e a Stage explode cada posição em uma coluna. |
