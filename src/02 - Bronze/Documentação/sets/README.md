# sets (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Sets/edições de Magic: The Gathering. Cada run da Stage traz os lançados a
partir de 1º de janeiro de (ano atual − `years_back`, padrão 5) e a Bronze
acumula as runs (append), incluindo edições só digitais e
anunciadas ainda não lançadas. Responde "quando saiu, quantas cartas tem e a
que bloco pertence" - útil pra organizar
coleção por edição ou situar uma carta na linha do tempo do jogo.

- **Tabela Unity Catalog:** `{catalog}.bronze.sets`.
- **Origem (Stage):** tabela `sets`, gravada por [`src/01 - Ingestion/sets.py`](<../../../01 - Ingestion/sets.py>) a partir da API Scryfall (`/sets`).
- **Notebook Bronze:** [`../../Dev/sets.py`](../../Dev/sets.py).
- **Histórico:** um mesmo set pode aparecer em runs diferentes com dados diferentes; cada run é preservada, sem deduplicação.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `code` | Código curto do set/edição (ex.: `dmu`, minúsculo como na Scryfall). |
| `name` | Nome completo do set/edição. |
| `type` | Tipo de set (core, expansion, masters, promo, etc.). |
| `border` | Cor de borda padrão das cartas do set (black/white/silver). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `mkm_id` | Id do set na Cardmarket (MKM) - usado pra cruzar com dado de preço/mercado da Cardmarket. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `mkm_name` | Nome do set na Cardmarket (MKM) - pode diferir do nome oficial usado na Scryfall. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `releaseDate` | Data de lançamento do set. |
| `gathererCode` | Código do set usado no Gatherer (Wizards). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `magicCardsInfoCode` | Código do set usado no site magiccards.info. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `oldCode` | Código antigo do set, se já foi renomeado. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall). |
| `onlineOnly` | `true` se o set só existe em ambiente digital (Arena/MTGO). |
| `card_count` | Quantidade de cartas no set. |
| `parent_set_code` | Código do set "pai", quando este é um sub-set (ex.: promos de um set principal). |
| `block` | Bloco de expansão ao qual o set pertence. |
| `icon_svg_uri` | URL do ícone SVG do set. |
| `booster` | Campo legado da magicthegathering.io (lista de booster serializada) sem equivalente na Scryfall - sempre nulo desde a migração pra Scryfall, mantido só por imutabilidade de schema. |
| `booster_0` … `booster_19` | Slot N do pacote de booster (legado da magicthegathering.io). Sempre nulo: a Scryfall não expõe booster; colunas mantidas só por imutabilidade de schema. |
