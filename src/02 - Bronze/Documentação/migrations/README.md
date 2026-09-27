# migrations (Bronze)

> Ver [`../README.md`](../README.md) para a arquitetura completa da camada
> Bronze (idempotência, controle de execução, colunas técnicas comuns,
> particionamento). Este arquivo cobre só o que é específico desta tabela.

Histórico de trocas de identificador de carta na Scryfall (quando duas
cartas são unificadas ou uma é removida do catálogo). Serve pra reconciliar
um id antigo com o novo e evitar perder o vínculo de uma carta em
análises/joins feitos antes da mudança.

- **Tabela Unity Catalog:** `{catalog}.bronze.migrations`.
- **Origem (Stage):** tabela `migrations`, gravada por [`src/01 - Ingestion/migrations.py`](<../../../01 - Ingestion/migrations.py>) a partir da API Scryfall (`/migrations`).
- **Notebook Bronze:** [`../../Dev/migrations.py`](../../Dev/migrations.py).
- **Histórico:** preservado integralmente, sem filtro, para a Silver resolver ids antigos se precisar.

## Colunas

Além das [colunas técnicas comuns](../README.md#colunas-técnicas-comuns):

| Coluna | Descrição |
|---|---|
| `id` | Id único do registro de migração na Scryfall. |
| `uri` | URL da API da Scryfall para este registro de migração. |
| `performed_at` | Data em que a migração de id foi executada pela Scryfall. |
| `migration_strategy` | Estratégia da migração (ex.: `merge`, `delete`). |
| `old_scryfall_id` | Id Scryfall antigo, substituído pela migração. |
| `new_scryfall_id` | Novo id Scryfall quando a estratégia é `merge` (pode ser nulo em `delete`). |
| `note` | Nota livre da Scryfall explicando o motivo da migração. |
| `metadata_id` | Id da carta associada a esta migração. |
| `metadata_lang` | Idioma da carta associada a esta migração. |
| `metadata_name` | Nome da carta associada a esta migração. |
| `metadata_set_code` | Código do set da carta associada a esta migração. |
| `metadata_oracle_id` | Oracle id da carta associada a esta migração. |
| `metadata_collector_number` | Número de colecionador da carta associada a esta migração. |
