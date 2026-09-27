# 📥 Stage - Magic: The Gathering

<div align="center">

![Shuko - Artifact Equipment](https://repositorio.sbrauble.com/arquivos/in/magic/199/5f424341cf64f-dzpnm7-nvfmuq-140f6969d5213fd0ece03148e62e461e.jpg)

*"A simple tool in the hands of a master becomes a deadly weapon."* - Shuko, Betrayers of Kamigawa

</div>

## 📋 Visão Geral

Camada **Stage**: coleta dados brutos da Scryfall e persiste em Parquet no S3, sem
nenhuma regra de negócio (isso é Bronze/Silver). Responsabilidade única: garantir que
o dado foi obtido corretamente, gravado de forma íntegra, idempotente e reprocessável,
com controle de execução auditável.

## 🔗 Fonte de dados: Scryfall API

A [magicthegathering.io](https://docs.magicthegathering.io) foi descontinuada como
fonte (issues #121/#123/#127/#128/#129) — os três notebooks usam exclusivamente a
[Scryfall API](https://scryfall.com/docs/api):

- **`cards.py`** e **`card_prices.py`**: [Bulk Data](https://scryfall.com/docs/api/bulk-data)
  (`default_cards` / `oracle_cards`) — 1 request pro índice + 1 download do `.jsonl.gz`
  inteiro, filtrado em memória. Sem paginação, sem 1 request por carta/coleção.
- **`sets.py`**: `GET /sets` — devolve o catálogo inteiro em 1 request (`has_more: false`),
  sem paginação. Além dos campos herdados da magicthegathering.io, captura também
  `card_count`, `parent_set_code`, `block` e `icon_svg_uri` — nativos da Scryfall,
  sem equivalente na fonte antiga, antes simplesmente não coletados.
- **`symbology.py`**: `GET /symbology` — catálogo inteiro de símbolos de carta/mana
  em 1 request (`has_more: false`), sem paginação. Tabela de referência estática (84
  símbolos): sem filtro temporal, idempotência só por arquivo do dia. Hoje a Silver
  decodifica símbolo de mana com `regexp_replace` hardcoded (`{W}`→branco, `{U}`→azul
  etc., em `TB_FATO_CARTAS`), cobrindo só os símbolos de cor básicos — perde
  híbrido/Phyrexian. `symbology.py` traz a fonte oficial pra esse mapeamento.
- **`rulings.py`**: [Bulk Data](https://scryfall.com/docs/api/bulk-data) (`rulings`)
  — mesmo padrão de `cards.py`/`card_prices.py` (1 request pro índice + 1
  download do `.jsonl.gz` inteiro). Sem filtro temporal: diferente de preço/impressão,
  uma ruling antiga sobre uma carta antiga continua válida hoje — não expira pelo
  calendário. Catálogo pequeno (~79k linhas, ~5MB comprimido), sem necessidade de
  recorte. Referencia a carta por `oracle_id` (não por impressão) — a Stage não hoje
  captura `oracle_id` em `cards.py`, então esse join fica pendente pra Bronze/Silver
  até que `oracle_id` seja adicionado a `cards.py` também.
- **`migrations.py`**: `GET /migrations` — único endpoint da Stage que pagina de
  verdade (`has_more`/`next_page`, ~350 registros por página), diferente do padrão
  "1 request só" usado no resto da camada. Histórico de reconciliação de
  `scryfall_id` (`migration_strategy`: `delete` remove um ID, `merge` aponta
  `old_scryfall_id` → `new_scryfall_id`), referenciado por `metadata.oracle_id`/
  `metadata.set_code`/`metadata.collector_number` (flattenados em colunas
  `metadata_*`, mesmo padrão do `booster` explodido em `sets.py`). Sem filtro
  temporal: cortar por data quebraria a rastreabilidade de IDs antigos que
  Bronze/Silver podem precisar resolver, mesmo tratando de cartas antigas.

A Scryfall não expõe CDC nem um cursor de "o que mudou desde X" para cards/sets — só
`released_at`/`digital` nos sets. Por isso **não existe incrementalidade "de verdade"**
além do filtro temporal por `years_back`: cada run relê o catálogo inteiro da Scryfall
e decide o que gravar via idempotência de arquivo (abaixo), não via delta da API.

## 📁 Notebooks

| Notebook | Fonte | Grão | Observação |
|---|---|---|---|
| `cards.py` | `bulk-data/default_cards` | 1 linha por impressão (set+número) | Filtra por `set_codes` dentro da janela `years_back` (via `sets`) |
| `sets.py` | `GET /sets` | 1 linha por coleção | Filtra por `releaseDate >= cutoff` |
| `card_prices.py` | `bulk-data/oracle_cards` | 1 linha por carta (nome, deduplicado por Oracle ID) | Filtra por `releaseDate >= cutoff`, independente de `cards.py` |
| `symbology.py` | `GET /symbology` | 1 linha por símbolo | Catálogo estático, sem filtro temporal |
| `rulings.py` | `bulk-data/rulings` | 1 linha por ruling (referenciada por `oracle_id`) | Sem filtro temporal, catálogo inteiro (~79k linhas) |
| `migrations.py` | `GET /migrations` | 1 linha por migração de ID | Único endpoint paginado da Stage, sem filtro temporal |

Os notebooks são independentes entre si — nenhum lê o S3 gravado por outro. No
job `MTG_STAGE` (`.github/DAGs/stage.yml`) as 6 tasks rodam em paralelo, sem
`depends_on` entre elas. Antes eram limitadas a 3 simultâneas via `depends_on`
em pares, só por throttling de concorrência (o cluster de 1 worker fixo já
deu OOM rodando as 6 juntas) — trocado por autoscale (1→2 workers) no cluster
do job, que dá folga pro pico das 6 tasks em paralelo sem exigir dependência
manual no yml nem manter o custo de 2 workers o tempo todo.

`card_prices.py` já leu os arquivos de `cards.parquet` pra descobrir quais cartas
precisava precificar (criando uma dependência de execução entre os dois); hoje ele
grava seu próprio snapshot do catálogo `oracle_cards` filtrado pela mesma janela
`years_back`, e o join "esse preço pertence a essas impressões" (1 preço → N
impressões, já que `oracle_cards` é deduplicado) fica pra Bronze/Silver.

`ingestion_utils.py` concentra o que é comum aos notebooks (`%run ./ingestion_utils`):
`get_secret`, `setup_s3_storage`, `http_get_with_retry`, `save_to_parquet`,
`get_scryfall_set_codes_since`, `start_run`/`finish_run`, `run_stage_ingestion`
(padroniza o wrapper `start_run` → `try`/ingest → `finish_run` repetido nos 6
notebooks — cada um só chama `run_stage_ingestion(table_name, endpoint,
ingest_fn, S3_BASE_PATH)` e monta seu próprio relatório com o DataFrame
devolvido).

## 📄 Documentação de negócio (o que é cada tabela/coluna)

A Stage grava o dado exatamente como recebido da Scryfall, sem renomear nem
transformar coluna nenhuma (ver [Imutabilidade](#-imutabilidade) abaixo) - é o
mesmo schema que a Bronze lê e persiste no Unity Catalog. Por isso o
significado de negócio de cada tabela e cada coluna (o que é, pra que serve,
que informação você tira dela) é documentado uma única vez, na Bronze, em vez
de duplicado aqui:

- **Por tabela:** [`cards`](<../02 - Bronze/Documentação/cards/README.md>),
  [`sets`](<../02 - Bronze/Documentação/sets/README.md>),
  [`card_prices`](<../02 - Bronze/Documentação/card_prices/README.md>),
  [`symbology`](<../02 - Bronze/Documentação/symbology/README.md>),
  [`rulings`](<../02 - Bronze/Documentação/rulings/README.md>),
  [`migrations`](<../02 - Bronze/Documentação/migrations/README.md>).
- **Fonte única (Python):** [`../02 - Bronze/Dev/bronze_column_docs.py`](<../02 - Bronze/Dev/bronze_column_docs.py>).

Diferença nas colunas técnicas: a Stage grava `ingestion_timestamp`, `source`
e `endpoint` (mesmo significado descrito nos links acima). `source_file`,
`bronze_run_id` e `bronze_ingestion_timestamp` **não existem na Stage** - são
adicionadas só a partir da Bronze (controle de idempotência/execução daquela
camada). A Stage também não tem tabela no Unity Catalog (grava só Parquet no
S3), então não há `COMMENT ON TABLE`/`ALTER COLUMN...COMMENT` aplicável aqui -
a documentação de negócio da Stage é só este Markdown.

## ⚙️ Segredos (scope `mtg-pipeline`)

```
scryfall_api_url     # URL base da Scryfall API
s3_bucket             # Bucket S3
s3_stage_prefix       # Prefixo do staging (padrão: "stage")
years_back            # Janela temporal em anos (padrão: 5)
max_retries           # Tentativas de retry por request HTTP (padrão: 3)
```

## 🗂️ Estrutura no S3

```
s3://{bucket}/{stage_prefix}/
├── cards/
│   └── {year}_{month}_{day}_cards.parquet   # dia da execução no nome - evita pular o mês
├── sets/                                    # inteiro a partir do 2º run do mesmo mês (AUD-04)
│   └── {year}_{month}_{day}_sets.parquet
├── card_prices/
│   └── {year}_{month}_{day}_card_prices.parquet   # mesma granularidade diária dos outros dois
├── symbology/
│   └── {year}_{month}_{day}_symbology.parquet     # partição por data de ingestão (sem coluna de data própria)
├── rulings/
│   └── {year}_{month}_{day}_rulings.parquet       # partição por data de ingestão (sem coluna de data própria)
├── migrations/
│   └── {year}_{month}_{day}_migrations.parquet    # partição por data de ingestão (sem filtro/coluna de data própria)
└── _control/
    ├── cards/{run_id}.json
    ├── sets/{run_id}.json
    ├── card_prices/{run_id}.json
    ├── symbology/{run_id}.json
    ├── rulings/{run_id}.json
    └── migrations/{run_id}.json
```

Cada tabela tem sua própria pasta - antes os 6 arquivos viviam juntos num diretório flat, distinguidos só pelo sufixo do nome.

## 🔁 Idempotência e controle de execução

- **Nome de arquivo determinístico** por tabela/dia — se o arquivo já existe, a run
  pula essa partição (`files_skipped`) em vez de sobrescrever. Os três notebooks
  (`cards`, `sets`, `card_prices`) usam o mesmo esquema via `save_to_parquet()`.
- **`start_run()`/`finish_run()`** (`ingestion_utils.py`) gravam um JSON por execução em
  `_control/{table}/{run_id}.json` com: `run_id`, `endpoint`, `params`, início/fim,
  duração, `files_written`/`files_skipped`/`records_written`, `status`
  (`RUNNING`/`SUCCESS`/`FAILED`) e `error`. É observabilidade — se o próprio
  write do controle falhar, o notebook só avisa e segue (não mascara o resultado real).
- Uma run nova **nunca apaga** dado de uma run anterior bem-sucedida — falha vira
  `FAILED`/`PARTIAL` registrado no controle, sem tocar nos arquivos já gravados.
  Reprocessar é rodar o notebook de novo (idempotente por arquivo).

## 🛡️ Erros e retry

`http_get_with_retry()` cobre todo request HTTP dos três notebooks: retry com backoff
em 429 e 5xx, timeout/erro de conexão também tenta de novo; 4xx (exceto 429) falha
direto, sem retry (erro do cliente não muda tentando de novo).

## 🧊 Imutabilidade

O dado gravado é o dado recebido da Scryfall (mapeado 1:1 pros nomes de coluna
esperados por Bronze/Silver, sem TRIM/normalização de acento/dedup/regra de negócio).
Campos exclusivos da extinta magicthegathering.io sem equivalente na Scryfall
(`border`, `mkm_id`, `gathererCode`, etc.) ficam `None` — a coluna existe, só não tem
dado de origem.

## ⚠️ Fora de escopo da Stage

Nome padronizado, dedup de negócio, PK/FK, modelagem dimensional, tratamento de NULL
para consumo analítico — isso é Bronze/Silver. A Stage só garante que o dado chegou
completo, íntegro e rastreável no S3.
