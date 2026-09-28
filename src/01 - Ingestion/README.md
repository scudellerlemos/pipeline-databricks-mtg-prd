# Stage - Magic: The Gathering

<div align="center">

![Shuko - Artifact Equipment](https://repositorio.sbrauble.com/arquivos/in/magic/199/5f424341cf64f-dzpnm7-nvfmuq-140f6969d5213fd0ece03148e62e461e.jpg)

*"A simple tool in the hands of a master becomes a deadly weapon."* - Shuko, Betrayers of Kamigawa

</div>

## Visão Geral

Camada **Stage**: coleta dados brutos da Scryfall e persiste em Parquet no S3, sem
nenhuma regra de negócio (isso é Bronze/Silver). Responsabilidade única: garantir que
o dado foi obtido corretamente, gravado de forma íntegra, idempotente e reprocessável,
com controle de execução auditável.

## Fonte de dados: Scryfall API

Os cinco notebooks usam exclusivamente a
[Scryfall API](https://scryfall.com/docs/api):

- **`cards.py`** e **`card_prices.py`**: [Bulk Data](https://scryfall.com/docs/api/bulk-data)
  (`default_cards`, ambos) — 1 request pro índice + 1 download do `.jsonl.gz`
  inteiro, filtrado em memória. Sem paginação, sem 1 request por carta/coleção.
- **`sets.py`**: `GET /sets` — devolve o catálogo inteiro em 1 request (`has_more: false`),
  sem paginação. Captura também `card_count`, `parent_set_code`, `block` e
  `icon_svg_uri` (campos nativos da Scryfall).
- **`rulings.py`**: [Bulk Data](https://scryfall.com/docs/api/bulk-data) (`rulings`)
  — mesmo padrão de `cards.py`/`card_prices.py` (1 request pro índice + 1
  download do `.jsonl.gz` inteiro). Sem filtro temporal: diferente de preço/impressão,
  uma ruling antiga sobre uma carta antiga continua válida hoje — não expira pelo
  calendário. Catálogo pequeno (~79k linhas, ~5MB comprimido), sem necessidade de
  recorte. Referencia a carta por `oracle_id` (não por impressão), também
  capturado em `cards.py`; o join fica pra Gold.
- **`migrations.py`**: `GET /migrations` — único endpoint da Stage que pagina de
  verdade (`has_more`/`next_page`, ~350 registros por página), diferente do padrão
  "1 request só" usado no resto da camada. Histórico de reconciliação de
  `scryfall_id` (`migration_strategy`: `delete` remove um ID, `merge` aponta
  `old_scryfall_id` → `new_scryfall_id`), referenciado por `metadata.oracle_id`/
  `metadata.set_code`/`metadata.collector_number` (flattenados em colunas
  `metadata_*`). Sem filtro
  temporal: cortar por data quebraria a rastreabilidade de IDs antigos que
  Bronze/Silver podem precisar resolver, mesmo tratando de cartas antigas.

**Todas as tabelas são snapshot**: cada run relê o catálogo inteiro da Scryfall
(recortado por `years_back` onde se aplica) e decide o que gravar via idempotência
de arquivo (abaixo), não via delta da API. O que a API oferece (conferido em 09/2026):

- **cards/card_prices**: sem campo de "atualizado em" (só `released_at` e
  `image_updated_at`). Dá pra buscar impressões novas por data de lançamento, mas
  legalidade, errata e preço mudam em carta antiga sem carimbo - incremental perderia isso.
- **sets**: sem campo de atualização; 1 request, não precisa.
- **rulings**: tem `published_at`, mas o bulk é um arquivo único (~5MB) e ruling
  editada/removida não seria vista.
- **migrations**: o único que permite incremental - a paginação vem ordenada por
  `performed_at` decrescente, daria pra parar no último carregado. Ficou snapshot
  mesmo assim: são poucas páginas por mês, e um watermark traria estado a manter e um
  reprocesso diferente do resto da camada.

Os bulks são regenerados diariamente pela Scryfall: a frequência mensal é escolha
do pipeline, não limite da fonte.

**Escopo fechado (decisão de produto, 09/2026):**

- **Frequência mensal**, inclusive para preço - 1 ponto por impressão por mês.
- **Janela de 5 anos** (`years_back`) por data de lançamento da coleção. Fica de fora
  ~43% das cartas (as sem impressão na janela) e as impressões antigas das cartas que
  estão dentro - de propósito.

## Notebooks

| Notebook | Fonte | Grão | Observação |
|---|---|---|---|
| `cards.py` | `bulk-data/default_cards` | 1 linha por impressão (set+número) | Filtra por `codigos_colecoes` dentro da janela `years_back` (via `sets`) |
| `sets.py` | `GET /sets` | 1 linha por coleção | Filtra por `releaseDate >= cutoff` |
| `card_prices.py` | `bulk-data/default_cards` | 1 linha por impressão (`id`) | Filtra por `releaseDate >= cutoff`, independente de `cards.py` |
| `rulings.py` | `bulk-data/rulings` | 1 linha por ruling (referenciada por `oracle_id`) | Sem filtro temporal, catálogo inteiro (~79k linhas) |
| `migrations.py` | `GET /migrations` | 1 linha por migração de ID | Único endpoint paginado da Stage, sem filtro temporal |

Os notebooks são independentes entre si — nenhum lê o S3 gravado por outro. No
job `MTG_STAGE` (`.github/DAGs/stage.yml`) as 5 tasks rodam em paralelo, sem
`depends_on` entre elas, num cluster single-node m5d.2xlarge (8 vCPU / 32 GB).
Antes era driver m5d.large + 1→2 workers, e o driver saturava (swap de até 85%)
com as tasks em paralelo.

`card_prices.py` não depende de `cards.py`: grava seu próprio snapshot de
`default_cards` (1 linha por impressão, com `id`) filtrado pela mesma janela
`years_back`, e o join com `cards` é 1:1 por `id` e fica pra Gold.

`ingestion_utils.py` concentra o que é comum aos notebooks (`%run ./ingestion_utils`):
`obter_segredo`, `configurar_armazenamento_s3`, `obter_http_com_retentativa`, `salvar_em_parquet`,
`obter_codigos_colecoes_scryfall_desde`, `iniciar_execucao`/`finalizar_execucao`, `executar_ingestao_stage`
(padroniza o wrapper `iniciar_execucao` → `try`/ingest → `finalizar_execucao` repetido nos 6
notebooks — cada um só chama `executar_ingestao_stage(nome_tabela, endpoint,
funcao_ingestao, CAMINHO_S3_STAGE)` e monta seu próprio relatório com o DataFrame
devolvido).

## Documentação de negócio (o que é cada tabela/coluna)

A Stage grava o dado como recebido da Scryfall, só mapeado 1:1 para os nomes
de coluna esperados por Bronze/Silver (ex.: `type_line`→`type`,
`released_at`→`releaseDate`), com campos compostos serializados em JSON e sem
regra de negócio (ver [Imutabilidade](#imutabilidade) abaixo) - é o
mesmo schema que a Bronze lê e persiste no Unity Catalog. Por isso o
significado de negócio de cada tabela e cada coluna (o que é, pra que serve,
que informação você tira dela) é documentado uma única vez, na Bronze, em vez
de duplicado aqui:

- **Por tabela:** [`cards`](<../02 - Bronze/Documentação/cards/README.md>),
  [`sets`](<../02 - Bronze/Documentação/sets/README.md>),
  [`card_prices`](<../02 - Bronze/Documentação/card_prices/README.md>),
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

## Segredos (scope `mtg-pipeline`)

```
scryfall_api_url     # URL base da Scryfall API
s3_bucket             # Bucket S3
s3_stage_prefix       # Prefixo do staging (padrão: "stage")
years_back            # Janela temporal em anos (padrão: 5)
max_retries           # Tentativas de retry por request HTTP (padrão: 3)
```

## Estrutura no S3

```
s3://{bucket}/{stage_prefix}/
├── cards/
│   └── {year}_{month}_{YYYYMMDD}_cards.parquet   # data completa da execução no nome
├── sets/
│   └── {year}_{month}_{YYYYMMDD}_sets.parquet          # ano/mês do releaseDate + data da execução: 1 arquivo por mês de lançamento
├── card_prices/
│   └── {year}_{month}_{YYYYMMDD}_card_prices.parquet   # idem sets (releaseDate da impressão)
├── rulings/
│   └── {year}_{month}_{YYYYMMDD}_rulings.parquet       # partição por data de ingestão (published_at não é usado)
├── migrations/
│   └── {year}_{month}_{YYYYMMDD}_migrations.parquet    # partição por data de ingestão (performed_at não é usado)
└── _control/
    ├── cards/{run_id}.json
    ├── sets/{run_id}.json
    ├── card_prices/{run_id}.json
    ├── rulings/{run_id}.json
    └── migrations/{run_id}.json
```

## Idempotência e controle de execução

- **Nome de arquivo determinístico** — se o arquivo já existe, a run
  pula essa partição (`files_skipped`) em vez de sobrescrever. Os cinco notebooks
  usam o mesmo esquema via `salvar_em_parquet()` (`nome_arquivo_parquet()`).
  `{YYYYMMDD}` é a data completa da execução; `{year}_{month}` é a partição -
  a data da execução, exceto em `sets` e `card_prices`, onde vem do
  `releaseDate`. Com a data completa no nome, runs em meses diferentes nunca
  colidem. Arquivos antigos, só com o dia no nome, continuam válidos: a Bronze
  controla por caminho.
- **`iniciar_execucao()`/`finalizar_execucao()`** (`ingestion_utils.py`): o `iniciar_execucao` monta o registro em memória e o `finalizar_execucao` grava um JSON por execução em
  `_control/{table}/{run_id}.json` com: `run_id`, `endpoint`, `params`, início/fim,
  duração, `files_written`/`files_skipped`/`records_written`, `status`
  (`SUCCESS`/`FAILED`) e `error`. É observabilidade — se o próprio
  write do controle falhar, o notebook só avisa e segue (não mascara o resultado real).
- Uma run nova **nunca apaga** dado de uma run anterior bem-sucedida — falha vira
  `FAILED` registrado no controle, sem tocar nos arquivos já gravados.
  Reprocessar é rodar o notebook de novo (idempotente por arquivo).

## Erros e retry

`obter_http_com_retentativa()` cobre todo request HTTP dos cinco notebooks: retry com backoff
em 429 e 5xx, timeout/erro de conexão também tenta de novo; 4xx (exceto 429) falha
direto, sem retry (erro do cliente não muda tentando de novo).

## Imutabilidade

O dado gravado é o dado recebido da Scryfall (mapeado 1:1 pros nomes de coluna
esperados por Bronze/Silver, sem TRIM/normalização de acento/dedup/regra de negócio).
Campos exclusivos da extinta magicthegathering.io sem equivalente na Scryfall
(`border`, `mkm_id`, `gathererCode`, etc.) ficam `None` — a coluna existe, só não tem
dado de origem.

## Fora de escopo da Stage

Nome padronizado, dedup de negócio, PK/FK, modelagem dimensional, tratamento de NULL
para consumo analítico — isso é Bronze/Silver. A Stage só garante que o dado chegou
completo, íntegro e rastreável no S3.
