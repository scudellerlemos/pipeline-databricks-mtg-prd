# Documentação da Camada Bronze

## Visão geral

A Bronze faz **EL puro** (Extract & Load): lê os arquivos Parquet gravados
pela Stage em S3 e grava um Delta append-only por tabela no Unity Catalog,
preservando o schema de origem 1:1. Nenhuma regra de negócio, renomeação de
coluna, deduplicação por chave de negócio ou MERGE/upsert acontece aqui -
isso é responsabilidade da Silver.

Toda a lógica compartilhada vive em
[`../Dev/bronze_utils.py`](../Dev/bronze_utils.py). Cada notebook por tabela
só declara a configuração (nome da tabela, nome da tabela de origem na
Stage) e chama `run_bronze_ingestion(...)`.

## Tabelas

Nomeadas sem prefixo `TB_BRONZE_` - já estão dentro do schema `bronze` no
Unity Catalog (`{catalog}.bronze.cards`, etc.), o prefixo seria redundante.

| Tabela Bronze | Tabela de origem (Stage) | Notebook | Schema (fonte) | Detalhes |
|---|---|---|---|---|
| `cards` | `cards` | [`cards.py`](../Dev/cards.py) | [`src/01 - Ingestion/cards.py`](<../../01 - Ingestion/cards.py>) | [`cards/README.md`](./cards/README.md) |
| `sets` | `sets` | [`sets.py`](../Dev/sets.py) | [`src/01 - Ingestion/sets.py`](<../../01 - Ingestion/sets.py>) | [`sets/README.md`](./sets/README.md) |
| `card_prices` | `card_prices` | [`card_prices.py`](../Dev/card_prices.py) | [`src/01 - Ingestion/card_prices.py`](<../../01 - Ingestion/card_prices.py>) | [`card_prices/README.md`](./card_prices/README.md) |
| `symbology` | `symbology` | [`symbology.py`](../Dev/symbology.py) | [`src/01 - Ingestion/symbology.py`](<../../01 - Ingestion/symbology.py>) | [`symbology/README.md`](./symbology/README.md) |
| `rulings` | `rulings` | [`rulings.py`](../Dev/rulings.py) | [`src/01 - Ingestion/rulings.py`](<../../01 - Ingestion/rulings.py>) | [`rulings/README.md`](./rulings/README.md) |
| `migrations` | `migrations` | [`migrations.py`](../Dev/migrations.py) | [`src/01 - Ingestion/migrations.py`](<../../01 - Ingestion/migrations.py>) | [`migrations/README.md`](./migrations/README.md) |

Todas as 6 tabelas têm um `README.md` próprio com a descrição de negócio da
tabela (o que é, pra que serve) e a lista completa de colunas específicas
dela. Os mesmos textos são a fonte usada para comentar a tabela/coluna no
Unity Catalog (`DESCRIBE TABLE EXTENDED {tabela}` mostra o mesmo conteúdo) -
ver [`../Dev/bronze_column_docs.py`](../Dev/bronze_column_docs.py), fonte
única compartilhada entre este README e o comentário aplicado no catálogo.

## Colunas técnicas comuns

Além das colunas próprias de cada fonte, toda tabela Bronze carrega estas
colunas técnicas (não repetidas nos READMEs por tabela). `source_file`,
`bronze_run_id` e `bronze_ingestion_timestamp` são adicionadas por esta
camada; `ingestion_timestamp`, `source` e `endpoint` já vêm gravadas pela
Stage (comuns a toda fonte, preservadas 1:1 como o resto do dado):

| Coluna | Adicionada por | Descrição |
|---|---|---|
| `ingestion_timestamp` | Stage | Timestamp em que a Stage coletou o registro da fonte - distinto do `bronze_ingestion_timestamp`. |
| `source` | Stage | Nome da fonte de dados de origem (ex.: `scryfall`). Em `rulings`, esta chave é sobrescrita com um significado de negócio diferente - ver [`rulings/README.md`](./rulings/README.md). |
| `endpoint` | Stage | Endpoint/URL da API de origem que devolveu este registro. |
| `source_file` | Bronze | Caminho completo do arquivo Parquet de origem na Stage (`_metadata.file_path`) - é a chave de idempotência: um arquivo só é lido de novo se seu `source_file` ainda não existir na tabela Bronze. |
| `bronze_run_id` | Bronze | Id da execução da Bronze que gravou a linha (controle de execução). |
| `bronze_ingestion_timestamp` | Bronze | Timestamp em que a Bronze processou o registro. |

## Carga inicial vs. incremental

Não há distinção de código entre a 1ª carga e as execuções seguintes: o
`write.format("delta").mode("append")` cria a tabela Delta automaticamente
se ela não existir. Toda execução segue o mesmo fluxo:

1. Lista os arquivos Parquet da Stage para a tabela (`*_{stage_table_name}.parquet`).
2. Descobre quais já foram carregados (via `source_file` distinto já presente na Bronze).
3. Lê só os arquivos novos, adiciona as 3 colunas técnicas.
4. Append no Delta com `mergeSchema=true` (evolução aditiva de schema).
5. Garante a tabela no Unity Catalog (`CREATE TABLE IF NOT EXISTS ... LOCATION`, nunca `DROP`/`ALTER` automático).
6. Grava o controle de execução em `{s3_bronze_path}/_control/{tabela}/{run_id}.json`.

Se não há arquivo novo (ex.: 2ª execução no mesmo dia, já que a Stage não
gera arquivo novo nesse caso), a run fecha como `SUCCESS` sem escrever nada -
idempotência por identidade de arquivo, não por `SELECT DISTINCT` em dado de
negócio.

## Histórico preservado (sem deduplicação)

A mesma carta/preço/regra pode aparecer em mais de um arquivo/execução da
Stage ao longo do tempo (ex.: preço de uma carta em dois dias diferentes).
A Bronze preserva as duas linhas - não há `dropDuplicates` nem `MERGE` por
chave de negócio. Decidir o que é "estado atual" vs. "histórico" é трabalho
da Silver.

## Particionamento

Nenhuma tabela Bronze é particionada. O volume atual não justifica, e
particionar preventivamente sem necessidade real é a complexidade que este
redesenho removeu (as tabelas antigas particionavam por `RELEASE_YEAR`/
`RELEASE_MONTH` derivados de um JOIN com `sets` dentro da Bronze - regra de
negócio que não deveria estar aqui).
