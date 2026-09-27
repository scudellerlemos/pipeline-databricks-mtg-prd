# Camada Bronze - Magic: The Gathering

## Visão geral

A camada Bronze faz **EL (Extract & Load)**: lê os arquivos Parquet gravados
pela Stage em S3 e grava um Delta append-only por tabela no Unity Catalog,
preservando o schema de origem 1:1. Nenhuma regra de negócio, renomeação de
coluna, deduplicação por chave de negócio ou MERGE/upsert acontece aqui -
isso é responsabilidade da Silver.

Documentação completa (arquitetura, idempotência, controle de execução,
schema, particionamento): [`Documentação/README.md`](./Documentação/README.md).

## Tabelas

6 tabelas, uma por origem da Stage: `cards`, `sets`, `card_prices`,
`symbology`, `rulings`, `migrations` - sem prefixo `TB_BRONZE_`, já que estão
dentro do schema `bronze` no Unity Catalog (`{catalog}.bronze.cards`, etc.).
Cada uma tem um notebook em [`Dev/`](./Dev) (mesmo nome da tabela) que só
configura os parâmetros e chama `run_bronze_ingestion(...)`, definida em
[`Dev/bronze_utils.py`](./Dev/bronze_utils.py).

## Como executar

Cada notebook é independente e idempotente - pode ser reexecutado a
qualquer momento sem duplicar dados (só processa arquivos novos da Stage):

```
cards.py
sets.py
card_prices.py
symbology.py
rulings.py
migrations.py
```

Não há ordem de dependência entre eles (cada um lê só sua própria origem na
Stage). Vivem no job `MTG_BRONZE` (`.github/DAGs/bronze.yml`), sem
depends_on entre si nem com a Stage - o job `MTG_PIPELINE`
(`.github/DAGs/pipeline.yml`) só aciona a Bronze inteira depois que a Stage
inteira (`MTG_STAGE`) termina, via `run_job_task`.

## Segredos necessários (scope `mtg-pipeline`)

```
catalog_name       # catálogo Unity Catalog
s3_bucket          # bucket S3
s3_stage_prefix    # prefixo da camada Stage
s3_bronze_prefix   # prefixo da camada Bronze
```
