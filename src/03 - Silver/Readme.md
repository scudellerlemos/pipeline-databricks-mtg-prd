# Camada Silver - Magic: The Gathering

<div align="center">

![RED XIII - Proud Warrior](https://img.mypcards.com/img/1/137/magic_ddajvc_001/magic_ddajvc_001_en.jpg)


*"Na forja alquímica da Silver, dados brutos ganham forma, clareza e poder analítico."* - Jace Beleren, Magic: The Gathering

</div>

## Visão Geral

Esta pasta contém os notebooks responsáveis pela **camada Silver** do pipeline de dados do Magic: The Gathering. A camada Silver realiza o processo **TL (Transform & Load)**, limpando, padronizando e enriquecendo os dados da Bronze.

## Objetivo

Transformar dados estruturados da Bronze em dados limpos, padronizados e enriquecidos na Silver (Unity Catalog/Delta), garantindo:
- **Transform**: Limpeza, padronização e enriquecimento
- **Load**: Carregamento incremental com merge por chave
- **Governança**: Controle e rastreabilidade via Unity Catalog
- **Performance**: Otimização com Delta Lake
- **Prontidão Analítica**: Dados prontos para análises e camadas superiores

## Processo TL (Transform & Load)

### Transform - Limpeza e Enriquecimento (SQL)
As regras de negócio de cada notebook são escritas em SQL puro, num `spark.sql()` com CTEs encadeadas (ex.: `_renomeado` -> `_limpo` -> `_sem_delimitador` em `TB_FATO_CARTAS`). Depois do SQL, `normalizar_valores()` (trim, sem acento via UDF, Title Case, espaço vira `_`; NULO, `''` e `'NA'` passam intactos) padroniza as colunas categóricas, e `TB_MOV_MIGRACOES_CARTAS` resolve a cadeia de ids em Python. As colunas saem em PT-BR, 100% MAIÚSCULAS:
```python
spark.sql(r"""
    WITH _renomeado AS (
        SELECT id AS ID_CARTA, name AS NME_CARTA, manaCost AS DESC_CUSTO_MANA,
               cmc AS QTD_CUSTO_MANA, colors AS COD_CORES, ...
        FROM ...
    ),
    ...
    SELECT *, CASE ... END AS NME_CATEGORIA_COR
    FROM _sem_delimitador
""")
```

### Load - Carregamento na Silver
A primeira carga (Delta inexistente) é `overwrite`. As seguintes usam o merge builder do Delta (`DeltaTable.merge()` com `withSchemaEvolution()`), condição nula-segura `silver.<chave> <=> novo.<chave>` (`salvar_na_silver` em `silver_utils.py`):
```python
(
    DeltaTable.forPath(spark, caminho_delta).alias("silver")
    .merge(df_final.alias("novo"), condicao_merge)
    .withSchemaEvolution()
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```
Antes do merge há dedup por chave: `row_number()` (ordem por `coluna_ordenacao` desc, nulos por último) quando o notebook informa `coluna_ordenacao`; senão `dropDuplicates`.

## Estrutura dos Notebooks

São 7 notebooks, orquestrados por `.github/DAGs/silver.yml` (detalhe de cada tabela em [`Documentação/Readme.md`](./Documentação/Readme.md)):

| Notebook | Chave | `coluna_ordenacao` (dedup) | Partição |
|---|---|---|---|
| `TB_FATO_CARTAS.py` | `ID_CARTA` | `DT_INGESTAO` | `ANO_INGESTAO`/`MES_INGESTAO` |
| `TB_DIM_COLECOES.py` | `COD_COLECAO` | `DT_INGESTAO` | `ANO_LANCAMENTO`/`MES_LANCAMENTO` |
| `TB_FATO_PRECOS_CARTAS.py` | `ID_CARTA` + `DT_INGESTAO` | — (dropDuplicates) | `ANO_INGESTAO`/`MES_INGESTAO` |
| `TB_MOV_MIGRACOES_CARTAS.py` | `ID_MIGRACAO` | `DT_INGESTAO` | `ANO_EXECUCAO`/`MES_EXECUCAO` |
| `TB_DOM_SIMBOLOS.py` | `COD_SIMBOLO` | `DT_INGESTAO` | sem partição |
| `TB_FATO_ESCLARECIMENTOS_CARTAS.py` | `ID_ESCLARECIMENTO` | `DT_INGESTAO` | `ANO_PUBLICACAO`/`MES_PUBLICACAO` |
| `TB_PONTE_CARTA_SIMBOLOS.py` | `ID_CARTA` + `NUM_ORDEM_SIMBOLO` | — (dropDuplicates) | sem partição |

`TB_PONTE_CARTA_SIMBOLOS` depende de `TB_FATO_CARTAS` e `TB_DOM_SIMBOLOS` no DAG; as demais rodam independentes.

## Configurações Necessárias

### Configuração (env var > secret > default)
Cada valor é resolvido nesta ordem (`base_utils.py`): env var `MTG_<NOME>` (injetada no deploy) > secret no scope `mtg-pipeline` > default do código.
```python
catalog_name           # MTG_CATALOG_NAME; default mtg_dev (em prd: mtg_prod)
s3_bucket              # MTG_S3_BUCKET
s3_silver_prefix       # MTG_S3_SILVER_PREFIX
```
Com `MTG_ENVIRONMENT=production`, resolver o catálogo para `mtg_dev` é bloqueado (erro).

### Estrutura Unity Catalog
```
{catalog_name}/
└── silver/
    ├── TB_FATO_CARTAS
    ├── TB_DIM_COLECOES
    ├── TB_FATO_PRECOS_CARTAS
    ├── TB_MOV_MIGRACOES_CARTAS
    ├── TB_DOM_SIMBOLOS
    ├── TB_FATO_ESCLARECIMENTOS_CARTAS
    └── TB_PONTE_CARTA_SIMBOLOS
```

## Fluxo de Execução

### 1. **Setup Unity Catalog**
- Criação do catálogo e schema
- Verificação de estrutura

### 2. **Transformação Silver**
- Limpeza, padronização e enriquecimento
- Aplicação de regras de negócio
- Validação de dados

### 3. **Preparação para Merge**
- Remoção de duplicatas
- Compatibilidade de schema
- Preparação de chaves de merge

### 4. **Load Incremental**
- Verificação de tabela existente
- Merge incremental com Delta Lake
- Criação/atualização da tabela Unity Catalog

### 5. **Validação e Monitoramento**
- Contagem de registros
- Verificação de integridade
- Logs de processamento

## Controle de Qualidade

### Validações Implementadas
- **Verificação de DataFrame nulo** (None; DataFrame vazio não é bloqueado)
- **Remoção de duplicatas**
- **Compatibilidade de schema**
- **Merge incremental**
- **Padronização e enriquecimento**

### Tratamento de Erros e Recuperação
- **Verificação de existência**: Antes de criar/atualizar tabelas
- **Upsert por chave**: linha com chave existente é sobrescrita inteira (`whenMatchedUpdateAll`); só linhas fora do lote atual ficam intocadas
- **Atomicidade**: MERGE do Delta é transacional; falha não deixa escrita parcial
- **Logs detalhados**: Para debugging e auditoria

### Logs e Monitoramento
- **Contagem de registros**: Antes e depois do processamento
- **Schema**: diferença de colunas entre origem e destino é avisada no log

## Características dos Dados

### Cartas e Coleções
- **Filtro**: só `TB_FATO_CARTAS` filtra (últimos 60 meses de `DT_INGESTAO`); `TB_DIM_COLECOES` não tem filtro na Silver, mas a Stage já restringe a coleções com `releaseDate` >= 1º de janeiro de (ano atual − `years_back`, padrão 5)
- **Merge**: Incremental por `ID_CARTA` / `COD_COLECAO`
- **Particionamento**: `ANO_INGESTAO`/`MES_INGESTAO` (cartas) e `ANO_LANCAMENTO`/`MES_LANCAMENTO` (coleções)
- **Histórico**: Mantido no Delta Lake
- **Tipo**: Creature/Spell/Artifact (dinâmicos)

### Dados de Preços (`TB_FATO_PRECOS_CARTAS`)
- **Grão**: uma cotação por impressão (`ID_CARTA`) por coleta (`DT_INGESTAO`)
- **Filtro**: nenhum na Silver (não depende de `TB_FATO_CARTAS`); a Stage já restringe a impressões com `releaseDate` >= 1º de janeiro de (ano atual − `years_back`, padrão 5)
- **Merge**: Incremental por `ID_CARTA` + `DT_INGESTAO`
- **Particionamento**: `ANO_INGESTAO`/`MES_INGESTAO`
- **Frequência**: Atualização frequente (preços dinâmicos)
- **Fonte**: Scryfall (todas as tabelas da Silver vêm da Scryfall)
- **Tipo**: Market Data (dados dinâmicos)

## Funcionalidades

### Merge Incremental
```python
tabela_delta.alias("silver").merge(df_final.alias("novo"), "silver.ID_CARTA <=> novo.ID_CARTA") \
    .withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Compatibilidade e Enriquecimento de Schema
- Diferença de schema é só logada; coluna nova entra via `withSchemaEvolution()` no merge
- Renomeação e padronização de colunas
- Enriquecimento com colunas derivadas (ex: categorias, flags, métricas)
- Preservação de dados existentes

### Metadados das Tabelas
- **`COMMENT ON TABLE`**: descrição de negócio + "Chave única: ..." (de `silver_column_docs.py`)
- **`COMMENT` por coluna**: vindo de `silver_column_docs.py`
- **`PRIMARY KEY`**: sempre declarada na chave; a run falha (RuntimeError) se a chave tiver NULO ou duplicata
- Nenhuma `TBLPROPERTIES` customizada é gravada

### Particionamento das Tabelas
- **Dados Temporais**: Particionamento por ano/mês de referência
- **Dados de Referência**: `TB_DIM_COLECOES` por ano/mês de lançamento; `TB_DOM_SIMBOLOS` sem partição
- **Dados de Preços**: Particionamento por ano/mês de ingestão

## Próximos Passos

Após o processamento na Silver, os dados estarão disponíveis para:
1. **Camada Gold**: Modelos de dados finais e métricas
2. **Análises**: Consultas e dashboards

## Engenharia de Dados

### Princípios da Camada Silver

#### 1. Enriquecimento
- Transformação de dados em informação
- Colunas derivadas e métricas
- Padronização e limpeza
- Metadados organizados

#### 2. Incrementalidade
- Merge por chave
- Preservação de histórico

#### 3. Governança
- Unity Catalog para controle

#### 4. Qualidade
- Validação de integridade
- Remoção de duplicatas
- Compatibilidade de schema
- Logs estruturados

### Regras da Camada

#### Regra #1: Enriquecimento e Padronização (SQL)
```sql
SELECT
    ...,
    CASE WHEN COD_CORES = 'Colorless' THEN 'Colorless' ... END AS NME_CATEGORIA_COR
FROM _sem_delimitador
```

#### Regra #2: Merge Incremental
```python
# condição nula-segura, uma por coluna da chave
condicao_merge = " AND ".join(f"silver.{k} <=> novo.{k}" for k in colunas_chave)
```

#### Regra #3: Compatibilidade de Schema
```python
# Diferença de schema só é logada; coluna nova entra pelo merge
.merge(df_final.alias("novo"), condicao_merge).withSchemaEvolution()
```

#### Regra #4: Logs Estruturados
```python
print(f"Merge concluído em {nome_completo_tabela}.")
```

## Suporte

Para dúvidas ou problemas:
- Verificar logs de execução
- Consultar histórico do Delta Lake
- Revisar configurações de segredos
- Verificar permissões Unity Catalog
- Consultar este README para referência 
