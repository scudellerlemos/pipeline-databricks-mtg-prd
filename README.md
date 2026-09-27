# 🃏 Pipeline de Dados - Magic: The Gathering

<div align="center">

![Magic: The Gathering](https://static.wikia.nocookie.net/finalfantasy/images/9/9e/FFAB_Thundara_-_Vivi_SR.png)

> **Pipeline completo de dados para análise de mercado de cartas Magic: The Gathering**

</div>

[![CI/CD Pipeline](https://github.com/scudellerlemos/pipeline-databricks-mtg-dev/actions/workflows/validate-pipeline.yml/badge.svg)](https://github.com/scudellerlemos/pipeline-databricks-mtg-dev/actions/workflows/validate-pipeline.yml)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)

## 📋 **Visão Geral**

Este projeto implementa um **pipeline completo de dados** para análise de mercado de cartas Magic: The Gathering, utilizando a API pública da Scryfall (endpoints bulk-data, /sets, /symbology e /migrations) e processando dados através de um pipeline ETL moderno no Databricks.

### 🎯 **Objetivos**

- 📊 **Análise de Mercado**: Monitoramento de preços e tendências
- 📈 **Métricas de Investimento**: Performance e ROI de cartas
- 🎮 **Insights Estratégicos**: Dados para decisões de negócio
- ⚡ **Automação Completa**: Pipeline CI/CD com deploy automático

## 🏗️ **Arquitetura**

<div align="center">

*Arquitetura do Pipeline ETL - Magic: The Gathering*

</div>

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Scryfall API  │    │                 │    │                 │
│                 │───▶│  Databricks     │───▶│  Analytics      │
│   (Extract)     │    │  (Transform)    │    │  (Load)         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bronze Layer  │    │   Silver Layer  │    │   Gold Layer    │
│   (Raw Data)    │    │   (Cleaned)     │    │   (Analytics)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 **Estrutura do Projeto**

```
pipeline-databricks-mtg-dev/
├── 📁 src/
│   ├── 📁 00 - Common/Dev/         # 🧰 base_utils.py (get_secret, trava de catálogo) e smoke_deploy.py
│   │
│   ├── 📁 01 - Ingestion/          # 🚀 Ingestão de dados da Scryfall API (Stage)
│   │   ├── cards.py             # Cartas
│   │   ├── sets.py              # Sets/Expansões
│   │   ├── card_prices.py       # Preços das cartas
│   │   ├── symbology.py         # Símbolos de mana/custo
│   │   ├── rulings.py           # Esclarecimentos de regras
│   │   ├── migrations.py        # Reconciliação de IDs Scryfall
│   │   └── ingestion_utils.py      # HTTP retry, S3, controle de execução
│   │
│   ├── 📁 02 - Bronze/             # 🥉 Camada Bronze (Raw)
│   │   ├── 📁 Dev/
│   │   │   ├── cards.py
│   │   │   ├── sets.py
│   │   │   ├── card_prices.py
│   │   │   ├── symbology.py
│   │   │   ├── rulings.py
│   │   │   ├── migrations.py
│   │   │   └── bronze_utils.py     # EL compartilhado (append + Unity Catalog)
│   │   └── 📁 Documentação/
│   │
│   ├── 📁 03 - Silver/             # 🥈 Camada Silver (Cleaned)
│   │   ├── 📁 Dev/
│   │   │   ├── TB_FATO_CARTAS.py
│   │   │   ├── TB_DIM_COLECOES.py
│   │   │   ├── TB_FATO_PRECOS_CARTAS.py
│   │   │   ├── TB_FATO_ESCLARECIMENTOS_CARTAS.py
│   │   │   ├── TB_MOV_MIGRACOES_CARTAS.py
│   │   │   ├── TB_DOM_SIMBOLOS.py
│   │   │   ├── TB_PONTE_CARTA_SIMBOLOS.py
│   │   │   └── silver_utils.py     # TL compartilhado (transform + Unity Catalog)
│   │   └── 📁 Documentação/
│   │
│   └── 📁 04 - Gold/               # 🥇 Camada Gold (Analytics)
│       ├── 📁 Dev/
│       │   ├── TB_FATO_MERCADO_CARTAS.py
│       │   └── gold_utils.py       # config/extract/load/auditoria, mesmo padrão da Silver
│       └── 📁 Documentação/
│
├── 📁 .github/
│   ├── 📁 workflows/
│   │   ├── validate-pipeline.yml   # 🔄 CI + deploy em dev
│   │   ├── promote.yml             # 🚀 Publica em produção após merge na main
│   │   └── check-credenciais.yml   # 🔑 Aviso mensal de token perto de expirar
│   ├── 📁 prd/
│   │   └── deploy-prd.yml          # 🏭 Workflow do repo de produção (copiado a cada publicação)
│   ├── 📁 scripts/
│   │   ├── deploy.py               # Deploy dos jobs (dev ou prd, por env var)
│   │   ├── smoke.py                # Smoke test pós-deploy
│   │   ├── validate_dag.py         # Validação da DAG e dos notebooks
│   │   └── lint_notebooks.py       # Lint das células Python
│   ├── requirements-ci.txt         # Dependências do CI e do deploy de prd
│   └── 📁 DAGs/
│       ├── stage.yml                # 📋 Job MTG_STAGE
│       ├── bronze.yml               # 📋 Job MTG_BRONZE
│       ├── pipeline.yml             # 📋 Job MTG_PIPELINE (orquestrador)
│       ├── silver.yml               # 📋 Job MTG_SILVER
│       └── gold.yml                 # 📋 Job MTG_GOLD
│
├── 📁 docs/
│   └── ADR.md                      # 📐 Decisões de arquitetura
└── 📄 README.md                    # 📖 Este arquivo
```

## 🚀 **Pipeline ETL**

### **1. Ingestão (Stage)**
- **Fonte**: Scryfall API (bulk-data para cartas/preços, `/sets` para expansões)
- **Dados**: Cartas, Sets, Preços de mercado (USD, EUR, TIX)
- **Formato**: Parquet, em snapshots datados (`{ano}_{mes}_{dia}_{tabela}.parquet`)
- **Estratégia de carga**: FULL LOAD por execução — a Scryfall não expõe incrementalidade real; o `{dia}` do nome do arquivo é sempre o dia da execução; em `sets` e `card_prices`, `{ano}_{mes}` vêm do `releaseDate`, com janela de anos — não há filtro incremental na origem
- **Frequência**: Mensal (1ª segunda-feira do mês, 6h, `America/Sao_Paulo` — ver `MTG_PIPELINE` em `.github/DAGs/pipeline.yml`); reexecução pula arquivos já gravados com o mesmo nome (em `sets`/`card_prices` isso inclui execuções de outros meses no mesmo dia do mês - bug conhecido)
- **Controle de execução**: um JSON por run em `_control/{tabela}/{run_id}.json` (status, contagens, duração, erro)
- **Resiliência**: retry com backoff em erros HTTP transitórios (429/5xx)

### **2. Bronze Layer**
- **Função**: EL puro (Extract & Load) - lê o Parquet da Stage e grava Delta append-only, sem regra de negócio
- **Dados**: 6 tabelas (`cards`, `sets`, `card_prices`, `symbology`, `rulings`, `migrations`), uma por origem da Stage
- **Particionamento**: Nenhum (volume atual não justifica)
- **Preservação**: Schema de origem 1:1, sem dedup nem MERGE/upsert
- **Documentação de negócio**: [`src/02 - Bronze/Documentação/`](<src/02 - Bronze/Documentação/README.md>) (tabela e coluna, comentado também no Unity Catalog)

### **3. Silver Layer**
- **Função**: Limpeza e padronização
- **Nomenclatura de coluna**: 100% PT-BR, sem acento, 100% MAIÚSCULAS, com prefixo semântico padronizado (ID_, NME_, DESC_, COD_, DT_, ANO_, MES_, QTD_, VLR_, NUM_, FLG_, URL_) - sem uso de `( ) { }` no dado (sinalizaria transformação incompleta)
- **Nomenclatura de tabela**: classificação DAMA-DMBOK (Fato/Dimensão) - ex.: `TB_FATO_CARTAS`, `TB_DIM_COLECOES`
- **Chave única**: sinalizada na própria tabela via `COMMENT ON TABLE` e constraint `PRIMARY KEY` (a carga falha se a chave tiver NULO ou duplicata; o `SET NOT NULL` é aplicado automaticamente)
- **Qualidade**: Validações e transformações

### **4. Gold Layer**
- **Função**: Visão de mercado pronta para consumo direto por analista, BI ou Genie, sem precisar conhecer Bronze/Silver
- **Dados**: 1 tabela (`TB_FATO_MERCADO_CARTAS`) — combina catálogo de carta, coleção, cotação de preço, esclarecimentos de regras e migrações de ID; 5 das 7 tabelas Silver alimentam a junção (`TB_DOM_SIMBOLOS`/`TB_PONTE_CARTA_SIMBOLOS` têm grão incompatível e ficam de fora)
- **Grão**: 1 linha por cotação de preço de uma impressão de carta — chave `(ID_CARTA, DT_COTACAO)`
- **Carga**: Full extract da Silver a cada execução + merge Delta idempotente pela chave, particionada por ano/mês de cotação
- **Qualidade**: Checagens de PK/FK, valor negativo de preço e nulo residual, auditadas por run em `TB_AUDITORIA_GOLD`
- **Documentação de negócio**: [`src/04 - Gold/Documentação/`](<src/04 - Gold/Documentação/Readme.md>)

## 🔄 **CI/CD Pipeline**

Dois ambientes no mesmo workspace: **dev** (este repo, catálogo `mtg_dev`, livre
para experimentar) e **prd** (repo [`pipeline-databricks-mtg-prd`](https://github.com/scudellerlemos/pipeline-databricks-mtg-prd),
catálogo `mtg_prod`). O porquê de cada escolha está em [`docs/ADR.md`](docs/ADR.md).

```
PR ──▶ CI ──▶ merge na main ──▶ CI + deploy dev ──▶ promote.yml ──▶ repo -prd ──▶ deploy-prd.yml
       (validate)               (jobs MTG_*)       (snapshot + tag     (tag         (revalida, deploy,
                                                    prd-*, dispatch)    imutável)    smoke, carga)
```

### **Validações Automáticas** (todo PR e push de código na `main`)
- ✅ **Sintaxe**: YAMLs das DAGs
- ✅ **Estrutura**: DAG, notebooks, cluster e git (`validate_dag.py`)
- ✅ **Lint**: células Python dos notebooks (`lint_notebooks.py`)
- ✅ **Testes**: `pytest`
- ✅ **Comentário no PR**: confirmação quando todas as validações passam (falha aparece só no check `validate`)

### **Deploy em Dev**
- 🚀 **Trigger**: push na `main` que mexe em código (`src/`, `.github/{DAGs,workflows,scripts,prd}/`, `.github/requirements-ci.txt`) — merge só de docs não deploya
- 🔧 **Ambiente**: GitHub Environment `Databricks` (secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN`)
- 📦 **Ordem**: `MTG_STAGE` → `MTG_BRONZE` → `MTG_SILVER` → `MTG_GOLD` → `MTG_PIPELINE` (orquestrador, referencia os `job_id` dos 4 anteriores)
- 🧪 **Smoke test**: roda um notebook no ambiente recém-deployado

### **Publicação em Produção**
- 🚀 **Trigger**: CI da `main` verde após um push — **merge na `main` = produção**, sem tag nem aprovação manual. Merge só de documentação (README, `docs/`) não dispara: vai junto na próxima publicação de código
- 📦 **Snapshot**: o repo inteiro é copiado para o repo de prd, commitado e marcado com a tag `prd-AAAAMMDD-HHMM-<sha7>`
- 🏭 **Deploy**: o `deploy-prd.yml` do repo de prd revalida, cria os jobs `MTG_*_PRD` apontando para a tag e roda o smoke test
- ▶️ **Carga**: se o snapshot publicado mudou algum arquivo (código ou não), dispara o `MTG_PIPELINE_PRD` na hora
- ⏪ **Rollback**: `Actions → Deploy produção (MTG) → Run workflow` no repo de prd, com a tag anterior (marque `rodar` para reprocessar)
- 🔑 **Token**: `PRD_DISPATCH_TOKEN` (PAT com *Contents* e *Workflows* no repo de prd); o `check-credenciais.yml` avisa 60 dias antes de expirar

## 🛠️ **Tecnologias**

*Stack Tecnológico*


| Componente | Tecnologia | Versão |
|------------|------------|---------|
| **Cloud Platform** | AWS | - |
| **Data Platform** | Databricks | 15.4.x |
| **Processing** | Apache Spark | 3.5+ |
| **Language** | Python | 3.9+ |
| **Storage** | Delta Lake | - |
| **CI/CD** | GitHub Actions | - |
| **APIs** | Scryfall | - |

## 🔗 **Fontes de Dados**

### **Scryfall API**
- **URL**: `https://api.scryfall.com`
- **Dados**: Cartas, Sets, Preços de mercado (USD, EUR, TIX), Símbolos de mana, Rulings, Migrações de ID
- **Características**: API pública, sem necessidade de chave; bulk-data: cards, card_prices e rulings baixam cada um o seu arquivo em 1 download, sem paginação manual (cards e card_prices baixam o mesmo `default_cards`)
- **Rate Limiting**: requisições sequenciais com retry/backoff (`http_get_with_retry`) em 429/5xx


### **Entidades Principais**
- 🃏 **Cartas**: catálogo completo via bulk-data
- 📦 **Sets**: Todas as expansões
- 💰 **Preços**: Histórico de preços (uma linha por coleta, sem dedup)
- 🔣 **Symbology**: Catálogo de símbolos de mana/custo
- 📜 **Rulings**: Esclarecimentos oficiais de regras por carta
- 🔀 **Migrations**: Histórico de reconciliação de IDs de carta

### **Processamento e Resiliência (Stage)**
- 📥 **Bulk-data**: catálogo completo em um único download (sem paginação)
- 🔁 **Idempotência**: arquivos datados, reexecução não reescreve arquivo já gravado com o mesmo nome
- 🛡️ **Tolerância a Falhas**: retry com backoff em erros HTTP transitórios


## 🚀 **Como Usar**

### **1. Configuração Inicial**
```bash
# Clone o repositório
git clone https://github.com/scudellerlemos/pipeline-databricks-mtg-dev.git
cd pipeline-databricks-mtg-dev

# Configure as variáveis de ambiente
export DATABRICKS_HOST="your-databricks-instance"
export DATABRICKS_TOKEN="your-token"
```

### **2. Deploy Automático**
Merge na `main` que mexe em código, com o CI verde, deploya em dev e publica em produção — ver
[CI/CD Pipeline](#-cicd-pipeline). Nenhum passo manual.

### **3. Monitoramento**
- 📊 **Databricks Jobs**: Monitoramento de execução
- 📈 **GitHub Actions**: Status do CI/CD
- 🔔 **Alertas**: Notificações de falhas


## 🔧 **Configuração Técnica**

### **Cluster Configuration**

Definido em `.github/DAGs/{stage,bronze,silver,gold}.yml` — os quatro usam o
mesmo bloco, e dev e prd usam o mesmo pool:

```yaml
spark_version: "15.4.x-scala2.12"
instance_pool_id: "0925-163505-peep89-pool-mgfqrcwi"
driver_instance_pool_id: "0925-163505-peep89-pool-mgfqrcwi"
autoscale:
  min_workers: 1
  max_workers: 2
spark_conf:
  spark.databricks.delta.preview.enabled: "true"
  spark.databricks.delta.optimizeWrite.enabled: "true"
  spark.databricks.delta.autoCompact.enabled: "true"
```

O node type (`m5d.large`), a zona (`us-west-2a`) e a disponibilidade (`ON_DEMAND`)
vêm do instance pool `mtg-pipeline-pool-dbr154`, não do YAML:

| Campo do pool | Valor |
|---|---|
| `node_type_id` | `m5d.large` |
| `preloaded_spark_versions` | `15.4.x-scala2.12` |
| `min_idle_instances` | `0` |
| `max_capacity` | `6` |
| `idle_instance_autotermination_minutes` | `10` |

> ⚠️ **`preloaded_spark_versions` do pool tem que bater com o `spark_version` dos
> YAMLs.** Se divergir, o cluster baixa e instala o runtime inteiro em cada subida
> — que é justamente o custo que o pool existe pra eliminar. Esse campo é
> **imutável depois que o pool é criado**: pra trocar de runtime é preciso criar um
> pool novo e atualizar o `instance_pool_id` nos quatro YAMLs.

### **Schedule**
- ⏰ **Frequência**: Mensal, 1ª segunda-feira do mês, às 6h (Brasil) — `MTG_PIPELINE` em `.github/DAGs/pipeline.yml`
- 🌍 **Timezone**: America/Sao_Paulo
- 🔄 **Status**: `PAUSED` em dev, `UNPAUSED` em prd

### **Diferenças entre Ambientes**

Mesmo código, workspace, pool e secret scope. Dev usa o YAML e o secret scope;
prd sobrescreve via env var `MTG_*` injetada pelo `deploy.py` ([ADR-005](docs/ADR.md#adr-005--dev-e-prd-no-mesmo-workspace-diferença-só-por-env-var)):

| Aspecto | DEV | PRD |
|---------|-----|-----|
| **Jobs** | `MTG_*` | `MTG_*_PRD` |
| **Catálogo** | `mtg_dev` | `mtg_prod` |
| **S3** | `s3://magicthegatheringdev/dev` | `s3://magicthegatheringdev/prd` |
| **Código** | branch `main` deste repo | tag `prd-*` do repo de prd |
| **Schedule** | pausado | ativo |
| **Uso** | Livre: testar, rodar, apagar | Só muda por publicação |

## 🤝 **Contribuição**

### **Fluxo de Desenvolvimento**
1. 🌿 **Crie** uma branch a partir da `main`
2. 💾 **Commit** suas mudanças (rode à vontade no catálogo `mtg_dev`)
3. 🔀 **Abra** um Pull Request e aguarde as validações
4. ✅ **Merge** na `main` — vai para produção sozinho (se tocar código)
5. 📐 **Decisão de arquitetura nova?** Registre em [`docs/ADR.md`](docs/ADR.md)

### **Padrões de Código**
- 📝 **Documentação**: READMEs em cada pasta
- 🏷️ **Nomenclatura**: Prefixos padronizados
- 🔍 **Validação**: Notebooks testados
- 📊 **Logs**: Mensagens informativas


## 🙏 **Agradecimentos**

- 🎮 **Wizards of the Coast**: Magic: The Gathering
- 💰 **Scryfall**: API de cartas, sets e dados de mercado
- ☁️ **Databricks**: Plataforma de dados
- 🚀 **GitHub**: CI/CD e versionamento

---

<div align="center">

**🎉 Pipeline Magic: The Gathering - Transformando dados em insights estratégicos! 🎉**


<img src="https://media1.tenor.com/m/yf2J9gTT3rQAAAAC/bye-bye.gif" alt="Bye Bye" width="200" height="150">

*Obrigado por explorar nosso pipeline! 👋*

</div> 