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

Este projeto implementa um **pipeline completo de dados** para análise de mercado de cartas Magic: The Gathering, utilizando múltiplas APIs especializadas e processando dados através de um pipeline ETL moderno no Databricks.

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
│   │   └── validate-pipeline.yml   # 🔄 CI/CD Pipeline
│   ├── 📁 scripts/
│   │   └── deploy.py               # 🚀 Script de Deploy
│   └── 📁 DAGs/
│       ├── stage.yml                # 📋 Job MTG_STAGE
│       ├── bronze.yml               # 📋 Job MTG_BRONZE
│       ├── pipeline.yml             # 📋 Job MTG_PIPELINE (orquestrador)
│       ├── silver.yml               # 📋 Job MTG_SILVER
│       └── gold.yml                 # 📋 Job MTG_GOLD
│
└── 📄 README.md                    # 📖 Este arquivo
```

## 🚀 **Pipeline ETL**

### **1. Ingestão (Stage)**
- **Fonte**: Scryfall API (bulk-data para cartas/preços, `/sets` para expansões)
- **Dados**: Cartas, Sets, Preços de mercado (USD, EUR, TIX)
- **Formato**: Parquet, em snapshots datados (`{ano}_{mes}_{dia}_{tabela}.parquet`)
- **Estratégia de carga**: FULL LOAD por execução — a Scryfall não expõe incrementalidade real; a data no nome do arquivo é a data de *ingestão* (para rastreabilidade e idempotência), não um filtro de negócio na origem
- **Frequência**: Mensal (1ª segunda-feira do mês, 6h, `America/Sao_Paulo` — ver `MTG_PIPELINE` em `.github/DAGs/pipeline.yml`); `card_prices` é idempotente por mês
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
- **Nomenclatura de coluna**: 100% PT-BR, sem acento, primeira letra maiúscula e resto minúsculo, com prefixo semântico padronizado (Id_, Nme_, Desc_, Cod_, Dt_, Qtd_, Vlr_, Num_, Url_) - sem uso de `( ) { }` no dado (sinalizaria transformação incompleta)
- **Nomenclatura de tabela**: classificação DAMA-DMBOK (Fato/Dimensão) - ex.: `TB_FATO_CARTAS`, `TB_DIM_COLECOES`
- **Chave única**: sinalizada na própria tabela via `COMMENT ON TABLE` e, quando a chave é NOT NULL, via constraint `PRIMARY KEY`
- **Qualidade**: Validações e transformações

### **4. Gold Layer**
- **Função**: Visão de mercado pronta para consumo direto por analista, BI ou Genie, sem precisar conhecer Bronze/Silver
- **Dados**: 1 tabela (`TB_FATO_MERCADO_CARTAS`) — combina catálogo de carta, coleção, cotação de preço e esclarecimentos de regras; 5 das 7 tabelas Silver alimentam a junção (`TB_DOM_SIMBOLOS`/`TB_PONTE_CARTA_SIMBOLOS` têm grão incompatível e ficam de fora)
- **Grão**: 1 linha por cotação de preço de uma impressão de carta — chave `(Id_carta, Dt_cotacao)`
- **Carga**: Full extract da Silver a cada execução + `MERGE INTO` idempotente, particionada por ano/mês de cotação
- **Qualidade**: Checagens de PK/FK, valor negativo de preço e nulo residual, auditadas por run em `TB_AUDITORIA_GOLD`
- **Documentação de negócio**: [`src/04 - Gold/Documentação/`](<src/04 - Gold/Documentação/Readme.md>)

## 🔄 **CI/CD Pipeline**

### **Validações Automáticas**
- ✅ **Verificação de notebooks**: Existência e sintaxe
- ✅ **Validação de dependências**: DAG do pipeline
- ✅ **Teste de conexão**: Databricks
- ✅ **Comentários em PR**: Feedback automático

### **Deploy Automático**
- 🚀 **Trigger**: Push na branch `main`
- 🔧 **Ambiente**: GitHub Environment `Databricks` (secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN`)
- 📦 **Ordem**: `MTG_STAGE` → `MTG_BRONZE` → `MTG_SILVER` → `MTG_GOLD` → `MTG_PIPELINE` (orquestrador, referencia os `job_id` dos 4 anteriores)
- 📊 **Verificação**: checa se os 5 jobs existem no workspace após o deploy

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
- **Características**: API pública, sem necessidade de chave; bulk-data cobre cartas/preços/rulings em um único download, sem paginação manual
- **Rate Limiting**: requisições sequenciais com retry/backoff (`http_get_with_retry`) em 429/5xx


### **Entidades Principais**
- 🃏 **Cartas**: catálogo completo via bulk-data
- 📦 **Sets**: Todas as expansões
- 💰 **Preços**: Histórico de preços (uma linha por coleta, sem dedup)
- 🔣 **Symbology**: Catálogo de símbolos de mana/custo
- 📜 **Rulings**: Esclarecimentos oficiais de regras por carta
- 🔀 **Migrations**: Histórico de reconciliação de IDs de carta

### **Métricas Calculadas**
- 📈 **ROI**: Retorno sobre investimento
- 📊 **Volatilidade**: Análise de risco
- 🎯 **Tendências**: Movimentos de mercado
- ⚡ **Alertas**: Oportunidades de investimento

### **Processamento e Resiliência (Stage)**
- 📥 **Bulk-data**: catálogo completo em um único download (sem paginação)
- 🔁 **Idempotência**: arquivos datados, reexecução no mesmo dia não reescreve dados já gravados
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
O pipeline é deployado automaticamente quando:
- ✅ PR é aprovada e mergeada na `main`
- ✅ Validações passam com sucesso
- ✅ Conexão com Databricks está ativa

### **3. Monitoramento**
- 📊 **Databricks Jobs**: Monitoramento de execução
- 📈 **GitHub Actions**: Status do CI/CD
- 🔔 **Alertas**: Notificações de falhas


### **Executivo**
- 📊 **Visão Geral**: Métricas principais
- 📈 **Tendências**: Movimentos de mercado
- 💰 **ROI**: Performance de investimentos

### **Operacional**
- 🔍 **Análise Temporal**: Evolução de preços
- ⚠️ **Alertas**: Oportunidades identificadas
- 📋 **Relatórios**: Detalhamento por categoria

## 🔧 **Configuração Técnica**

### **Cluster Configuration**

#### **🛠️ Ambiente de Desenvolvimento (DEV)**

Definido em `.github/DAGs/{stage,bronze,silver,gold}.yml` — os quatro usam o mesmo bloco:

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

#### **🚀 Ambiente de Produção (PRD) - Recomendado**
```yaml
spark_version: "15.4.x-scala2.12"
node_type_id: "m5d.xlarge"          # Maior capacidade
num_workers: 2                       # Mais workers para performance
aws_attributes:
  first_on_demand: 1
  zone_id: "us-west-2a"
  spot_bid_price_percent: 100
spark_conf:
  spark.databricks.delta.preview.enabled: "true"
  spark.databricks.delta.optimizeWrite.enabled: "true"
  spark.databricks.delta.autoCompact.enabled: "true"
```

### **Schedule**
- ⏰ **Frequência**: Mensal, 1ª segunda-feira do mês, às 6h (Brasil) — `MTG_PIPELINE` em `.github/DAGs/pipeline.yml`
- 🌍 **Timezone**: America/Sao_Paulo
- 🔄 **Status**: UNPAUSED

### **Diferenças entre Ambientes**

| Aspecto | DEV | PRD |
|---------|-----|-----|
| **Node Type** | m5d.large | m5d.xlarge |
| **Workers** | 1-2 (autoscale) | 2 |
| **Performance** | Básica | Otimizada |
| **Custo** | Baixo | Médio |
| **Uso** | Testes/Desenvolvimento | Produção |

## 🤝 **Contribuição**

### **Fluxo de Desenvolvimento**
1. 🍴 **Fork** o projeto
2. 🌿 **Crie** uma branch para sua feature
3. 💾 **Commit** suas mudanças
4. 🔀 **Abra** um Pull Request
5. ✅ **Aguarde** as validações automáticas

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