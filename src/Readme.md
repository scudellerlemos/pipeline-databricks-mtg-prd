# 🏗️ Data Lake - Magic: The Gathering

<div align="center">

![Jace, the Mind Sculptor](https://repositorio.sbrauble.com/arquivos/in/magic/480738/68250850dd567-7s4u3-8loi0-bd6caeb231d828e67d6e2c1b6abc7239.jpg)

*"Como um mágico negro que busca entender sua própria existência, cada camada do Data Lake é um passo na jornada de transformação, onde dados brutos ganham consciência e se tornam insights de poder inestimável."* - Vivi Ornitier, Final Fantasy IX - Magic The Gathering

</div>

## 📋 Visão Geral do Data Lake

Este repositório contém o **pipeline completo de dados** do Magic: The Gathering, implementado como um Data Lake moderno no Databricks. O pipeline segue a arquitetura **Medallion** com três camadas principais: **Bronze** (dados brutos), **Silver** (dados limpos) e **Gold** (análises executivas).

### 🎯 **Objetivo Principal**

Transformar dados brutos da API do Magic: The Gathering em insights estratégicos e análises executivas, seguindo as melhores práticas de Data Engineering:

- **Extract & Load** (Bronze) - Carregamento de dados brutos
- **Transform & Load** (Silver) - Limpeza e enriquecimento
- **Analyze & Load** (Gold) - Análises executivas e métricas

## 🏛️ Arquitetura do Data Lake

```
┌─────────────────────────────────────────────────────────────┐
│                    🎮 MAGIC: THE GATHERING                  │
│                            DATA LAKE                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   🥉 BRONZE     │    │   🥈 SILVER    │    │   🥇 GOLD       │
│                 │    │                 │    │                 │
│ • Extract       │───▶│ • Transform     │───▶│ • Analyze       │
│ • Load          │    │ • Load          │    │ • Load          │
│ • Raw Data      │    │ • Clean Data    │    │ • Insights      │
│ • Staging       │    │ • Enriched      │    │ • Metrics       │
│ • Delta Lake    │    │ • Delta Lake    │    │ • Delta Lake    │
└─────────────────┘    └─────────────────┘    └─────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    🏛️ UNITY CATALOG                         │
│              mtg_dev.{bronze|silver|gold}                    │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Estrutura das Camadas

### 🥉 **Camada Bronze** - Dados Brutos
**Localização**: `src/01 - Ingestion/` → `src/02 - Bronze/`

**Processo**: **EL (Extract & Load)**
- **Extract**: Leitura de dados Parquet da staging (S3)
- **Load**: Append-only no Unity Catalog (sem MERGE/upsert), idempotente por `source_file`
- **Dados**: 6 tabelas, uma por origem da Stage (`cards`, `sets`, `card_prices`, `symbology`, `rulings`, `migrations`)

**Características**:
- ✅ Dados brutos preservados 1:1 (schema de origem, sem renomeação)
- ✅ Append-only, sem dedup por chave de negócio - histórico completo preservado
- ✅ Sem particionamento (volume atual não justifica)
- ✅ Governança via Unity Catalog (tabela e coluna comentadas - ver [`Documentação/`](<02 - Bronze/Documentação/README.md>))
- ✅ Histórico completo via Delta Lake

**Tabelas**: `cards`, `sets`, `card_prices`, `symbology`, `rulings`, `migrations` -
sem prefixo `TB_BRONZE_`, já que vivem no schema `bronze` do Unity Catalog.

### 🥈 **Camada Silver** - Dados Limpos
**Localização**: `src/03 - Silver/`

**Processo**: **TL (Transform & Load)**
- **Transform**: Limpeza, padronização e enriquecimento via SQL (`spark.sql()` sobre temp views)
- **Load**: Carregamento incremental com dados refinados
- **Dados**: 7 tabelas enriquecidas e padronizadas

**Características**:
- ✅ Dados limpos e padronizados
- ✅ Enriquecimento com categorias e métricas
- ✅ Nomenclatura 100% PT-BR com prefixo semântico (Id_, Nme_, Desc_, Cod_, Dt_, Qtd_, Vlr_, Num_, Url_)
- ✅ Nomenclatura de tabela DAMA-DMBOK (Fato/Dimensão/Domínio/Ponte)
- ✅ Qualidade de dados garantida
- ✅ Transformações em SQL puro, sem UDFs Python

**Tabelas**:
- 🃏 **TB_FATO_CARTAS** - Cartas enriquecidas
- 📦 **TB_DIM_COLECOES** - Expansões com metadados
- 💰 **TB_FATO_PRECOS_CARTAS** - Preços processados
- 📜 **TB_FATO_ESCLARECIMENTOS_CARTAS** - Esclarecimentos oficiais de regras
- 🔀 **TB_MOV_MIGRACOES_CARTAS** - Reconciliação de IDs de carta
- 🔣 **TB_DOM_SIMBOLOS** - Catálogo de símbolos de mana/custo
- 🧩 **TB_PONTE_CARTA_SIMBOLOS** - Ponte carta x símbolo (custo de mana explodido)

### 🥇 **Camada Gold** - Análises Executivas
**Localização**: `src/04 - Gold/`

**Processo**: **AL (Analyze & Load)**
- **Analyze**: Junção das tabelas Silver e cálculo de indicadores de mercado via SQL (`spark.sql()` sobre temp views)
- **Load**: `MERGE INTO` idempotente, particionado por ano/mês de cotação
- **Dados**: 1 tabela pronta para consumo direto (analista/BI/Genie), sem precisar conhecer Bronze/Silver

**Características**:
- ✅ Visão única de mercado (catálogo + coleção + preço + esclarecimentos de regras)
- ✅ Grão: 1 linha por cotação de preço de uma impressão de carta
- ✅ Data quality e auditoria por run (`TB_AUDITORIA_GOLD`)
- ✅ Transformações em SQL puro, sem UDFs Python

**Tabelas**:
- 📊 **TB_FATO_MERCADO_CARTAS** - Visão única de mercado (catálogo + coleção + preço + esclarecimentos de regras)

## 🔄 Fluxo de Dados Completo

### **1. Ingestão / Stage (01 - Ingestion)**
```python
# Controle de execução: início do run
run_id = start_run(base_path, "cards", endpoint, params)

# Extração da Scryfall com retry/backoff em 429/5xx
data = http_get_with_retry(url, headers, timeout, retries)

# Salvamento em Parquet no Stage (snapshot datado, idempotente)
save_to_parquet(data, f"{base_path}/cards/{year}_{month}_{day}_cards.parquet")

# Controle de execução: fim do run (SUCCESS/FAILED/PARTIAL)
finish_run(base_path, "cards", run_id, status="SUCCESS", ...)
```

### **2. Bronze (02 - Bronze)**
```python
# EL puro: lê só os arquivos novos da Stage e faz append no Delta,
# sem regra de negócio - ver Dev/bronze_utils.py
run_bronze_ingestion(
    spark, dbutils, catalog_name, schema_name="bronze",
    bronze_table_name="cards", stage_table_name="cards",
    s3_stage_path=..., s3_bronze_path=...,
    table_comment=get_table_comment("cards"),
    column_comments=get_column_comments("cards"),
)
```

### **3. Silver (03 - Silver)**
```python
# Extração da Bronze e transformação via SQL (spark.sql() sobre temp view)
df_bronze = extract_from_bronze(catalog_name, "cards")
df_silver = spark.sql("SELECT ... FROM bronze_cards")  # ver Dev/TB_FATO_CARTAS.py
# MERGE idempotente na Silver + comentários/PK no Unity Catalog
save_to_silver(df_silver, catalog_name, "silver", "TB_FATO_CARTAS", s3_silver_path, ...)
```

### **4. Gold (04 - Gold)**
```python
# Extração das tabelas Silver e junção via SQL (spark.sql() sobre temp views)
df_gold = spark.sql("SELECT ... FROM silver_fato_cartas JOIN ...")  # ver Dev/TB_FATO_MERCADO_CARTAS.py
# Data quality + MERGE idempotente na Gold + auditoria em TB_AUDITORIA_GOLD
save_to_gold(df_gold, catalog_name, "gold", "TB_FATO_MERCADO_CARTAS", s3_gold_path, ...)
```

## 🛠️ Tecnologias Utilizadas

### **Plataforma Principal**
- **Databricks** - Plataforma unificada de analytics
- **Unity Catalog** - Governança de dados
- **Delta Lake** - Storage layer ACID
- **Apache Spark** - Processamento distribuído

### **Linguagens e APIs**
- **SQL** - Regras de negócio das camadas Silver e Gold (`spark.sql()` sobre temp views, sem UDFs Python)
- **Python** - Orquestração (extract/load/save/config)
- **PySpark** - Leitura/escrita de dados e integração com Delta Lake

### **Infraestrutura**
- **AWS S3** - Storage de staging
- **Databricks Secrets** - Gerenciamento de credenciais
- **Databricks Clusters** - Computação escalável

## 📊 Métricas e KPIs do Pipeline

### **Performance**
- **Ingestão**: bulk-data em um único download por tabela, sem paginação
- **Processamento**: Incremental por chaves específicas
- **Tempo de Execução**: <50 minutos para pipeline completo

### **Qualidade**
- **Bronze**: Preservação de dados originais
- **Silver**: Dados limpos e válidos
- **Gold**: Análises com métricas validadas

## 🎯 Casos de Uso

### **Análises de Mercado**
- Valorização de cartas por set e raridade
- Análise de tendências temporais
- Identificação de oportunidades de investimento

### **Análises de Jogo**
- Performance de cartas por formato
- Análise de metagame e tendências
- Estatísticas de uso e popularidade

### **Análises Executivas**
- KPIs de performance de investimentos
- Alertas de oportunidades e riscos
- Relatórios estratégicos para tomada de decisão

## 🔧 Configuração e Execução

### **Pré-requisitos**
- Databricks Workspace configurado
- Unity Catalog habilitado
- Cluster Spark disponível
- Segredos configurados no scope `mtg-pipeline`

### **Segredos Necessários**
```python
catalog_name           # Nome do catálogo Unity
s3_bucket             # Bucket S3 para staging
s3_bronze_prefix      # Prefixo da camada bronze
s3_silver_prefix      # Prefixo da camada silver
s3_gold_prefix        # Prefixo da camada gold
```

### **Ordem de Execução**
1. **Ingestão**: `src/01 - Ingestion/` (extração da API)
2. **Bronze**: `src/02 - Bronze/` (carregamento de dados brutos)
3. **Silver**: `src/03 - Silver/` (transformação e limpeza)
4. **Gold**: `src/04 - Gold/` (análises executivas)


## 🚀 Próximos Passos

### **Expansão Imediata**
- Camada Gold com múltiplas tabelas (Star Schema completo, hoje é 1 tabela larga)
- Análises por formato de jogo (Standard, Modern, Commander)

### **Melhorias Futuras**
- Análises de sentimento de cartas
- Integração com dados de torneios
- Dashboard executivo em tempo real

### **Otimizações**
- Particionamento avançado por múltiplas dimensões
- Cache inteligente para consultas frequentes
- Otimização de queries com Z-Order
- Monitoramento avançado de performance

## 🎴 Flavor Text do Data Lake

*"Como um multiverso de dados que se expande infinitamente, este Data Lake transforma a magia bruta da informação em insights estratégicos de poder inestimável. Cada camada é um plano de existência, cada tabela uma criatura mágica, cada análise um feitiço de poder executivo."*

---

## 📞 Suporte e Contato

Para dúvidas, sugestões ou problemas:
- Verificar documentação específica de cada camada
- Consultar logs de execução no Databricks
- Revisar configurações de segredos e permissões
- Verificar status do Unity Catalog e Delta Lake

**🎮 Que a magia dos dados esteja sempre com você!** 
