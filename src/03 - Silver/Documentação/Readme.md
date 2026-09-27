# 📚 Documentação da Camada Silver
<br>
<br>
<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/1t61LYFb/doc.png" alt="Imagem de documentação" width="400"/>
</div>
<br>

## 📋 Visão Geral

Esta pasta contém a **documentação completa** de todas as tabelas da camada Silver do pipeline de dados do Magic: The Gathering. Cada tabela possui sua documentação detalhada com schema, regras de negócio, chave única, particionamento e linhagem de dados.

## 🎯 Objetivo

Fornecer documentação executiva e técnica de todas as tabelas Silver, permitindo:
- **Visão geral rápida** das tabelas disponíveis
- **Acesso direto** à documentação detalhada de cada tabela
- **Entendimento da arquitetura** de dados da camada Silver
- **Referência técnica** para desenvolvimento, análise e manutenção

## 🃏 Tabelas Documentadas

### 🎴 [**TB_FATO_CARTAS**](TB_FATO_CARTAS/Readme.md) - Cartas do Magic
- **Descrição**: Dados limpos de cartas (uma linha por impressão) - o que é a carta: regras, custo, tipo, raridade, coleção
- **Classificação DAMA**: Fato
- **Chave Única**: `Id_carta`
- **Particionamento**: `Ano_ingestao`, `Mes_ingestao`

### 📦 [**TB_DIM_COLECOES**](TB_DIM_COLECOES/Readme.md) - Coleções
- **Descrição**: Dados limpos de sets/edições do Magic
- **Classificação DAMA**: Dimensão
- **Chave Única**: `Cod_colecao`
- **Particionamento**: `Ano_lancamento`, `Mes_lancamento`

### 💰 [**TB_FATO_PRECOS_CARTAS**](TB_FATO_PRECOS_CARTAS/Readme.md) - Preços de Cartas
- **Descrição**: Histórico de cotações de preço por carta (USD/EUR/TIX) - uma linha por coleta
- **Classificação DAMA**: Fato
- **Chave Única**: `Nme_carta` + `Dt_ingestao`
- **Particionamento**: `Ano_ingestao`, `Mes_ingestao`

### 🔀 [**TB_MOV_MIGRACOES_CARTAS**](TB_MOV_MIGRACOES_CARTAS/Readme.md) - Migrações de Id
- **Descrição**: Histórico de migrações de id feitas pela Scryfall (unificação/remoção) e id canônico resolvido
- **Classificação DAMA**: MOV
- **Chave Única**: `Id_migracao`
- **Particionamento**: `Ano_execucao`, `Mes_execucao`

### 🔣 [**TB_DOM_SIMBOLOS**](TB_DOM_SIMBOLOS/Readme.md) - Símbolos de Mana
- **Descrição**: Catálogo de referência de símbolos de mana/custo (cores, híbridos, phyrexianos)
- **Classificação DAMA**: DOM/REF
- **Chave Única**: `Cod_simbolo`
- **Particionamento**: nenhum (tabela pequena e estática)

### 📖 [**TB_FATO_ESCLARECIMENTOS_CARTAS**](TB_FATO_ESCLARECIMENTOS_CARTAS/Readme.md) - Esclarecimentos de Regras
- **Descrição**: Esclarecimentos oficiais de regras (rulings) publicados para cartas específicas
- **Classificação DAMA**: Fato sem medida
- **Chave Única**: `Id_esclarecimento` (surrogate hash)
- **Particionamento**: `Ano_publicacao`, `Mes_publicacao`

### 🌉 [**TB_PONTE_CARTA_SIMBOLOS**](TB_PONTE_CARTA_SIMBOLOS/Readme.md) - Ponte Carta x Símbolos
- **Descrição**: Explode o custo de mana de `TB_FATO_CARTAS` em 1 linha por símbolo (Silver -> Silver, resolve a relação N:N escondida em `Desc_custo_mana`)
- **Classificação DAMA**: Ponte/associativa
- **Chave Única**: `Id_carta` + `Num_ordem_simbolo`
- **Particionamento**: nenhum (sem data de evento própria)

## 🔄 Categorização das Tabelas

| Tabela | Classificação DAMA | Chave Única | Particionamento |
|--------|--------------------|--------------|------------------|
| TB_FATO_CARTAS | Fato | Id_carta | Ano_ingestao/Mes_ingestao |
| TB_DIM_COLECOES | Dimensão | Cod_colecao | Ano_lancamento/Mes_lancamento |
| TB_FATO_PRECOS_CARTAS | Fato | Nme_carta + Dt_ingestao | Ano_ingestao/Mes_ingestao |
| TB_MOV_MIGRACOES_CARTAS | MOV | Id_migracao | Ano_execucao/Mes_execucao |
| TB_DOM_SIMBOLOS | DOM/REF | Cod_simbolo | nenhum |
| TB_FATO_ESCLARECIMENTOS_CARTAS | Fato sem medida | Id_esclarecimento | Ano_publicacao/Mes_publicacao |
| TB_PONTE_CARTA_SIMBOLOS | Ponte/associativa | Id_carta + Num_ordem_simbolo | nenhum |

## 🎴 **Flavor Text da Documentação**
*"Como um bibliotecário arcano organizando grimórios lapidados, a documentação da camada Silver revela o valor oculto de cada tabela, guiando magos e engenheiros de dados na busca por insights refinados."*

## 📈 Estatísticas da Camada Silver

### **Volume de Dados**
- **7 tabelas** documentadas
- Cada tabela mantém o grão da sua fonte Bronze original - preço e migração de id têm grão próprio, separado de cartas (ver `TB_FATO_CARTAS/Readme.md`, seção 2). Exceção: `TB_PONTE_CARTA_SIMBOLOS` não vem da Bronze, é derivada de `TB_FATO_CARTAS` (Silver -> Silver)

### **Padrões de Nomenclatura**
Todas as colunas a partir da Silver são em PT-BR, sem acento, primeira letra maiúscula e restante minúsculo. Prefixos semânticos:
- **Id_**: Identificador
- **Nme_**: Nome
- **Cod_**: Código
- **Vlr_**: Valor monetário
- **Dt_**: Data/timestamp
- **Qtd_**: Quantidade
- **Num_**: Número
- **Url_**: URL
- **Desc_**: Descrição/texto livre
- **Flg_**: Flag booleano
- **Ano_/Mes_**: Colunas derivadas usadas só como `partition_cols`

### **Regra "sem `( ) { }` no dado Silver"**
Todo texto livre/estrutura serializada da fonte converte `{...}`/`(...)` para `[...]` na Silver, sem exceção por tabela - presença de parêntese/chave no dado Silver indica transformação incompleta.

## 🔍 Como Usar Esta Documentação

### **Para Desenvolvedores**
1. **Visão Geral**: Comece por este README para entender a arquitetura
2. **Documentação Específica**: Acesse a documentação da tabela desejada
3. **Schema Detalhado**: Consulte as colunas e tipos de dados
4. **Regras de Negócio**: Entenda limpeza, tradução e derivação de colunas

### **Para Analistas de Dados**
1. **Linhagem de Dados**: Entenda a origem e transformações
2. **Particionamento**: Otimize consultas usando partições
3. **Chaves Únicas**: Confira a seção 7 de cada README antes de fazer join/agregação
4. **Relacionamentos**: `TB_FATO_CARTAS.Cod_colecao` -> `TB_DIM_COLECOES`; `TB_FATO_PRECOS_CARTAS.Nme_carta`/`TB_FATO_ESCLARECIMENTOS_CARTAS.Id_oracle` -> `TB_FATO_CARTAS`; `TB_MOV_MIGRACOES_CARTAS.Id_carta_canonico` para navegar id pós-migração; `TB_PONTE_CARTA_SIMBOLOS.Id_carta` -> `TB_FATO_CARTAS` e `TB_PONTE_CARTA_SIMBOLOS.Cod_simbolo` -> `TB_DOM_SIMBOLOS` para análise por símbolo/cor de mana

### **Para Administradores**
1. **Configuração**: Verifique segredos e configurações necessárias
2. **Monitoramento**: Acompanhe logs e métricas de processamento
3. **Manutenção**: Entenda estratégias de merge e atualização incremental
4. **Documentação no Unity Catalog**: `COMMENT ON TABLE`/`ALTER COLUMN ... COMMENT` são aplicados automaticamente por todo notebook via `silver_utils.apply_table_documentation`, com o texto centralizado em `silver_column_docs.py`

## 🛡️ Controle de Qualidade

### **Validações Implementadas**
- ✅ **Schema Padronizado**: Nomenclatura PT-BR consistente
- ✅ **Chave Única Sinalizada**: `PRIMARY KEY` no Unity Catalog quando a coluna é NOT NULL por natureza
- ✅ **Particionamento Adequado**: Otimização de performance
- ✅ **Limpeza de Dados**: Sem `( ) { }` remanescente no dado Silver
- ✅ **Merge Incremental**: Atualização idempotente pela chave única de cada tabela

### **Monitoramento**
- 📊 **Contagem de Registros**: Antes e depois do processamento
- 🔄 **Taxa de Atualização**: Frequência de mudanças
- ⚡ **Performance**: Tempo de processamento por tabela
- 🎯 **Qualidade**: Validação de integridade dos dados
