# Documentação da Camada Silver
<br>
<br>
<div align="center">
<img src="https://i.postimg.cc/1t61LYFb/doc.png" alt="Imagem de documentação" width="400"/>
</div>
<br>

## Visão Geral

Esta pasta contém a documentação de todas as tabelas da camada Silver do pipeline de dados do Magic: The Gathering. Cada tabela possui sua documentação detalhada com schema, regras de negócio, chave única, particionamento e linhagem de dados.

## Objetivo

Fornecer documentação de negócio e técnica de todas as tabelas Silver, permitindo:
- **Visão geral rápida** das tabelas disponíveis
- **Acesso direto** à documentação detalhada de cada tabela
- **Entendimento da arquitetura** de dados da camada Silver
- **Referência técnica** para desenvolvimento, análise e manutenção

## Tabelas Documentadas

### [TB_FATO_CARTAS](TB_FATO_CARTAS/Readme.md) - Cartas do Magic
- **Descrição**: Dados limpos de cartas (uma linha por impressão) - o que é a carta: regras, custo, tipo, raridade, coleção
- **Classificação DAMA**: Fato
- **Chave Única**: `ID_CARTA`
- **Particionamento**: `ANO_INGESTAO`, `MES_INGESTAO`

### [TB_DIM_COLECOES](TB_DIM_COLECOES/Readme.md) - Coleções
- **Descrição**: Dados limpos de sets/edições do Magic
- **Classificação DAMA**: Dimensão
- **Chave Única**: `COD_COLECAO`
- **Particionamento**: `ANO_LANCAMENTO`, `MES_LANCAMENTO`

### [TB_FATO_PRECOS_CARTAS](TB_FATO_PRECOS_CARTAS/Readme.md) - Preços de Cartas
- **Descrição**: Histórico de cotações de preço por impressão (USD/EUR/TIX) - uma linha por impressão por coleta
- **Classificação DAMA**: Fato
- **Chave Única**: `ID_CARTA` + `DT_INGESTAO`
- **Particionamento**: `ANO_INGESTAO`, `MES_INGESTAO`

### [TB_MOV_MIGRACOES_CARTAS](TB_MOV_MIGRACOES_CARTAS/Readme.md) - Migrações de Id
- **Descrição**: Histórico de migrações de id feitas pela Scryfall (unificação/remoção) e id canônico resolvido
- **Classificação DAMA**: MOV
- **Chave Única**: `ID_MIGRACAO`
- **Particionamento**: `ANO_EXECUCAO`, `MES_EXECUCAO`

### [TB_DOM_SIMBOLOS](TB_DOM_SIMBOLOS/Readme.md) - Símbolos de Mana
- **Descrição**: Catálogo de referência de símbolos de mana/custo (cores, híbridos, phyrexianos)
- **Classificação DAMA**: DOM/REF
- **Chave Única**: `COD_SIMBOLO`
- **Particionamento**: nenhum (tabela pequena e estática)

### [TB_FATO_ESCLARECIMENTOS_CARTAS](TB_FATO_ESCLARECIMENTOS_CARTAS/Readme.md) - Esclarecimentos de Regras
- **Descrição**: Esclarecimentos oficiais de regras (rulings) publicados para cartas específicas
- **Classificação DAMA**: Fato sem medida
- **Chave Única**: `ID_ESCLARECIMENTO` (surrogate hash)
- **Particionamento**: `ANO_PUBLICACAO`, `MES_PUBLICACAO`

### [TB_PONTE_CARTA_SIMBOLOS](TB_PONTE_CARTA_SIMBOLOS/Readme.md) - Ponte Carta x Símbolos
- **Descrição**: Explode o custo de mana de `TB_FATO_CARTAS` em 1 linha por símbolo (Silver -> Silver, resolve a relação N:N escondida em `DESC_CUSTO_MANA`)
- **Classificação DAMA**: Ponte/associativa
- **Chave Única**: `ID_CARTA` + `NUM_ORDEM_SIMBOLO`
- **Particionamento**: nenhum (sem data de evento própria)

## Categorização das Tabelas

| Tabela | Classificação DAMA | Chave Única | Particionamento |
|--------|--------------------|--------------|------------------|
| TB_FATO_CARTAS | Fato | ID_CARTA | ANO_INGESTAO/MES_INGESTAO |
| TB_DIM_COLECOES | Dimensão | COD_COLECAO | ANO_LANCAMENTO/MES_LANCAMENTO |
| TB_FATO_PRECOS_CARTAS | Fato | ID_CARTA + DT_INGESTAO | ANO_INGESTAO/MES_INGESTAO |
| TB_MOV_MIGRACOES_CARTAS | MOV | ID_MIGRACAO | ANO_EXECUCAO/MES_EXECUCAO |
| TB_DOM_SIMBOLOS | DOM/REF | COD_SIMBOLO | nenhum |
| TB_FATO_ESCLARECIMENTOS_CARTAS | Fato sem medida | ID_ESCLARECIMENTO | ANO_PUBLICACAO/MES_PUBLICACAO |
| TB_PONTE_CARTA_SIMBOLOS | Ponte/associativa | ID_CARTA + NUM_ORDEM_SIMBOLO | nenhum |

## Estatísticas da Camada Silver

### Volume de Dados
- **7 tabelas** documentadas
- Cada tabela mantém o grão da sua fonte Bronze original - preço e migração de id têm grão próprio, separado de cartas (ver `TB_FATO_CARTAS/Readme.md`, seção 2). Exceção: `TB_PONTE_CARTA_SIMBOLOS` não vem da Bronze, é derivada de `TB_FATO_CARTAS` (Silver -> Silver)

### Padrões de Nomenclatura
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS (ex.: `ID_CARTA`, `NME_CARTA`). Prefixos semânticos:
- **ID_**: Identificador
- **NME_**: Nome
- **COD_**: Código
- **VLR_**: Valor monetário
- **DT_**: Data/timestamp
- **QTD_**: Quantidade
- **NUM_**: Número
- **URL_**: URL
- **DESC_**: Descrição/texto livre
- **FLG_**: Flag booleano
- **ANO_/MES_**: Colunas derivadas usadas só como `partition_cols`

### Regra "sem `( ) { }` no dado Silver"
Todo texto livre/estrutura serializada da fonte converte `{...}`/`(...)` para `[...]` na Silver, sem exceção por tabela - presença de parêntese/chave no dado Silver indica transformação incompleta.

## Como Usar Esta Documentação

### Para Desenvolvedores
1. **Visão Geral**: Comece por este README para entender a arquitetura
2. **Documentação Específica**: Acesse a documentação da tabela desejada
3. **Schema Detalhado**: Consulte as colunas e tipos de dados
4. **Regras de Negócio**: Entenda limpeza, tradução e derivação de colunas

### Para Analistas de Dados
1. **Linhagem de Dados**: Entenda a origem e transformações
2. **Particionamento**: Otimize consultas usando partições
3. **Chaves Únicas**: Confira a seção 7 de cada README antes de fazer join/agregação
4. **Relacionamentos**: `TB_FATO_CARTAS.COD_COLECAO` -> `TB_DIM_COLECOES`; `TB_FATO_PRECOS_CARTAS.ID_CARTA`/`TB_FATO_ESCLARECIMENTOS_CARTAS.ID_ORACLE` -> `TB_FATO_CARTAS`; `TB_MOV_MIGRACOES_CARTAS.ID_CARTA_CANONICO` para navegar id pós-migração; `TB_PONTE_CARTA_SIMBOLOS.ID_CARTA` -> `TB_FATO_CARTAS` e `TB_PONTE_CARTA_SIMBOLOS.COD_SIMBOLO` -> `TB_DOM_SIMBOLOS` para análise por símbolo/cor de mana

### Para Administradores
1. **Configuração**: Verifique segredos e configurações necessárias
2. **Monitoramento**: Acompanhe logs e métricas de processamento
3. **Manutenção**: Entenda estratégias de merge e atualização incremental
4. **Documentação no Unity Catalog**: `COMMENT ON TABLE`/`ALTER COLUMN ... COMMENT` são aplicados automaticamente por todo notebook via `silver_utils.apply_table_documentation`, com o texto centralizado em `silver_column_docs.py`

## Controle de Qualidade

### Validações Implementadas
- **Schema Padronizado**: Nomenclatura PT-BR consistente
- **Chave Única Sinalizada**: `PRIMARY KEY` sempre declarada no Unity Catalog; a run falha (RuntimeError) se a chave tiver NULO ou duplicata
- **Particionamento Adequado**: Otimização de performance
- **Limpeza de Dados**: Sem `( ) { }` remanescente no dado Silver
- **Merge Incremental**: Atualização idempotente pela chave única de cada tabela

### Monitoramento
- **Contagem de Registros**: Antes e depois do processamento
- **Taxa de Atualização**: Frequência de mudanças
- **Qualidade**: Validação de integridade dos dados
