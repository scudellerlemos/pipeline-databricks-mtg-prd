# TB_FATO_MERCADO_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_MERCADO_CARTAS
- **Camada:** Gold

## 2. Descrição Completa
Visão única de mercado de cartas de Magic: The Gathering - combina catálogo de carta, coleção de origem, histórico de cotação de preço e volume de esclarecimentos oficiais de regras. Uma linha por cotação de preço de uma impressão de carta. Feita para consumo direto por analista/BI/Genie, sem precisar conhecer Bronze/Silver.

## 3. Origem dos Dados (Silver)
- **Usadas (5 de 7):** `TB_FATO_CARTAS` (driver), `TB_FATO_PRECOS_CARTAS` (INNER JOIN por `NME_CARTA`), `TB_DIM_COLECOES` (LEFT JOIN por `COD_COLECAO`), `TB_FATO_ESCLARECIMENTOS_CARTAS` (agregada por `ID_ORACLE`, LEFT JOIN), `TB_MOV_MIGRACOES_CARTAS` (agregada por `ID_CARTA_ANTIGO`, LEFT JOIN em `ID_CARTA = ID_CARTA_ANTIGO` - resolve id de carta migrado/descontinuado pela Scryfall).
- **Não usadas (2 de 7), desvio documentado:** `TB_DOM_SIMBOLOS` e `TB_PONTE_CARTA_SIMBOLOS` (grão carta x símbolo de mana, incompatível com o grão carta x cotação desta Gold - juntar exigiria explodir `DESC_CUSTO_MANA` em tokens). Ver docstring de `TB_FATO_MERCADO_CARTAS.py` para o raciocínio completo.

## 4. Grão e Chave Única
- **Grão:** 1 linha por cotação de preço de uma impressão de carta.
- **Chave:** `(ID_CARTA, DT_COTACAO)`.

## 5. Regras de Nulo
- Categórico/descritivo NULO (por join sem match) -> `Nao_Identificado`.
- Medida (`VLR_USD`/`VLR_EUR`/`VLR_TIX`) NULA continua NULA - 0 não é "sem cotação".
- `QTD_ESCLARECIMENTOS` NULO -> 0 (zero é valor real).
- Data NULA -> sentinela `1001-01-01`.
- Chave (`ID_CARTA`, `DT_COTACAO`) nunca é mascarada - run falha (`RuntimeError`) se vier NULA.
- `ID_CARTA_CANONICO` nunca é NULO - cai no próprio `ID_CARTA` quando não há migração.

## 6. Carga
Full extract da Silver a cada execução + `MERGE INTO` idempotente por `(ID_CARTA, DT_COTACAO)` (equality nula-segura `<=>`). Particionada por `ANO_COTACAO`/`MES_COTACAO`.

## 7. Data Quality e Auditoria
Checagens de PK (nulo/duplicata), FK (`ID_ORACLE` nulo, coleção não encontrada), valor negativo de preço e nulo residual em coluna categórica - logadas a cada run. 1 linha de auditoria por execução em `TB_AUDITORIA_GOLD` (run id, início/fim, duração, contagens, resultado de DQ, status).

## 8. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | Criação - substitui as 3 tabelas Gold antigas (schema pré-DAMA, colunas em inglês) por uma única tabela Gold sobre o schema Silver atual |
| 2026-09-15 | Felipe | Adiciona `TB_MOV_MIGRACOES_CARTAS` (LEFT JOIN agregado) e as colunas `ID_CARTA_CANONICO`/`FLG_ID_CARTA_MIGRADO`, pra resolver id de carta migrado/descontinuado direto na Gold |
