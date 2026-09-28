# TB_FATO_MERCADO_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_MERCADO_CARTAS
- **Camada:** Gold (`silver.TB_FATO_PRECOS_CARTAS` x `gold.TB_DIM_CARTAS`)

## 2. Descrição Completa
Visão única de mercado de cartas de Magic: The Gathering - combina catálogo de carta, coleção de origem, histórico de cotação de preço e volume de esclarecimentos oficiais de regras. Uma linha por cotação de preço de uma impressão de carta. Feita para consumo direto por analista/BI/Genie, sem precisar conhecer Bronze/Silver.

## 3. Origem dos Dados (Silver)
- **Usadas (todas as 5):** `TB_FATO_CARTAS` (driver de `TB_DIM_CARTAS`), `TB_FATO_PRECOS_CARTAS` (INNER JOIN por `ID_CARTA`), `TB_DIM_COLECOES` (LEFT JOIN por `COD_COLECAO`), `TB_FATO_ESCLARECIMENTOS_CARTAS` (agregada por `ID_ORACLE`, LEFT JOIN), `TB_MOV_MIGRACOES_CARTAS` (agregada por `ID_CARTA_ANTIGO`, LEFT JOIN em `ID_CARTA = ID_CARTA_ANTIGO` - resolve id de carta migrado/descontinuado pela Scryfall).

## 4. Grão e Chave Única
- **Grão:** 1 linha por cotação de preço de uma impressão de carta.
- **Chave:** `(ID_CARTA, DT_COTACAO)`, declarada como PRIMARY KEY.

## 5. Regras de Nulo
- `NME_COLECAO`/`NME_BLOCO` NULOS -> `Nao_Identificado` (coleção sem match no LEFT JOIN; em `NME_BLOCO` também quando a coleção não pertence a nenhum bloco). Os demais categóricos vêm da Silver como estão.
- Medida (`VLR_USD`/`VLR_USD_FOIL`/`VLR_USD_ETCHED`/`VLR_EUR`/`VLR_EUR_FOIL`/`VLR_TIX`) NULA continua NULA - 0 não é "sem cotação".
- `QTD_ESCLARECIMENTOS` NULO -> 0 (zero é valor real).
- Data NULA -> sentinela `1001-01-01`.
- Chave (`ID_CARTA`, `DT_COTACAO`) nunca é mascarada - run falha (`RuntimeError`) se vier NULA.
- `ID_CARTA_CANONICO` nunca é NULO - cai no próprio `ID_CARTA` quando não há migração.

## 6. Carga
`TB_DIM_CARTAS` é recalculada e gravada com `overwrite` a cada execução. Esta tabela é incremental: MERGE só com as cotações posteriores à última `DT_COTACAO` gravada e com todo o histórico das cartas que mudaram em `TB_DIM_CARTAS` (atributo novo, como migração ou ruling, vale para cotações antigas). Chave duplicada no lote aborta antes de gravar. Carga completa (`overwrite`) na primeira execução, se a dimensão mudar de colunas ou com o widget `rebuild=true` — único jeito de tirar da Gold uma cotação apagada da Silver. Particionada por `ANO_COTACAO`/`MES_COTACAO`.

## 7. Data Quality e Auditoria
Abortam a run: PK (desta tabela ou de `TB_DIM_CARTAS`) com chave NULA ou duplicada (no lote, antes de gravar; ou já gravada na tabela), `ID_ORACLE` nulo, valor negativo de preço e nulo residual em coluna categórica (limite 0); no pré-join, preços sem carta acima de 12000. Só informativos: coleção não encontrada, cartas sem cotação e ids migrados. 1 linha de auditoria em `TB_AUDITORIA_GOLD` (run id, início/fim, duração, contagens, resultado de DQ, status) por execução, inclusive as abortadas (gravada num `finally`): `SUCESSO`, `FALHA_DQ` (DQ pré-join ou pós-carga), `FALHA_DQ_PK` (validação de PK) ou `FALHA` (erro inesperado).

## 8. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | Criação - substitui as 3 tabelas Gold antigas (schema pré-DAMA, colunas em inglês) por uma única tabela Gold sobre o schema Silver atual |
| 2026-09-15 | Felipe | Adiciona `TB_MOV_MIGRACOES_CARTAS` (LEFT JOIN agregado) e as colunas `ID_CARTA_CANONICO`/`FLG_ID_CARTA_MIGRADO`, pra resolver id de carta migrado/descontinuado direto na Gold |
| 2026-09-27 | Felipe | Passa a ser montada a partir de `TB_DIM_CARTAS` e carga incremental que propaga mudança da dimensão ao histórico (ADR-013); `rebuild=true` recalcula tudo |
