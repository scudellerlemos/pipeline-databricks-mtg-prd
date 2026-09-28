# TB_DIM_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_DIM_CARTAS
- **Camada:** Gold (base de `gold.TB_FATO_MERCADO_CARTAS`)

## 2. Descrição Completa
Uma linha por impressão de carta de Magic: The Gathering com os atributos atuais: catálogo, coleção de origem, esclarecimentos oficiais de regras e migração de id. `TB_FATO_MERCADO_CARTAS` junta estes atributos a cada cotação de preço.

## 3. Origem dos Dados (Silver)
- `TB_FATO_CARTAS` (driver), `TB_DIM_COLECOES` (LEFT JOIN por `COD_COLECAO`), `TB_FATO_ESCLARECIMENTOS_CARTAS` (agregada por `ID_ORACLE`, LEFT JOIN), `TB_MOV_MIGRACOES_CARTAS` (agregada por `ID_CARTA_ANTIGO`, LEFT JOIN em `ID_CARTA = ID_CARTA_ANTIGO`).
- `TB_FATO_PRECOS_CARTAS` não entra na dimensão; só é lida no DQ pré-join (preços sem carta).

## 4. Grão e Chave Única
- **Grão:** 1 linha por impressão de carta (`ID_CARTA`).
- **Chave:** `ID_CARTA`, declarada como PRIMARY KEY.

## 5. Regras de Nulo
Mesmas de `TB_FATO_MERCADO_CARTAS` para as colunas em comum: `NME_COLECAO`/`NME_BLOCO` NULOS -> `Nao_Identificado`, `QTD_ESCLARECIMENTOS` NULO -> 0, data NULA -> `1001-01-01`, `ID_CARTA_CANONICO` cai no próprio `ID_CARTA` sem migração. Ver [TB_FATO_MERCADO_CARTAS](../TB_FATO_MERCADO_CARTAS/Readme.md#5-regras-de-nulo).

## 6. Carga
Recalculada inteira e gravada com `overwrite` a cada execução (tamanho do catálogo, não cresce com o histórico). A dimensão recalculada é comparada com a gravada (`EXCEPT`): as cartas que mudaram têm todo o histórico reprocessado em `TB_FATO_MERCADO_CARTAS`. É gravada depois da fato: se a fato falhar, esta tabela segue a antiga e a próxima run acha as mesmas cartas mudadas ([ADR-013](../../../../docs/ADR.md#adr-013--gold-incremental-com-propagação-da-dimensão)). Mesmo notebook e mesma task da fato (`Dev/TB_FATO_MERCADO_CARTAS.py`).

## 7. Data Quality e Auditoria
PK com chave NULA ou duplicada aborta a run (`FALHA_DQ_PK`). Os DQs pré-join e a auditoria em `TB_AUDITORIA_GOLD` são os da execução de `TB_FATO_MERCADO_CARTAS` (1 linha por run cobre as duas tabelas).

## 8. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-27 | Felipe | Criação - atributos da carta saem da fato para uma dimensão própria, base da carga incremental da fato (ADR-013) |
