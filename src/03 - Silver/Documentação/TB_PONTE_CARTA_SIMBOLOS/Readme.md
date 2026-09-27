# TB_PONTE_CARTA_SIMBOLOS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_PONTE_CARTA_SIMBOLOS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** Ponte/associativa (bridge table) - resolve a relação N:N entre carta e símbolo de mana, sem grão de evento (não é Fato/MOV) e sem ser um catálogo de referência (não é DOM/REF - quem define o que cada símbolo significa é `TB_DOM_SIMBOLOS`; esta tabela só resolve qual carta tem qual símbolo).

## 2. Descrição Completa
Explode o custo de mana já limpo de `TB_FATO_CARTAS` (`DESC_CUSTO_MANA`, ex.: `[2][U][U]`) em uma linha por símbolo. Use para analisar cartas por símbolo/cor de mana (curva de mana, distribuição de cor, quantidade de símbolos coloridos) sem precisar fazer parsing de texto - junte `COD_SIMBOLO` com `TB_DOM_SIMBOLOS` para nome/cor/valor do símbolo.

## 3. Origem dos Dados
- **Fonte:** Silver -> Silver (não Bronze) - lê `TB_FATO_CARTAS.DESC_CUSTO_MANA` (já limpo, notação `[X][Y]...`) e `TB_DOM_SIMBOLOS.COD_SIMBOLO` (só DQ informativo de símbolo sem match; sem enriquecimento).
- **Localização:** `<catalog>.silver.TB_PONTE_CARTA_SIMBOLOS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. `TB_FATO_CARTAS` (Silver, já processada)
  2. Transformação `src/03 - Silver/Dev/TB_PONTE_CARTA_SIMBOLOS.py`: `regexp_extract_all` + `posexplode` sobre `DESC_CUSTO_MANA`
  3. Escrita na tabela Delta: `TB_PONTE_CARTA_SIMBOLOS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Mesma convenção PT-BR de `TB_FATO_CARTAS` - sem colunas de linhagem Bronze (`DT_INGESTAO`/`NME_FONTE`/etc.), pois esta tabela não é extraída direto da Bronze.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| ID_CARTA | string | Impressão de carta a que este símbolo pertence - junte com `TB_FATO_CARTAS.ID_CARTA`. | Sim |
| NUM_ORDEM_SIMBOLO | int | Posição deste símbolo dentro do custo de mana da carta (1 = primeiro símbolo à esquerda). | Sim |
| COD_SIMBOLO | string | Símbolo de mana nesta posição do custo, mesma notação de `TB_DOM_SIMBOLOS.COD_SIMBOLO` - junte lá para nome/cor/valor do símbolo. | Não (FK) |

## 7. Chave Única
`(ID_CARTA, NUM_ORDEM_SIMBOLO)`. Nunca nula por natureza - `NUM_ORDEM_SIMBOLO` é sempre gerada pelo próprio `posexplode` - `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro:** carta com `DESC_CUSTO_MANA` nulo ou `'NA'` (sem custo de mana, ex.: terrenos) não gera nenhuma linha aqui.
- **Merge incremental:** por `(ID_CARTA, NUM_ORDEM_SIMBOLO)`.
- **Particionamento:** nenhum - a tabela não tem data de evento própria (é derivada de um atributo estático de `TB_FATO_CARTAS`), então `ANO_X`/`MES_X` seria particionamento artificial.
- **`COD_SIMBOLO` nunca é mascarado:** é a FK para `TB_DOM_SIMBOLOS.COD_SIMBOLO`. Símbolo sem match no domínio (catálogo desatualizado/símbolo novo) ainda vira uma linha aqui - o token existe no custo de mana da carta, é fato. A contagem de símbolo sem match é logada como DQ informativo na execução, nunca falha a run nem inventa um valor.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | Criação - resolve o parsing de `DESC_CUSTO_MANA` na Silver (em vez de deixar para Gold/BI fazer parsing de texto) |

## 10. Observações
- Tabela cresce proporcionalmente ao número de símbolos por carta (várias linhas por carta) - não confundir com o grão de `TB_FATO_CARTAS` (1 linha por carta).
- Não entra na `TB_FATO_MERCADO_CARTAS` (grão incompatível: carta x símbolo vs. carta x cotação) - fica disponível na Silver para quem precisar de análise por símbolo sem mudar o grão da Gold.
