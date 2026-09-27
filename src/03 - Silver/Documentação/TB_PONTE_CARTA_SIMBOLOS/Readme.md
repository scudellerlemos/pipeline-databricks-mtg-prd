# TB_PONTE_CARTA_SIMBOLOS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_PONTE_CARTA_SIMBOLOS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** Ponte/associativa (bridge table) - resolve a relação N:N entre carta e símbolo de mana, sem grão de evento (não é Fato/MOV) e sem ser um catálogo de referência (não é DOM/REF - quem define o que cada símbolo significa é `TB_DOM_SIMBOLOS`; esta tabela só resolve qual carta tem qual símbolo).

## 2. Descrição Completa
Explode o custo de mana já limpo de `TB_FATO_CARTAS` (`Desc_custo_mana`, ex.: `[2][U][U]`) em uma linha por símbolo. Use para analisar cartas por símbolo/cor de mana (curva de mana, distribuição de cor, quantidade de símbolos coloridos) sem precisar fazer parsing de texto - junte `Cod_simbolo` com `TB_DOM_SIMBOLOS` para nome/cor/valor do símbolo.

## 3. Origem dos Dados
- **Fonte:** Silver -> Silver (não Bronze) - lê `TB_FATO_CARTAS.Desc_custo_mana` (já limpo, notação `[X][Y]...`) e `TB_DOM_SIMBOLOS.Cod_simbolo` (validação/enriquecimento).
- **Localização:** `<catalog>.silver.TB_PONTE_CARTA_SIMBOLOS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. `TB_FATO_CARTAS` (Silver, já processada)
  2. Transformação `src/03 - Silver/Dev/TB_PONTE_CARTA_SIMBOLOS.py`: `regexp_extract_all` + `posexplode` sobre `Desc_custo_mana`
  3. Escrita na tabela Delta: `TB_PONTE_CARTA_SIMBOLOS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Mesma convenção PT-BR de `TB_FATO_CARTAS` - sem colunas de linhagem Bronze (`Dt_ingestao`/`Nme_fonte`/etc.), pois esta tabela não é extraída direto da Bronze.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Id_carta | string | Impressão de carta a que este símbolo pertence - junte com `TB_FATO_CARTAS.Id_carta`. | Sim |
| Num_ordem_simbolo | int | Posição deste símbolo dentro do custo de mana da carta (1 = primeiro símbolo à esquerda). | Sim |
| Cod_simbolo | string | Símbolo de mana nesta posição do custo, mesma notação de `TB_DOM_SIMBOLOS.Cod_simbolo` - junte lá para nome/cor/valor do símbolo. | Não (FK) |

## 7. Chave Única
`(Id_carta, Num_ordem_simbolo)`. Nunca nula por natureza - `Num_ordem_simbolo` é sempre gerada pelo próprio `posexplode` - `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro:** carta com `Desc_custo_mana` nulo ou `'NA'` (sem custo de mana, ex.: terrenos) não gera nenhuma linha aqui.
- **Merge incremental:** por `(Id_carta, Num_ordem_simbolo)`.
- **Particionamento:** nenhum - a tabela não tem data de evento própria (é derivada de um atributo estático de `TB_FATO_CARTAS`), então `Ano_x`/`Mes_x` seria particionamento artificial.
- **`Cod_simbolo` nunca é mascarado:** é a FK para `TB_DOM_SIMBOLOS.Cod_simbolo`. Símbolo sem match no domínio (catálogo desatualizado/símbolo novo) ainda vira uma linha aqui - o token existe no custo de mana da carta, é fato. A contagem de símbolo sem match é logada como DQ informativo na execução, nunca falha a run nem inventa um valor.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | Criação - resolve o parsing de `Desc_custo_mana` na Silver (em vez de deixar para Gold/BI fazer parsing de texto) |

## 10. Observações
- Tabela cresce proporcionalmente ao número de símbolos por carta (várias linhas por carta) - não confundir com o grão de `TB_FATO_CARTAS` (1 linha por carta).
- Não entra na `TB_FATO_MERCADO_CARTAS` (grão incompatível: carta x símbolo vs. carta x cotação) - fica disponível na Silver para quem precisar de análise por símbolo sem mudar o grão da Gold.
