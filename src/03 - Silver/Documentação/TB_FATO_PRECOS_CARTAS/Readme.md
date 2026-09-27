<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_FATO_PRECOS_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_PRECOS_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** Fato - uma linha por coleta de preço de uma IMPRESSÃO de carta (grão), com medidas quantitativas (`VLR_USD`, `VLR_USD_FOIL`, `VLR_USD_ETCHED`, `VLR_EUR`, `VLR_EUR_FOIL`, `VLR_TIX`). As variantes físicas (foil/etched) são colunas da mesma linha, não linhas novas. Fato independente de `TB_FATO_CARTAS` - ver o motivo abaixo.

## 2. Descrição Completa
Histórico de cotações de preço de cartas de Magic: The Gathering em dólar, euro e MTGO ticket - uma linha por impressão por coleta de preço. Use para acompanhar valorização/desvalorização de uma carta ao longo do tempo, comparar preço entre cartas/coleções ou montar um indicador de valor de coleção. A mesma impressão tem várias linhas (uma por coleta) de propósito - é histórico, não é "o preço atual".

**Por que é uma tabela separada de `TB_FATO_CARTAS`:** cards (Bronze `cards`) e preços (Bronze `card_prices`) são Fatos distintos no mesmo grão de impressão (`ID_CARTA`) - o preço tem, além disso, a data de coleta. A Stage ingere o bulk `default_cards` (1 objeto por impressão, cada um com seu próprio `prices`), e preço por NOME não existe como número único (o Lightning Bolt tem ~70 impressões de preços muito diferentes). Com o preço dentro de `TB_FATO_CARTAS`, a chave de cartas teria que incluir a data de coleta. Manter cada Fato no seu grão natural é mais simples de entender e de consultar - junte por `ID_CARTA` (cada cotação casa com uma só impressão; uma impressão tem N cotações) quando precisar combinar carta e preço.

## 3. Origem dos Dados
- **Fonte (Bronze):** `card_prices`
- **Localização:** `<catalog>.silver.TB_FATO_PRECOS_CARTAS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`card_prices`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_FATO_PRECOS_CARTAS.py`)
  5. Escrita na tabela Delta: `TB_FATO_PRECOS_CARTAS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS (ex.: `ID_CARTA`, `NME_CARTA`), mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| ID_CARTA | string | Id da impressão (Scryfall) a que esta cotação se refere. FK para `TB_FATO_CARTAS.ID_CARTA`. | Sim |
| NME_CARTA | string | Nome da carta desta impressão. Title case. | Não |
| COD_COLECAO | string | Coleção/edição desta impressão cotada (upper case). | Não |
| NME_RARIDADE | string | Raridade desta impressão cotada. Title case. | Não |
| VLR_USD | float | Preço em dólares. NULO = sem cotação em dólar nesta coleta, não zero. | Não |
| VLR_USD_FOIL | float | Preço em dólares da versão foil. NULO = sem cotação, não zero. | Não |
| VLR_USD_ETCHED | float | Preço em dólares da versão etched. NULO = sem cotação, não zero. | Não |
| VLR_EUR | float | Preço em euros. NULO = sem cotação em euro nesta coleta, não zero. | Não |
| VLR_EUR_FOIL | float | Preço em euros da versão foil. NULO = sem cotação, não zero. | Não |
| VLR_TIX | float | Preço em MTGO tickets. NULO = sem cotação em tix nesta coleta, não zero. | Não |
| URL_SCRYFALL | string | URL da página da carta na Scryfall. | Não |
| URL_IMAGEM | string | URL da imagem desta impressão cotada (em dupla face, a imagem da frente - `card_faces[0]`). | Não |
| DT_LANCAMENTO | date | Data de lançamento desta impressão (released_at da carta na Scryfall) - pode diferir da data da coleção (TB_DIM_COLECOES.DT_LANCAMENTO). | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Sim |
| NME_FONTE | string | Fonte de dados de origem ('Scryfall'). 'NA' se ausente. | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (sempre `card_prices`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| ANO_INGESTAO | int | Ano derivado de DT_INGESTAO (partição física). | Não |
| MES_INGESTAO | int | Mês derivado de DT_INGESTAO (partição física). | Não |

## 7. Chave Única
`ID_CARTA` + `DT_INGESTAO`. A Bronze `card_prices` é append-only (uma cotação por impressão por coleta, sem MERGE/upsert), então o histórico já nasce na Bronze. A Silver preserva esse histórico: cada run acrescenta uma nova linha em vez de sobrescrever, e é assim que o histórico diário de preço se acumula.

## 8. Regras de Implementação
- **Filtro temporal:** nenhum na Silver; a Stage já restringe a impressões com `releaseDate` >= 1º de janeiro de (ano atual − `years_back`, padrão 5) - não é o preço de todas as impressões.
- **Merge incremental:** por `ID_CARTA` + `DT_INGESTAO` (duplicata exata da chave resolvida por `dropDuplicates`, sem `coluna_ordenacao`).
- **Particionamento:** por `ANO_INGESTAO` e `MES_INGESTAO`.
- **Limpeza:** `NME_CARTA`/`NME_RARIDADE` em Title Case, `COD_COLECAO` em upper case; `VLR_*` sem coalesce (NULO permanece NULO).

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-07-20 | Felipe | Criação inicial (`TB_FATO_SILVER_CARDPRICES`, schema pivotado nunca implementado no notebook real) |
| 2026-09-15 | Felipe | #115/#116: recriada como `TB_FATO_PRECOS_CARTAS`, separada de `TB_FATO_CARTAS`, schema alinhado ao formato largo real da Bronze `card_prices` (usd/eur/tix), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- `VLR_*` NULO significa "sem preço encontrado", não zero.
- Junte com `TB_FATO_CARTAS.ID_CARTA` para combinar preço com dados de carta (cada cotação casa com uma só impressão; uma impressão tem N cotações).
