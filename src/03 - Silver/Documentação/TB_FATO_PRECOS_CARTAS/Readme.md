<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_FATO_PRECOS_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_PRECOS_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** Fato - uma linha por coleta de preço de uma carta (grão), com medidas quantitativas (`Vlr_usd`, `Vlr_eur`, `Vlr_tix`). Fato independente de `TB_FATO_CARTAS` desde #115/#116 - ver "Motivo da Separação" abaixo.

## 2. Descrição Completa
Histórico de cotações de preço de cartas de Magic: The Gathering em dólar, euro e MTGO ticket - uma linha por coleta de preço. Use para acompanhar valorização/desvalorização de uma carta ao longo do tempo, comparar preço entre cartas/coleções ou montar um indicador de valor de coleção. A mesma carta tem várias linhas (uma por coleta) de propósito - é histórico, não é "o preço atual".

**Motivo da separação (Silver, #115/#116):** cards (Bronze `cards`) e preços (Bronze `card_prices`) vêm de fontes diferentes com grão diferente - cards é por IMPRESSÃO (`Id_carta`), preço é por NOME (a Scryfall só responde preço por `/cards/named?exact=<name>`, sem granularidade de impressão). Antes desta tabela existir, o preço vivia embutido em `TB_FATO_CARTAS`, obrigando `Id_carta` a carregar a data de coleta na chave só por causa do histórico de preço. Manter cada Fato no seu grão natural é mais simples de entender e de consultar - junte por `Nme_carta` quando precisar combinar carta e preço.

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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Nme_carta | string | Carta a que esta cotação se refere. Title case. Uma cotação vale para todas as impressões do nome. | Sim |
| Cod_colecao | string | Coleção/edição de referência usada nesta coleta de preço (upper case). | Não |
| Nme_raridade | string | Raridade de referência usada nesta coleta de preço. Title case. | Não |
| Vlr_usd | float | Preço em dólares. NULO = sem cotação em dólar nesta coleta, não zero. | Não |
| Vlr_eur | float | Preço em euros. NULO = sem cotação em euro nesta coleta, não zero. | Não |
| Vlr_tix | float | Preço em MTGO tickets. NULO = sem cotação em tix nesta coleta, não zero. | Não |
| Url_scryfall | string | URL da página da carta na Scryfall. | Não |
| Url_imagem | string | URL da imagem de referência usada nesta coleta. | Não |
| Dt_lancamento | date | Data de lançamento da coleção de referência usada nesta coleta. | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou esta cotação de preço. | Sim |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). 'NA' se ausente. | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| Ano_ingestao | int | Ano derivado de Dt_ingestao (partição física). | Não |
| Mes_ingestao | int | Mês derivado de Dt_ingestao (partição física). | Não |

## 7. Chave Única
`Nme_carta` + `Dt_ingestao`. A Bronze `card_prices` guarda só o preço mais recente por nome (merge upsert por nome, sem histórico próprio) - mas como esse "mais recente" muda de data a cada execução, cada run acrescenta uma nova linha na Silver em vez de sobrescrever, e é assim que o histórico diário de preço se acumula.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (histórico de preço é útil por completo).
- **Merge incremental:** por `Nme_carta` + `Dt_ingestao`, desempate por `Dt_ingestao` mais recente.
- **Particionamento:** por `Ano_ingestao` e `Mes_ingestao`.
- **Limpeza:** `Nme_carta`/`Nme_raridade` em Title Case, `Cod_colecao` em upper case; `Vlr_usd`/`Vlr_eur`/`Vlr_tix` sem coalesce (NULO permanece NULO).

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-07-20 | Felipe | Criação inicial (`TB_FATO_SILVER_CARDPRICES`, schema pivotado nunca implementado no notebook real) |
| 2026-09-15 | Felipe | #115/#116: recriada como `TB_FATO_PRECOS_CARTAS`, separada de `TB_FATO_CARTAS`, schema alinhado ao formato largo real da Bronze `card_prices` (usd/eur/tix), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- `Vlr_usd`/`Vlr_eur`/`Vlr_tix` NULO significa "sem preço encontrado", não zero.
- Junte com `TB_FATO_CARTAS.Nme_carta` para combinar preço com dados de carta (join fan-out: uma cotação vale para todas as impressões do nome).
