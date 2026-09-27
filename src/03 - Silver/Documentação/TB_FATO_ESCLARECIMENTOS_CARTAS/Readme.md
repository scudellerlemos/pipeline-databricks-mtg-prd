<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_FATO_ESCLARECIMENTOS_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_ESCLARECIMENTOS_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** Fato sem medida (factless fact) - uma linha por esclarecimento oficial de regra (ruling) publicado para uma carta, grão de evento (publicação de um esclarecimento), sem medida quantitativa própria. Ainda é Fato e não DOM/REF: cresce continuamente (a Wizards publica esclarecimento novo a cada carta lançada) e não é uma lista de opções fixa.

## 2. Descrição Completa
Esclarecimentos oficiais de regras (rulings) publicados pela Wizards/Scryfall para cartas específicas - interpretações de como uma carta interage com outras regras, casos especiais, erratas de funcionamento. Use para responder dúvidas de regra sobre uma carta ou para montar um FAQ por carta.

## 3. Origem dos Dados
- **Fonte (Bronze):** `rulings`
- **Localização:** `<catalog>.silver.TB_FATO_ESCLARECIMENTOS_CARTAS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`rulings`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_FATO_ESCLARECIMENTOS_CARTAS.py`)
  5. Escrita na tabela Delta: `TB_FATO_ESCLARECIMENTOS_CARTAS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| ID_ESCLARECIMENTO | string | Id surrogate (hash SHA-256 determinístico sobre os valores crus da Bronze: oracle_id, source antes da tradução wotc/scryfall, to_date(published_at) e comment antes da troca de ( ) { } por [ ] - não dá pra recalcular a partir das colunas Silver). A fonte não traz id próprio de registro. | Sim |
| ID_ORACLE | string | Oracle id da carta a que este esclarecimento se refere (estável entre impressões). FK para `TB_FATO_CARTAS.ID_ORACLE`. | Não |
| NME_EMISSOR | string | Quem publicou o esclarecimento. **Bug conhecido:** hoje é sempre 'Scryfall' - a Stage sobrescreve `source` com a fonte de linhagem (`ingestion_utils.py`), então o emissor original ('wotc'/'scryfall') se perde antes da Silver. | Não |
| DT_PUBLICACAO | date | Data de publicação do esclarecimento. | Não |
| DESC_ESCLARECIMENTO | string | Texto do esclarecimento, notação `[..]`. 'NA' se ausente. | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Não |
| NME_FONTE | string | Fonte de dados de origem ('Scryfall'). 'NA' se ausente. Lida da mesma coluna `source` que `NME_EMISSOR`. | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (sempre `rulings`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| ANO_PUBLICACAO | int | Ano derivado de DT_PUBLICACAO (partição física). | Não |
| MES_PUBLICACAO | int | Mês derivado de DT_PUBLICACAO (partição física). | Não |

## 7. Chave Única
`ID_ESCLARECIMENTO`. Id surrogate: a Bronze `rulings` não traz um id próprio de registro (Scryfall só garante `oracle_id` + `source` + `published_at` + `comment`) - gerado por hash determinístico (`sha2(concat_ws('|', ...), 256)`) sobre essas 4 colunas, garantindo o mesmo id em reprocessamentos do mesmo dado e permitindo declarar `PRIMARY KEY` real (coluna sempre NOT NULL).

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (histórico de esclarecimentos é útil por completo).
- **Merge incremental:** por `ID_ESCLARECIMENTO`, desempate por `DT_INGESTAO` mais recente.
- **Particionamento:** por `ANO_PUBLICACAO` e `MES_PUBLICACAO`.
- **Tradução de `NME_EMISSOR`:** `'wotc'` -> `'Wizards'`, `'scryfall'` -> `'Scryfall'`, demais valores em Title Case. **Bug conhecido:** como a Stage sobrescreve `source` com `'scryfall'` em toda tabela, o ramo `'wotc'` nunca é atingido e o valor gravado é sempre `'Scryfall'`.
- **Regra "sem `( ) { } no dado Silver"`:** `DESC_ESCLARECIMENTO` converte `{...}`/`(...)` para `[...]`, mesma regra de `TB_FATO_CARTAS`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | #115/#116: criação inicial (`TB_FATO_ESCLARECIMENTOS_CARTAS`, DAMA - Fato sem medida), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Junte por `ID_ORACLE` com `TB_FATO_CARTAS.ID_ORACLE` para trazer esclarecimentos de uma carta (join fan-out esperado: várias impressões compartilham o mesmo `ID_ORACLE`, e uma carta pode ter vários esclarecimentos).
