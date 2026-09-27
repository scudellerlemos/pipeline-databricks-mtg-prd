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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Id_esclarecimento | string | Id surrogate (hash SHA-256 determinístico sobre Id_oracle+Nme_emissor+Dt_publicacao+Desc_esclarecimento). A fonte não traz id próprio de registro. | Sim |
| Id_oracle | string | Oracle id da carta a que este esclarecimento se refere (estável entre impressões). FK para `TB_FATO_CARTAS.Id_oracle`. | Não |
| Nme_emissor | string | Quem publicou o esclarecimento: 'Wizards' ou 'Scryfall'. | Não |
| Dt_publicacao | date | Data de publicação do esclarecimento. | Não |
| Desc_esclarecimento | string | Texto do esclarecimento, notação `[..]`. 'NA' se ausente. | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou o registro de esclarecimento. | Não |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). 'NA' se ausente. | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| Ano_publicacao | int | Ano derivado de Dt_publicacao (partição física). | Não |
| Mes_publicacao | int | Mês derivado de Dt_publicacao (partição física). | Não |

## 7. Chave Única
`Id_esclarecimento`. Id surrogate: a Bronze `rulings` não traz um id próprio de registro (Scryfall só garante `oracle_id` + `source` + `published_at` + `comment`) - gerado por hash determinístico (`sha2(concat_ws('|', ...), 256)`) sobre essas 4 colunas, garantindo o mesmo id em reprocessamentos do mesmo dado e permitindo declarar `PRIMARY KEY` real (coluna sempre NOT NULL).

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (histórico de esclarecimentos é útil por completo).
- **Merge incremental:** por `Id_esclarecimento`, desempate por `Dt_ingestao` mais recente.
- **Particionamento:** por `Ano_publicacao` e `Mes_publicacao`.
- **Tradução de `Nme_emissor`:** `'wotc'` -> `'Wizards'`, `'scryfall'` -> `'Scryfall'`, demais valores em Title Case.
- **Regra "sem `( ) { } no dado Silver"`:** `Desc_esclarecimento` converte `{...}`/`(...)` para `[...]`, mesma regra de `TB_FATO_CARTAS`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | #115/#116: criação inicial (`TB_FATO_ESCLARECIMENTOS_CARTAS`, DAMA - Fato sem medida), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Junte por `Id_oracle` com `TB_FATO_CARTAS.Id_oracle` para trazer esclarecimentos de uma carta (join fan-out esperado: várias impressões compartilham o mesmo `Id_oracle`, e uma carta pode ter vários esclarecimentos).
