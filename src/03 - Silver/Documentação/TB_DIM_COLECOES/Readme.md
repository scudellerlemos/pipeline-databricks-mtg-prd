<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_DIM_COLECOES

## 1. Nome da Tabela e Camada
- **Tabela:** TB_DIM_COLECOES
- **Camada:** Silver
- **Classificação DAMA-DMBOK (#116):** Dimensão - descreve a entidade de negócio "coleção/edição" (nome, tipo, data de lançamento, bloco...), referenciada por `COD_COLECAO` a partir de `TB_FATO_CARTAS`. Não é uma lista de domínio estática pequena (REF): cresce a cada lançamento, por isso `TB_DIM_` e não `TB_REF_`.

## 2. Descrição Completa
Tabela Silver contendo os dados limpos e transformados de conjuntos (sets/edições) do Magic: The Gathering, processados a partir da camada Bronze, com aplicação de regras de negócio, limpeza de dados e padronização para análises de lançamento e coleção.

## 3. Origem dos Dados
- **Fonte (Bronze):** `sets`
- **Localização:** `<catalog>.silver.TB_DIM_COLECOES` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`sets`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_DIM_COLECOES.py`)
  5. Escrita na tabela Delta: `TB_DIM_COLECOES` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS (ex.: `COD_COLECAO`, `NME_COLECAO`), mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| COD_COLECAO | string | Código curto do set/edição (ex.: 'M19'). Sempre presente. | Sim |
| NME_COLECAO | string | Nome completo do set/edição. Title case. | Não |
| NME_TIPO_COLECAO | string | Tipo de set em Title Case (Core, Expansion, Masters, Promo...). | Não |
| NME_COR_BORDA | string | Cor de borda padrão das cartas do set em Title Case (Black/White/Silver). Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| ID_CARDMARKET | int | Id do set na Cardmarket (MKM). Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| NME_CARDMARKET | string | Nome do set na Cardmarket (MKM). Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| DT_LANCAMENTO | date | Data de lançamento do set. | Não |
| COD_GATHERER | string | Código do set usado no Gatherer (Wizards). Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| COD_MAGICCARDSINFO | string | Código do set usado no site magiccards.info. Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| COD_ANTIGO | string | Código antigo do set, se já foi renomeado. Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| FLG_SOMENTE_ONLINE | boolean | true se o set só existe em ambiente digital (Arena/MTGO). NULO se ausente na Bronze. | Não |
| QTD_CARTAS | int | Quantidade de cartas no set. | Não |
| COD_COLECAO_PAI | string | Código do set "pai", quando este é um sub-set. Em minúsculas, como vem da Scryfall (COD_COLECAO é upper) - junte com `upper(COD_COLECAO_PAI)` = COD_COLECAO. | Não |
| NME_BLOCO | string | Bloco de expansão ao qual o set pertence, normalizado (Title_Case, espaço vira `_`). NULO se o set não pertence a bloco. | Não |
| URL_ICONE | string | URL do ícone SVG do set. | Não |
| DESC_BOOSTER_SLOT_0..19 | string | Slot 0-19 do pacote de booster deste set (tipo de carta possível nessa posição). Sempre NULL: campo legado da magicthegathering.io, sem equivalente na Scryfall (a Stage grava None). | Não |
| NME_FONTE | string | Fonte de dados de origem ('Scryfall'). 'NA' se ausente. | Não |
| ANO_LANCAMENTO | int | Ano derivado de DT_LANCAMENTO (partição física). | Não |
| MES_LANCAMENTO | int | Mês derivado de DT_LANCAMENTO (partição física). | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (sempre `sets`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |

## 7. Chave Única
`COD_COLECAO`. Coluna NOT NULL por natureza (todo set tem código) - `silver_utils.save_to_silver` valida isso antes de declarar a constraint (1 `SELECT` que soma as linhas nulas da(s) coluna(s) de chave) e só então aplica `ALTER COLUMN ... SET NOT NULL` + `PRIMARY KEY` de verdade no Unity Catalog; se algum dia houver linha com `COD_COLECAO` nulo: na primeira carga (antes de a PK existir) a run falha com erro explícito (contagem exata) logo após a gravação - as linhas já gravadas permanecem na tabela - em vez de a tabela ficar sem PK silenciosamente; nas execuções seguintes a coluna já é NOT NULL, então o próprio MERGE é rejeitado pelo Delta (violação de NOT NULL) e nada é gravado. `COMMENT ON TABLE` é sempre gravado, independente da PK.

## 8. Regras de Implementação
- **Filtro temporal:** nenhum na Silver; a Stage (`sets.py`) já restringe a coleções com `releaseDate` >= 1º de janeiro de (ano atual − `years_back`, padrão 5) - não é o histórico completo.
- **Merge incremental:** por `COD_COLECAO`.
- **Particionamento:** por `ANO_LANCAMENTO` e `MES_LANCAMENTO`.
- **Limpeza:** `NME_COLECAO`/`NME_TIPO_COLECAO`/`NME_BLOCO`/`NME_FONTE` (e `NME_CARDMARKET`/`NME_COR_BORDA`, sempre NULOS) normalizados via `normalizar_valores`; `NME_FONTE` nulo/vazio -> 'NA'.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-01-18 | Felipe | Criação inicial (`TB_REF_SILVER_SETS`) |
| 2026-09-15 | Felipe | #115/#116: renomeada para TB_DIM_COLECOES (DAMA - Dimensão), colunas 100% PT-BR/recasadas, adicionado Estágio 0 de renomeação a partir da Bronze real (SELECT anterior nunca resolvia contra a Bronze crua), corrigido `extract_from_bronze` para o nome real da tabela ("sets"), sinalização de chave única na tabela |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Merge incremental idempotente por `COD_COLECAO`.
- `FLG_SOMENTE_ONLINE` pode vir NULO em cargas Bronze antigas sem a coluna `onlineOnly`.
