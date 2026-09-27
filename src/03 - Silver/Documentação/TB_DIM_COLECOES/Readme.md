<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_DIM_COLECOES

## 1. Nome da Tabela e Camada
- **Tabela:** TB_DIM_COLECOES
- **Camada:** Silver
- **Classificação DAMA-DMBOK (#116):** Dimensão - descreve a entidade de negócio "coleção/edição" (nome, tipo, data de lançamento, bloco...), referenciada por `Cod_colecao` a partir de `TB_FATO_CARTAS`. Não é uma lista de domínio estática pequena (REF): cresce a cada lançamento, por isso `TB_DIM_` e não `TB_REF_`.

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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo (ex.: `Cod_colecao`, `Nme_colecao`), mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Cod_colecao | string | Código curto do set/edição (ex.: 'M19'). Sempre presente. | Sim |
| Nme_colecao | string | Nome completo do set/edição. Title case. | Não |
| Nme_tipo_colecao | string | Tipo de set (core, expansion, masters, promo...). Title case. | Não |
| Nme_cor_borda | string | Cor de borda padrão das cartas do set (black/white/silver). | Não |
| Id_cardmarket | string | Id do set na Cardmarket (MKM). | Não |
| Nme_cardmarket | string | Nome do set na Cardmarket (MKM). | Não |
| Dt_lancamento | date | Data de lançamento do set. | Não |
| Cod_gatherer | string | Código do set usado no Gatherer (Wizards). | Não |
| Cod_magiccardsinfo | string | Código do set usado no site magiccards.info. | Não |
| Cod_antigo | string | Código antigo do set, se já foi renomeado. | Não |
| Flg_somente_online | boolean | true se o set só existe em ambiente digital (Arena/MTGO). NULO se ausente na Bronze. | Não |
| Qtd_cartas | int | Quantidade de cartas no set. | Não |
| Cod_colecao_pai | string | Código do set "pai", quando este é um sub-set. | Não |
| Nme_bloco | string | Bloco de expansão ao qual o set pertence. | Não |
| Url_icone | string | URL do ícone SVG do set. | Não |
| Desc_booster_slot_0..19 | string | Slot 0-19 do pacote de booster deste set (tipo de carta possível nessa posição). | Não |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). 'NA' se ausente. | Não |
| Ano_lancamento | int | Ano derivado de Dt_lancamento (partição física). | Não |
| Mes_lancamento | int | Mês derivado de Dt_lancamento (partição física). | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou o registro de set. | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |

## 7. Chave Única
`Cod_colecao`. Coluna NOT NULL por natureza (todo set tem código) - `silver_utils.save_to_silver` valida isso antes de declarar a constraint (1 `SELECT` que soma as linhas nulas da(s) coluna(s) de chave) e só então aplica `ALTER COLUMN ... SET NOT NULL` + `PRIMARY KEY` de verdade no Unity Catalog; se algum dia houver linha com `Cod_colecao` nulo, a gravação falha com erro explícito (contagem exata) em vez de a tabela ficar sem PK silenciosamente. `COMMENT ON TABLE` é sempre gravado, independente da PK.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (dado de dimensão, histórico completo).
- **Merge incremental:** por `Cod_colecao`.
- **Particionamento:** por `Ano_lancamento` e `Mes_lancamento`.
- **Limpeza:** `Nme_colecao`/`Nme_tipo_colecao`/`Nme_fonte` em Title Case; `Nme_fonte` nulo/vazio -> 'NA'.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-01-18 | Felipe | Criação inicial (`TB_REF_SILVER_SETS`) |
| 2026-09-15 | Felipe | #115/#116: renomeada para TB_DIM_COLECOES (DAMA - Dimensão), colunas 100% PT-BR/recasadas, adicionado Estágio 0 de renomeação a partir da Bronze real (SELECT anterior nunca resolvia contra a Bronze crua), corrigido `extract_from_bronze` para o nome real da tabela ("sets"), sinalização de chave única na tabela |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Merge incremental idempotente por `Cod_colecao`.
- `Flg_somente_online` pode vir NULO em cargas Bronze antigas sem a coluna `onlineOnly`.
