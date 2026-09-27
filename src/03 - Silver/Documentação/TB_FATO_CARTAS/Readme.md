<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_FATO_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK (#116):** Fato - uma linha por impressão de carta (grão), com medidas quantitativas (QTD_CUSTO_MANA, QTD_CORES) e chave estrangeira implícita para a dimensão de coleção (COD_COLECAO -> TB_DIM_COLECOES). Preço e histórico de migração de id são Fatos/movimento à parte (TB_FATO_PRECOS_CARTAS, TB_MOV_MIGRACOES_CARTAS - ver observação abaixo).

## 2. Descrição Completa
Tabela Silver contendo os dados limpos e transformados de cartas do Magic: The Gathering, processados a partir da camada Bronze com aplicação de regras de negócio, limpeza de dados e padronização para análises de gameplay, deckbuilding e coleção. Responde "o que é essa carta" - texto de regras, custo de mana, tipo, raridade, artista e em qual coleção ela saiu.

**Separação de preço e migração:** até a revisão de 2026-09-15 (#115/#116), esta tabela também carregava o histórico diário de preço (`VLR_USD`/`VLR_EUR`/`VLR_TIX`) e o id canônico pós-migração da Scryfall (`ID_SCRYFALL_CANONICO`), unificados via `attach_prices`/`attach_canonical_id`. Essas duas fontes têm grão diferente do de cartas (preço é por impressão com data de coleta; migração é um evento de mudança de id, não um atributo de carta) e foram separadas em tabelas próprias - ver `TB_FATO_PRECOS_CARTAS` e `TB_MOV_MIGRACOES_CARTAS`. Preço junta por `ID_CARTA`; migração junta `ID_CARTA = ID_CARTA_ANTIGO` e usa `ID_CARTA_CANONICO`.

## 3. Origem dos Dados
- **Fonte (Bronze):** `cards`
- **Localização:** `<catalog>.silver.TB_FATO_CARTAS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`cards`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_FATO_CARTAS.py`)
  5. Escrita na tabela Delta: `TB_FATO_CARTAS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS (ex.: `ID_CARTA`, `NME_CARTA`). Prefixos semânticos usados: `ID_` (identificador), `NME_` (nome), `DESC_` (texto/descrição), `COD_` (código), `DT_` (data/timestamp), `QTD_` (quantidade), `VLR_` (valor monetário), `NUM_` (número), `URL_` (URL).

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| ID_CARTA | string | Id único da impressão na Scryfall. Preservado como veio da fonte - nunca reatribuído. | Sim |
| ID_ORACLE | string | Oracle id (estável entre impressões da mesma carta). NULO em partições anteriores a #135. | Não |
| NME_CARTA | string | Nome da carta. Title case, sem acento. | Não |
| DESC_CUSTO_MANA | string | Custo de mana em notação `[..]` (ex.: `[2][U][U]`), 'NA' se ausente. | Não |
| QTD_CUSTO_MANA | float | Custo de mana convertido (CMC). Nulos -> 0. | Não |
| COD_CORES | string | Cores da carta, sem colchete/aspas. 'Colorless' se vazio. | Não |
| COD_IDENTIDADE_COR | string | Identidade de cor (formatos tipo Commander), sem colchete/aspas. | Não |
| NME_TIPO_CARTA | string | Linha de tipo antes do '—' (supertipos + tipos), em Title_Case com '_' - ex.: 'Creature', 'Legendary_Creature', 'Artifact_Creature', 'Basic_Land'; qualquer Planeswalker vira 'Planeswalker'; 'NA' se ausente. Para achar todas as criaturas use LIKE '%Creature%'. | Não |
| DESC_DETALHE_TIPO_CARTA | string | Subtipo/detalhe do tipo, quando a linha de tipo tem '—'; em Planeswalker, a linha de tipo inteira (ex.: `Legendary_Planeswalker__Jace`: o `—` some no `remover_acentos`, após `normalizar_valores`). 'NA' se não houver. | Não |
| DESC_TIPOS | string | Tipos principais da carta. Sempre 'NA' hoje (a Stage grava `types` nulo). | Não |
| DESC_SUBTIPOS | string | Sempre 'NA' hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None e a regra de nulo converte em 'NA'). | Não |
| NME_RARIDADE | string | Raridade da impressão. Title case. | Não |
| COD_COLECAO | string | Código do set/edição desta impressão (upper case). FK para `TB_DIM_COLECOES.COD_COLECAO`. | Não |
| NME_COLECAO | string | Nome completo do set/edição. Title case. | Não |
| DESC_CARTA | string | Texto de regras oficial atual (Oracle text, com erratas - não necessariamente o impresso nesta edição; em dupla face, só a frente), com símbolos de mana e texto de lembrete em notação `[..]`. 'NA' se ausente. | Não |
| NME_ARTISTA | string | Nome do ilustrador. Title case. | Não |
| NUM_COLECIONADOR | string | Número de colecionador dentro do set. | Não |
| NME_FORCA | string | Força da criatura (texto - pode ser `*`). Nulos -> '0'. | Não |
| NME_RESISTENCIA | string | Resistência da criatura (texto - pode ser `*`). Nulos -> '0'. | Não |
| NME_DISPOSICAO_CARTA | string | Layout físico da carta (normal, split, transform...). | Não |
| ID_MULTIVERSO | int | Sempre NULL hoje (campo legado da magicthegathering.io; a Scryfall tem `multiverse_ids`, mas a Stage não mapeia e grava None). | Não |
| URL_IMAGEM | string | URL da imagem da carta. | Não |
| COD_VARIACOES | string | Sempre NULL hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None). | Não |
| DESC_NOMES_ESTRANGEIROS | string | Sempre NULL hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None). | Não |
| DESC_IMPRESSOES | string | Sempre NULL hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None). | Não |
| DESC_CARTA_ORIGINAL | string | Sempre NULL hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None). | Não |
| NME_TIPO_ORIGINAL | string | Sempre NULL hoje (campo legado da magicthegathering.io sem equivalente na Scryfall; a Stage grava None). | Não |
| DESC_LEGALIDADES | string | Legalidade por formato de jogo, notação `[..]`. | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Não |
| NME_FONTE | string | Fonte de dados de origem ('scryfall'). | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (ex.: `cards`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| NME_CATEGORIA_COR | string | Categoria de cor derivada (Colorless, Mono, Dual_Color, Multicolor). | Não |
| QTD_CORES | int | Quantidade de símbolos de cor (WUBRG) no custo de mana, contando repetições (`{2}{U}{U}` = 2). | Não |
| ANO_INGESTAO | int | Ano derivado de DT_INGESTAO (partição física). | Não |
| MES_INGESTAO | int | Mês derivado de DT_INGESTAO (partição física). | Não |

## 7. Chave Única
`ID_CARTA`. Coluna NOT NULL por natureza (toda impressão tem id) - a constraint `PRIMARY KEY` no Unity Catalog é aplicada com sucesso (ver `silver_utils.save_to_silver`), além do `COMMENT ON TABLE` sempre gravado.

## 8. Regras de Implementação
- **Filtro temporal:** últimos 60 meses de `DT_INGESTAO` (CTE `_renomeado`).
- **Merge incremental:** por `ID_CARTA`, desempate por `DT_INGESTAO` mais recente.
- **Particionamento:** por `ANO_INGESTAO` e `MES_INGESTAO`.
- **Regra "sem `( ) { }` no dado Silver":** todo texto livre/estrutura serializada (`DESC_CARTA`, `DESC_CUSTO_MANA`, `DESC_CARTA_ORIGINAL`, `DESC_LEGALIDADES`, `DESC_NOMES_ESTRANGEIROS`) converte `{...}`/`(...)`  para `[...]` na CTE `_sem_delimitador` - presença de parêntese/chave no dado Silver indica transformação incompleta.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-07-20 | Felipe | Criação inicial (`TB_FATO_SILVER_CARDS`) |
| 2026-09-08 | Felipe | AUD-20/AUD-21 (#135/#136): captura de oracle_id, resolução de migração de id, particionamento por data de preço |
| 2026-09-15 | Felipe | #115/#116: renomeada para TB_FATO_CARTAS (DAMA - Fato), colunas 100% PT-BR/recasadas, correção do bug de fallback sempre-NULL no Estágio 2, eliminação de `(){}` do dado Silver, sinalização de chave única na tabela |
| 2026-09-15 | Felipe | #115/#116: separação de preço (`TB_FATO_PRECOS_CARTAS`) e migração de id (`TB_MOV_MIGRACOES_CARTAS`) em tabelas próprias - grão volta a ser só `ID_CARTA`, partição volta a `ANO_INGESTAO`/`MES_INGESTAO` |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Merge incremental idempotente por `ID_CARTA`.
- Preço de mercado agora está em `TB_FATO_PRECOS_CARTAS` (junte por `ID_CARTA`).
- Consumidores Gold que agrupam/janelam por carta através de uma migração de id devem usar `TB_MOV_MIGRACOES_CARTAS.ID_CARTA_CANONICO`, não `ID_CARTA`.
