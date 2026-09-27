<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_FATO_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_FATO_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK (#116):** Fato - uma linha por impressão de carta (grão), com medidas quantitativas (Qtd_custo_mana, Qtd_cores) e chave estrangeira implícita para a dimensão de coleção (Cod_colecao -> TB_DIM_COLECOES). Preço e histórico de migração de id são Fatos/movimento à parte (TB_FATO_PRECOS_CARTAS, TB_MOV_MIGRACOES_CARTAS - ver observação abaixo).

## 2. Descrição Completa
Tabela Silver contendo os dados limpos e transformados de cartas do Magic: The Gathering, processados a partir da camada Bronze com aplicação de regras de negócio, limpeza de dados e padronização para análises de gameplay, deckbuilding e coleção. Responde "o que é essa carta" - texto de regras, custo de mana, tipo, raridade, artista e em qual coleção ela saiu.

**Separação de preço e migração:** até a revisão de 2026-09-15 (#115/#116), esta tabela também carregava o histórico diário de preço (`Vlr_usd`/`Vlr_eur`/`Vlr_tix`) e o id canônico pós-migração da Scryfall (`Id_scryfall_canonico`), unificados via `attach_prices`/`attach_canonical_id`. Essas duas fontes têm grão diferente do de cartas (preço é por NOME, não por impressão; migração é um evento de mudança de id, não um atributo de carta) e foram separadas em tabelas próprias - ver `TB_FATO_PRECOS_CARTAS` e `TB_MOV_MIGRACOES_CARTAS`. Junte por `Nme_carta`/`Id_carta` quando precisar combinar.

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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo (ex.: `Id_carta`, `Nme_carta`). Prefixos semânticos usados: `Id_` (identificador), `Nme_` (nome), `Desc_` (texto/descrição), `Cod_` (código), `Dt_` (data/timestamp), `Qtd_` (quantidade), `Vlr_` (valor monetário), `Num_` (número), `Url_` (URL).

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Id_carta | string | Id único da impressão na Scryfall. Preservado como veio da fonte - nunca reatribuído. | Sim |
| Id_oracle | string | Oracle id (estável entre impressões da mesma carta). NULO em partições anteriores a #135. | Não |
| Nme_carta | string | Nome da carta. Title case, sem acento. Usado na junção com preço. | Não |
| Desc_custo_mana | string | Custo de mana em notação `[..]` (ex.: `[2][U][U]`), 'NA' se ausente. | Não |
| Qtd_custo_mana | int | Custo de mana convertido (CMC). Nulos -> 0. | Não |
| Cod_cores | string | Cores da carta, sem colchete/aspas. 'Colorless' se vazio. | Não |
| Cod_identidade_cor | string | Identidade de cor (formatos tipo Commander), sem colchete/aspas. | Não |
| Nme_tipo_carta | string | Tipo principal da carta (ex.: 'Creature', 'Planeswalker'). | Não |
| Desc_detalhe_tipo_carta | string | Subtipo/detalhe do tipo, quando a linha de tipo tem '—'. 'NA' se não houver. | Não |
| Desc_tipos | string | Tipos principais da carta, sem colchete/aspas. 'NA' se ausente. | Não |
| Desc_subtipos | string | Subtipos da carta, sem colchete/aspas. 'NA' se ausente. | Não |
| Nme_raridade | string | Raridade da impressão. Title case. | Não |
| Cod_colecao | string | Código do set/edição desta impressão (upper case). FK para `TB_DIM_COLECOES.Cod_colecao`. | Não |
| Nme_colecao | string | Nome completo do set/edição. Title case. | Não |
| Desc_carta | string | Texto de regras (oracle text), com símbolos de mana e texto de lembrete em notação `[..]`. 'NA' se ausente. | Não |
| Nme_artista | string | Nome do ilustrador. Title case. | Não |
| Num_colecionador | string | Número de colecionador dentro do set. | Não |
| Nme_forca | string | Força da criatura (texto - pode ser `*`). Nulos -> '0'. | Não |
| Nme_resistencia | string | Resistência da criatura (texto - pode ser `*`). Nulos -> '0'. | Não |
| Nme_disposicao_carta | string | Layout físico da carta (normal, split, transform...). | Não |
| Id_multiverso | string | Id da carta no Gatherer. | Não |
| Url_imagem | string | URL da imagem da carta. | Não |
| Cod_variacoes | string | Ids de outras impressões/variações visuais, sem colchete/aspas. | Não |
| Desc_nomes_estrangeiros | string | Nomes/textos traduzidos, notação `[..]`. | Não |
| Desc_impressoes | string | Códigos de todos os sets em que a carta já saiu, sem colchete/aspas. | Não |
| Desc_carta_original | string | Texto de regras original (pré-errata), notação `[..]`. | Não |
| Nme_tipo_original | string | Linha de tipo original antes de reclassificações. | Não |
| Desc_legalidades | string | Legalidade por formato de jogo, notação `[..]`. | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou o registro de carta. | Não |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| Nme_categoria_cor | string | Categoria de cor derivada (Colorless, Mono, Dual Color, Multicolor). | Não |
| Qtd_cores | int | Quantidade de símbolos de cor (WUBRG) distintos no custo de mana. | Não |
| Ano_ingestao | int | Ano derivado de Dt_ingestao (partição física). | Não |
| Mes_ingestao | int | Mês derivado de Dt_ingestao (partição física). | Não |

## 7. Chave Única
`Id_carta`. Coluna NOT NULL por natureza (toda impressão tem id) - a constraint `PRIMARY KEY` no Unity Catalog é aplicada com sucesso (ver `silver_utils.save_to_silver`), além do `COMMENT ON TABLE` sempre gravado.

## 8. Regras de Implementação
- **Filtro temporal:** últimos 60 meses de `Dt_ingestao` (Estágio 1).
- **Merge incremental:** por `Id_carta`, desempate por `Dt_ingestao` mais recente.
- **Particionamento:** por `Ano_ingestao` e `Mes_ingestao`.
- **Regra "sem `( ) { }` no dado Silver":** todo texto livre/estrutura serializada (`Desc_carta`, `Desc_custo_mana`, `Desc_carta_original`, `Desc_legalidades`, `Desc_nomes_estrangeiros`) converte `{...}`/`(...)`  para `[...]` no Estágio 3 - presença de parêntese/chave no dado Silver indica transformação incompleta.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2025-07-20 | Felipe | Criação inicial (`TB_FATO_SILVER_CARDS`) |
| 2026-09-08 | Felipe | AUD-20/AUD-21 (#135/#136): captura de oracle_id, resolução de migração de id, particionamento por data de preço |
| 2026-09-15 | Felipe | #115/#116: renomeada para TB_FATO_CARTAS (DAMA - Fato), colunas 100% PT-BR/recasadas, correção do bug de fallback sempre-NULL no Estágio 2, eliminação de `(){}` do dado Silver, sinalização de chave única na tabela |
| 2026-09-15 | Felipe | #115/#116: separação de preço (`TB_FATO_PRECOS_CARTAS`) e migração de id (`TB_MOV_MIGRACOES_CARTAS`) em tabelas próprias - grão volta a ser só `Id_carta`, partição volta a `Ano_ingestao`/`Mes_ingestao` |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Merge incremental idempotente por `Id_carta`.
- Preço de mercado agora está em `TB_FATO_PRECOS_CARTAS` (junte por `Nme_carta`).
- Consumidores Gold que agrupam/janelam por carta através de uma migração de id devem usar `TB_MOV_MIGRACOES_CARTAS.Id_carta_canonico`, não `Id_carta`.
