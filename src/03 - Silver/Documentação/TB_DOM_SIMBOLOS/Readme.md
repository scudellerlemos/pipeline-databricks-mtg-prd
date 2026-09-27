<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_DOM_SIMBOLOS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_DOM_SIMBOLOS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** DOM/REF - catálogo de referência pequeno e praticamente estático (símbolos de mana/custo conhecidos pela Scryfall), sem grão de evento nem medida de negócio. Por isso `TB_DOM_` e não `TB_DIM_` (reservado a entidades que crescem organicamente, ex.: `TB_DIM_COLECOES`).

## 2. Descrição Completa
Catálogo de todos os símbolos de mana e custo usados pela Scryfall (cores, híbridos, phyrexianos, genéricos, variantes) com seus atributos (valor de mana, se é mana de verdade, se é humorístico/Un-set etc.). Use para decodificar um símbolo encontrado em `DESC_CUSTO_MANA` (mesma notação `[..]`) ou para montar filtros/agregações por tipo de símbolo.

## 3. Origem dos Dados
- **Fonte (Bronze):** `symbology`
- **Localização:** `<catalog>.silver.TB_DOM_SIMBOLOS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`symbology`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_DOM_SIMBOLOS.py`)
  5. Escrita na tabela Delta: `TB_DOM_SIMBOLOS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| COD_SIMBOLO | string | Notação do símbolo em colchete (ex.: `[W]`, `[2/U]`) - mesma conversão de chave-para-colchete de `DESC_CUSTO_MANA` em `TB_FATO_CARTAS`, para permitir junção direta. Em `DESC_CARTA` os símbolos básicos viram nomes (`[White]`, `[Tap]`) e não casam direto. | Sim |
| URL_ICONE | string | URL do ícone SVG do símbolo. | Não |
| DESC_VARIANTE_LIVRE | string | Forma alternativa de digitar este símbolo em texto livre (ex.: "2/u" - passa por `normalizar_valores`, que aplica `initcap`). 'NA' se ausente. | Não |
| DESC_SIMBOLO | string | Descrição do símbolo em inglês, normalizada (Title Case, espaço vira `_`, ex.: `One_White_Mana`). 'NA' se ausente. | Não |
| FLG_TRANSPONIVEL | boolean | Se o símbolo pode aparecer em ordem trocada (ex.: `{U/W}` e `{W/U}`). | Não |
| FLG_REPRESENTA_MANA | boolean | Se o símbolo representa mana de verdade (produzível/gastável), não só um marcador. | Não |
| FLG_APARECE_CUSTO_MANA | boolean | Se o símbolo pode aparecer em um custo de mana de carta. | Não |
| QTD_VALOR_MANA | double | Valor de mana (contribuição ao custo convertido) que este símbolo representa. | Não |
| FLG_HIBRIDO | boolean | Se é um símbolo híbrido (ex.: `[W/U]`). | Não |
| FLG_PHYREXIANO | boolean | Se é um símbolo phyrexiano (ex.: `[W/P]`). | Não |
| QTD_CUSTO_CONVERTIDO | double | Contribuição deste símbolo ao CMC (custo de mana convertido) da carta. | Não |
| FLG_HUMORISTICO | boolean | Se o símbolo só aparece em cartas humorísticas (Un-sets). | Não |
| COD_CORES | string | Cores associadas a este símbolo (WUBRG), sem colchete/aspas. | Não |
| DESC_GRAFIAS_GATHERER | string | Formas alternativas deste símbolo usadas no Gatherer, separadas por ',_' e normalizadas em Title_Case (ex.: 'Ow,_Oow') - não reproduzem o case original; NULO se não houver. | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Não |
| NME_FONTE | string | Fonte de dados de origem ('Scryfall'). 'NA' se ausente. | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (sempre `symbology`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |

## 7. Chave Única
`COD_SIMBOLO`. Coluna NOT NULL por natureza (todo símbolo tem sua própria notação) - `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado. A Bronze acumula um snapshot do catálogo por execução e a Silver lê todos; o dedup por `COD_SIMBOLO` fica com a versão de `DT_INGESTAO` mais recente (`coluna_ordenacao`).
- **Merge incremental:** por `COD_SIMBOLO`.
- **Particionamento:** nenhum (tabela pequena e estática - algumas dezenas de linhas).
- **Regra "sem `( ) { }` no dado Silver" - aplicada sem exceção a `COD_SIMBOLO`:** a notação nativa de símbolo da Scryfall usa chave (`{W}`), que é notação legítima do domínio, não um artefato de serialização. Mesmo assim, esta tabela converte para colchete (`[W]`) pela mesma regra usada em `TB_FATO_CARTAS`, garantindo que o mesmo símbolo tenha a mesma notação em toda a Silver e permita junção direta entre um token de `DESC_CUSTO_MANA` e `COD_SIMBOLO`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | #115/#116: criação inicial (`TB_DOM_SIMBOLOS`, DAMA - DOM/REF), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Tabela de referência pequena - útil para lookup/decode, não para agregação de volume.
