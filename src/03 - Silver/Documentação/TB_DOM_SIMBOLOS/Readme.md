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
Catálogo de todos os símbolos de mana e custo usados pela Scryfall (cores, híbridos, phyrexianos, genéricos, variantes) com seus atributos (valor de mana, se é mana de verdade, se é humorístico/Un-set etc.). Use para decodificar um símbolo encontrado em `Desc_custo_mana`/`Desc_carta` (após reconversão de `[..]` para `{..}`) ou para montar filtros/agregações por tipo de símbolo.

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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Cod_simbolo | string | Notação do símbolo em colchete (ex.: `[W]`, `[2/U]`) - mesma conversão de chave-para-colchete aplicada a símbolos embutidos em `TB_FATO_CARTAS`, para permitir junção direta. | Sim |
| Url_icone | string | URL do ícone SVG do símbolo. | Não |
| Desc_variante_livre | string | Forma alternativa de digitar este símbolo em texto livre (ex.: "2/U"). 'NA' se ausente. | Não |
| Desc_simbolo | string | Descrição do símbolo em inglês (nome oficial da Scryfall). 'NA' se ausente. | Não |
| Flg_transponivel | boolean | Se o símbolo pode ser digitado sem chave em texto livre. | Não |
| Flg_representa_mana | boolean | Se o símbolo representa mana de verdade (produzível/gastável), não só um marcador. | Não |
| Flg_aparece_custo_mana | boolean | Se o símbolo pode aparecer em um custo de mana de carta. | Não |
| Qtd_valor_mana | double | Valor de mana (contribuição ao custo convertido) que este símbolo representa. | Não |
| Flg_hibrido | boolean | Se é um símbolo híbrido (ex.: `[W/U]`). | Não |
| Flg_phyrexiano | boolean | Se é um símbolo phyrexiano (ex.: `[W/P]`). | Não |
| Qtd_custo_convertido | double | Contribuição deste símbolo ao CMC (custo de mana convertido) da carta. | Não |
| Flg_humoristico | boolean | Se o símbolo só aparece em cartas humorísticas (Un-sets). | Não |
| Cod_cores | string | Cores associadas a este símbolo (WUBRG), sem colchete/aspas. | Não |
| Desc_grafias_gatherer | string | Grafias alternativas usadas pelo Gatherer para este símbolo, sem colchete/aspas. | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou o registro de símbolo. | Não |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). 'NA' se ausente. | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |

## 7. Chave Única
`Cod_simbolo`. Coluna NOT NULL por natureza (todo símbolo tem sua própria notação) - `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (catálogo de referência, não tem "histórico" - cada execução sobrescreve com o catálogo atual da Scryfall via merge por `Cod_simbolo`).
- **Merge incremental:** por `Cod_simbolo`.
- **Particionamento:** nenhum (tabela pequena e estática - algumas dezenas de linhas).
- **Regra "sem `( ) { } no dado Silver"` - aplicada sem exceção a `Cod_simbolo`:** a notação nativa de símbolo da Scryfall usa chave (`{W}`), que é notação legítima do domínio, não um artefato de serialização. Mesmo assim, esta tabela converte para colchete (`[W]`) pela mesma regra usada em `TB_FATO_CARTAS`, garantindo que o mesmo símbolo tenha a mesma notação em toda a Silver e permita junção direta entre um token extraído do texto da carta e `Cod_simbolo`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-15 | Felipe | #115/#116: criação inicial (`TB_DOM_SIMBOLOS`, DAMA - DOM/REF), documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Tabela de referência pequena - útil para lookup/decode, não para agregação de volume.
