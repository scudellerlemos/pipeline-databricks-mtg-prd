<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_MOV_MIGRACOES_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_MOV_MIGRACOES_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** MOV (movimentação) - registra um evento de mudança de identificador (`ID_CARTA_ANTIGO` -> `ID_CARTA_NOVO`), não um atributo de carta nem uma dimensão. Prefixo `TB_MOV_` sinaliza isso.

## 2. Descrição Completa
Histórico de migrações de id de carta feitas pela Scryfall (unificação de duplicatas, remoção de registros errados) e o id canônico já resolvido para cada carta afetada. A Scryfall ocasionalmente descobre que duas impressões cadastradas eram a mesma carta e as unifica sob um único id, ou remove um id criado por engano - quem consumia o `ID_CARTA` antigo precisa saber para qual id atual ele aponta, ou a analise fica presa em um id morto.

## 3. Origem dos Dados
- **Fonte (Bronze):** `migrations`
- **Localização:** `<catalog>.silver.TB_MOV_MIGRACOES_CARTAS` (Unity Catalog / Delta)

## 4. Linhagem dos Dados
- **Fluxo:**
  1. Scryfall API
  2. Ingestão para S3 (Stage)
  3. Processamento Bronze (`migrations`)
  4. Transformação Silver (`src/03 - Silver/Dev/TB_MOV_MIGRACOES_CARTAS.py`)
  5. Escrita na tabela Delta: `TB_MOV_MIGRACOES_CARTAS` (Unity Catalog)

## 5. Convenção de Nome de Coluna
Todas as colunas a partir da Silver são em PT-BR, sem acento, 100% MAIÚSCULAS, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| ID_MIGRACAO | string | Id único do registro de migração na Scryfall (id natural da fonte). | Sim |
| URL_SCRYFALL | string | URL do registro de migração na Scryfall. | Não |
| DT_EXECUCAO | date | Data em que a migração foi executada pela Scryfall. | Não |
| NME_ESTRATEGIA_MIGRACAO | string | Estratégia da migração: 'Unificacao' (duas cartas viraram uma) ou 'Remocao' (id descontinuado sem substituto). | Não |
| ID_CARTA_ANTIGO | string | Id que deixou de ser válido. | Não |
| ID_CARTA_NOVO | string | Id que substitui o antigo (NULO quando a estratégia é 'Remocao', sem substituto). | Não |
| DESC_NOTA | string | Nota explicativa da Scryfall sobre a migração, notação `[..]`. 'NA' se ausente. | Não |
| ID_CARTA_ASSOCIADA | string | Id de carta associado a este registro de migração, quando informado pela fonte. | Não |
| COD_IDIOMA | string | Idioma associado a este registro de migração. | Não |
| NME_CARTA_ASSOCIADA | string | Nome de carta associado a este registro de migração. | Não |
| COD_COLECAO_ASSOCIADA | string | Código de coleção associado a este registro de migração. Em maiúsculas, como COD_COLECAO - junta direto. | Não |
| ID_ORACLE_ASSOCIADO | string | Oracle id associado a este registro de migração. | Não |
| NUM_COLECIONADOR_ASSOCIADO | string | Número de colecionador associado a este registro de migração. | Não |
| ID_CARTA_CANONICO | string | Id final resolvido após seguir toda a cadeia de unificações a partir de `ID_CARTA_ANTIGO` (ex.: A->B->C resolve direto para C). Igual a `ID_CARTA_ANTIGO` quando não há migração de unificação para essa carta. | Não |
| DT_INGESTAO | timestamp | Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run). | Não |
| NME_FONTE | string | Fonte de dados de origem ('Scryfall'). 'NA' se ausente. | Não |
| DESC_URL_ORIGEM | string | Nome lógico da tabela de origem na Stage (sempre `migrations`), não a URL da API. | Não |
| DESC_ARQUIVO_ORIGEM | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| ID_EXECUCAO_BRONZE | string | Id da execução da Bronze que gravou a linha. | Não |
| DT_INGESTAO_BRONZE | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| ANO_EXECUCAO | int | Ano derivado de DT_EXECUCAO (partição física). | Não |
| MES_EXECUCAO | int | Mês derivado de DT_EXECUCAO (partição física). | Não |

## 7. Chave Única
`ID_MIGRACAO`. Id natural, sempre presente na fonte (toda migração tem um id próprio na Scryfall) - coluna NOT NULL, `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (histórico de migração é útil por completo).
- **Merge incremental:** por `ID_MIGRACAO`, desempate por `DT_INGESTAO` mais recente.
- **Particionamento:** por `ANO_EXECUCAO` e `MES_EXECUCAO`.
- **Resolução de cadeia (`ID_CARTA_CANONICO`):** relocada de `TB_FATO_CARTAS.py` (#135/#136) para este notebook nesta revisão. Segue a cadeia de unificações em Python puro (`_resolve_id_chain`, máx. 10 saltos, seguro contra ciclo) a partir das migrações com `NME_ESTRATEGIA_MIGRACAO = 'Unificacao'` e `ID_CARTA_NOVO` preenchido - testado isoladamente em `test_migration_chain.py`.
- **Regra "sem `( ) { }` no dado Silver":** `DESC_NOTA` converte `{...}`/`(...)` para `[...]`, mesma regra de `TB_FATO_CARTAS`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-08 | Felipe | AUD-20 (#135): implementação inicial de `_resolve_id_chain`/`attach_canonical_id` dentro de `TB_FATO_SILVER_CARDS.py` |
| 2026-09-15 | Felipe | #115/#116: criada como tabela própria `TB_MOV_MIGRACOES_CARTAS` (DAMA - MOV), lógica de resolução de cadeia relocada de `TB_FATO_CARTAS.py`, documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Consumidores Gold que agrupam/janelam uma carta através de uma migração de id devem usar `ID_CARTA_CANONICO`, não `ID_CARTA_ANTIGO`/`ID_CARTA_NOVO` diretamente.
- `ID_CARTA_NOVO` NULO é esperado para `NME_ESTRATEGIA_MIGRACAO = 'Remocao'` - não é dado faltante.
