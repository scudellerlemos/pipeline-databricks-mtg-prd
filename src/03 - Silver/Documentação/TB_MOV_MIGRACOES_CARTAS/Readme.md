<div align="center">
<!-- Imagem ilustrativa da tabela (adicione o link abaixo) -->
<img src="https://i.postimg.cc/jjvN23QK/remote-image.png" alt="Imagem de documentação" width="600"/>
</div>
<br>

# TB_MOV_MIGRACOES_CARTAS

## 1. Nome da Tabela e Camada
- **Tabela:** TB_MOV_MIGRACOES_CARTAS
- **Camada:** Silver
- **Classificação DAMA-DMBOK:** MOV (movimentação) - registra um evento de mudança de identificador (`Id_carta_antigo` -> `Id_carta_novo`), não um atributo de carta nem uma dimensão. Prefixo `TB_MOV_` sinaliza isso.

## 2. Descrição Completa
Histórico de migrações de id de carta feitas pela Scryfall (unificação de duplicatas, remoção de registros errados) e o id canônico já resolvido para cada carta afetada. A Scryfall ocasionalmente descobre que duas impressões cadastradas eram a mesma carta e as unifica sob um único id, ou remove um id criado por engano - quem consumia o `Id_carta` antigo precisa saber para qual id atual ele aponta, ou a analise fica presa em um id morto.

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
Todas as colunas a partir da Silver são em PT-BR, sem acento, com a primeira letra maiúscula e o restante minúsculo, mesma convenção de `TB_FATO_CARTAS`.

## 6. Schema Detalhado
| Nome da Coluna | Tipo | Descrição | Chave |
|---|---|---|---|
| Id_migracao | string | Id único do registro de migração na Scryfall (id natural da fonte). | Sim |
| Url_scryfall | string | URL do registro de migração na Scryfall. | Não |
| Dt_execucao | timestamp | Timestamp em que a migração foi executada pela Scryfall. | Não |
| Nme_estrategia_migracao | string | Estratégia da migração: 'Unificacao' (duas cartas viraram uma) ou 'Remocao' (id descontinuado sem substituto). | Não |
| Id_carta_antigo | string | Id que deixou de ser válido. | Não |
| Id_carta_novo | string | Id que substitui o antigo (NULO quando a estratégia é 'Remocao', sem substituto). | Não |
| Desc_nota | string | Nota explicativa da Scryfall sobre a migração, notação `[..]`. 'NA' se ausente. | Não |
| Id_carta_associada | string | Id de carta associado a este registro de migração, quando informado pela fonte. | Não |
| Cod_idioma | string | Idioma associado a este registro de migração. | Não |
| Nme_carta_associada | string | Nome de carta associado a este registro de migração. | Não |
| Cod_colecao_associada | string | Código de coleção associado a este registro de migração. | Não |
| Id_oracle_associado | string | Oracle id associado a este registro de migração. | Não |
| Num_colecionador_associado | string | Número de colecionador associado a este registro de migração. | Não |
| Id_carta_canonico | string | Id final resolvido após seguir toda a cadeia de unificações a partir de `Id_carta_antigo` (ex.: A->B->C resolve direto para C). Igual a `Id_carta_antigo` quando não há migração de unificação para essa carta. | Não |
| Dt_ingestao | timestamp | Timestamp em que a Stage coletou o registro de migração. | Não |
| Nme_fonte | string | Fonte de dados de origem ('scryfall'). 'NA' se ausente. | Não |
| Desc_url_origem | string | Endpoint/URL da API de origem. | Não |
| Desc_arquivo_origem | string | Caminho do arquivo Parquet de origem na Stage. | Não |
| Id_execucao_bronze | string | Id da execução da Bronze que gravou a linha. | Não |
| Dt_ingestao_bronze | timestamp | Timestamp em que a Bronze processou o registro. | Não |
| Ano_execucao | int | Ano derivado de Dt_execucao (partição física). | Não |
| Mes_execucao | int | Mês derivado de Dt_execucao (partição física). | Não |

## 7. Chave Única
`Id_migracao`. Id natural, sempre presente na fonte (toda migração tem um id próprio na Scryfall) - coluna NOT NULL, `PRIMARY KEY` real no Unity Catalog.

## 8. Regras de Implementação
- **Filtro temporal:** não aplicado (histórico de migração é útil por completo).
- **Merge incremental:** por `Id_migracao`, desempate por `Dt_ingestao` mais recente.
- **Particionamento:** por `Ano_execucao` e `Mes_execucao`.
- **Resolução de cadeia (`Id_carta_canonico`):** relocada de `TB_FATO_CARTAS.py` (#135/#136) para este notebook nesta revisão. Segue a cadeia de unificações em Python puro (`_resolve_id_chain`, máx. 10 saltos, seguro contra ciclo) a partir das migrações com `Nme_estrategia_migracao = 'Unificacao'` e `Id_carta_novo` preenchido - testado isoladamente em `test_migration_chain.py`.
- **Regra "sem `( ) { }` no dado Silver":** `Desc_nota` converte `{...}`/`(...)` para `[...]`, mesma regra de `TB_FATO_CARTAS`.

## 9. Histórico de Alterações
| Data | Responsável | Alteração |
|---|---|---|
| 2026-09-08 | Felipe | AUD-20 (#135): implementação inicial de `_resolve_id_chain`/`attach_canonical_id` dentro de `TB_FATO_SILVER_CARDS.py` |
| 2026-09-15 | Felipe | #115/#116: criada como tabela própria `TB_MOV_MIGRACOES_CARTAS` (DAMA - MOV), lógica de resolução de cadeia relocada de `TB_FATO_CARTAS.py`, documentação de colunas de negócio no Unity Catalog |

## 10. Observações
- Pipeline exibe logs detalhados de transformações aplicadas.
- Consumidores Gold que agrupam/janelam uma carta através de uma migração de id devem usar `Id_carta_canonico`, não `Id_carta_antigo`/`Id_carta_novo` diretamente.
- `Id_carta_novo` NULO é esperado para `Nme_estrategia_migracao = 'Remocao'` - não é dado faltante.
