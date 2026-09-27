# 📐 Decisões de Arquitetura (ADR)

Registro das decisões que moldam o projeto: o que foi decidido, por quê, e o que
se aceitou em troca. Decisão nova entra no fim com o próximo número; decisão
revista não é apagada, ganha status **Substituída por ADR-N**.

Contexto que vale para todas: **projeto solo**, um único workspace Databricks,
volume de dados pequeno (catálogo Scryfall inteiro cabe em um download) e carga
mensal. Várias escolhas aqui seriam diferentes numa equipe ou numa empresa, e
isso está dito em cada uma.

| # | Decisão | Status |
|---|---------|--------|
| [001](#adr-001--camadas-stage--bronze--silver--gold) | Camadas Stage → Bronze → Silver → Gold | Aceita |
| [002](#adr-002--full-load-mensal) | Full load mensal | Aceita |
| [003](#adr-003--bronze-append-only-silver-e-gold-com-merge) | Bronze append-only, Silver e Gold com MERGE | Aceita |
| [004](#adr-004--jobs-em-yaml--deploypy-sem-asset-bundles) | Jobs em YAML + `deploy.py`, sem Asset Bundles | Aceita |
| [005](#adr-005--dev-e-prd-no-mesmo-workspace-diferença-só-por-env-var) | Dev e prd no mesmo workspace, diferença só por env var | Aceita |
| [006](#adr-006--repo-de-produção-separado-alimentado-por-snapshot) | Repo de produção separado, alimentado por snapshot | Aceita |
| [007](#adr-007--merge-na-main--produção) | Merge na `main` = produção | Aceita |
| [008](#adr-008--tag-imutável-por-publicação-rollback-por-redeploy) | Tag imutável por publicação, rollback por redeploy | Aceita |
| [009](#adr-009--workflow-de-prd-versionado-no-repo-de-dev) | Workflow de prd versionado no repo de dev | Aceita |
| [010](#adr-010--validação-em-camadas-ci-estático--smoke-test) | Validação em camadas: CI estático + smoke test | Aceita |
| [011](#adr-011--publicação-com-código-novo-roda-o-pipeline-de-prd) | Publicação com código novo roda o pipeline de prd | Aceita |

---

## ADR-001 — Camadas Stage → Bronze → Silver → Gold

**Contexto.** A fonte (Scryfall) muda de schema sem aviso e o consumo final é
analítico (BI, Genie). Misturar coleta, limpeza e regra de negócio num passo só
torna impossível reprocessar uma etapa sem refazer as outras.

**Decisão.**
- **Stage** (`src/01 - Ingestion`): coleta da API e grava Parquet no S3 com o mínimo
  de tratamento (schema explícito, janela de anos, colunas técnicas de ingestão),
  sem regra de negócio. Sem tabela no Unity Catalog.
- **Bronze** (`src/02 - Bronze`): EL puro — lê o Parquet e grava Delta 1:1, sem
  regra de negócio.
- **Silver** (`src/03 - Silver`): limpeza, tipagem e nomenclatura PT-BR com
  classificação DAMA-DMBOK (`TB_FATO_*`, `TB_DIM_*`, ...).
- **Gold** (`src/04 - Gold`): uma visão de mercado pronta para consumo
  (`TB_FATO_MERCADO_CARTAS`).

Cada camada é um job (`MTG_STAGE`, `MTG_BRONZE`, `MTG_SILVER`, `MTG_GOLD`) e o
`MTG_PIPELINE` orquestra a ordem via `run_job_task`.

**Consequências.** Qualquer camada reprocessa sozinha a partir da anterior. O
custo é armazenar o dado em cada camada (Parquet na Stage + Delta em Bronze/Silver/Gold), irrelevante no volume atual.

## ADR-002 — Full load mensal

**Contexto.** A Scryfall não expõe incrementalidade real: o bulk-data é sempre o
catálogo inteiro. Preço muda diariamente, mas a análise é de tendência.

**Decisão.** Carga completa a cada execução, agendada no `MTG_PIPELINE` para a
1ª segunda-feira do mês, 6h (`America/Sao_Paulo`). O nome do arquivo da Stage é
`{ano}_{mes}_{dia}_{tabela}.parquet`: `{dia}` é sempre o dia da execução
(rastreabilidade e idempotência por dia). Em `sets` e `card_prices`, `{ano}_{mes}`
vêm do `releaseDate` e há uma janela de anos sobre ele; nas demais, da própria
execução.

**Consequências.** Lógica simples, sem estado de "até onde já li". O histórico
de preço tem uma coleta por execução agendada (mensal) — mais nas publicações
com código novo
([ADR-011](#adr-011--publicação-com-código-novo-roda-o-pipeline-de-prd)).
Exceção conhecida: em `sets` e `card_prices` o nome do arquivo só tem o dia da
execução (ano/mês vêm do `releaseDate`), então duas execuções em meses
diferentes no mesmo dia do mês colidem e a segunda pula a coleta (ex.:
2027-02-01 e 2027-03-01, ambas 1ª segunda-feira). Se
precisar de diária, é só mudar o cron.

## ADR-003 — Bronze append-only, Silver e Gold com MERGE

**Contexto.** O pipeline precisa poder rodar de novo — por falha, por deploy,
por curiosidade — sem duplicar dado.

**Decisão.**
- **Bronze** grava em `append` com `mergeSchema` e só lê arquivos da Stage cujo
  `source_file` ainda não está na tabela. Nada é deduplicado nem sobrescrito:
  a Bronze é o histórico bruto.
- **Silver** e **Gold** deduplicam a origem pela chave de negócio (`row_number`
  na Silver quando há coluna de ordenação, senão `dropDuplicates`; `dropDuplicates` na Gold) e gravam com merge do Delta (`DeltaTable.merge`) por essa
  chave. Na primeira carga, sem tabela ainda, é `overwrite`.

**Consequências.** Toda camada é idempotente, o que viabiliza a [ADR-011](#adr-011--publicação-com-código-novo-roda-o-pipeline-de-prd)
(rodar a cada publicação). A Bronze só cresce; se o volume algum dia pesar, a
saída é retenção/`VACUUM`, não mudar o modo de escrita.

## ADR-004 — Jobs em YAML + `deploy.py`, sem Asset Bundles

**Contexto.** São 5 jobs, com o orquestrador referenciando os outros 4 por
`job_id`, que só existe depois do deploy.

**Decisão.** Cada job é um YAML em `.github/DAGs/`. O `.github/scripts/deploy.py`
faz o deploy na ordem Stage → Bronze → Silver → Gold → Pipeline, troca os
placeholders `{{MTG_*_JOB_ID}}` pelos IDs reais e usa `jobs reset` (settings
inteiras, sem drift de alteração manual pela UI).

**Alternativa descartada.** Databricks Asset Bundles resolvem as referências
nativamente e seriam o caminho numa empresa (um bundle por projeto, deploy só
do que mudou). Para 5 jobs, o script é menor que a configuração do bundle.

**Consequências.** Mudança na UI do Databricks é apagada no próximo deploy —
é o comportamento desejado. Migrar para bundles fica aberto se o número de
projetos crescer.

## ADR-005 — Dev e prd no mesmo workspace, diferença só por env var

**Contexto.** Projeto solo, um workspace, um bucket, uma identidade. Criar
outro workspace duplicaria custo e administração sem ganho real de isolamento
para uma pessoa só.

**Decisão.** Os dois ambientes usam o mesmo código, o mesmo instance pool e o
mesmo secret scope (`mtg-pipeline`). O que difere viaja como env var `MTG_*`
no workflow e o `deploy.py` injeta no job:

| Knob | Dev | Prd |
|------|-----|-----|
| Nome dos jobs (`MTG_JOB_SUFFIX`) | `MTG_*` | `MTG_*_PRD` |
| Catálogo (`MTG_CATALOG_NAME`) | `mtg_dev` | `mtg_prod` |
| Dados (`MTG_S3_BUCKET`) | `s3://magicthegatheringdev/dev` | `s3://magicthegatheringdev/prd` |
| Código lido pelos jobs | branch `main` do repo de dev | tag `prd-*` do repo de prd |
| Schedule (`MTG_PAUSE_STATUS`) | `PAUSED` | `UNPAUSED` |
| Alerta de falha (`MTG_ALERT_EMAIL`) | variável de repositório `MTG_ALERT_EMAIL` do repo de dev (vazia = sem alerta) | variável de repositório `MTG_ALERT_EMAIL` do repo de prd |

Em dev, fora o alerta, nenhuma dessas env vars é definida: vale o YAML (`PAUSED`, sem sufixo,
`git_branch: main`) e o secret scope `mtg-pipeline` (catálogo `mtg_dev` como
default, bucket `.../dev`). Só prd sobrescreve.

`get_secret()` resolve na ordem env var > secret > default, e a trava
`_barra_catalogo_de_dev_em_producao` (`src/00 - Common/Dev/base_utils.py`)
explode se um job de produção resolver o catálogo para `mtg_dev`.

**Consequências.** Isolamento é lógico (catálogo, prefixo S3, nome de job), não
físico: um erro de permissão no workspace afeta os dois. Aceito por desenho.
Numa empresa, workspace e identidade separados por ambiente.

## ADR-006 — Repo de produção separado, alimentado por snapshot

**Contexto.** No repo de dev há liberdade total: branch de qualquer coisa,
force-push, tag apagada. Se os jobs de prod lessem do repo de dev, qualquer uma
dessas ações poderia mudar ou derrubar produção.

**Decisão.** Os jobs `_PRD` leem código de
`scudellerlemos/pipeline-databricks-mtg-prd`. A cada publicação, o
`promote.yml` copia o repo de dev inteiro para lá (`rsync --delete`, sem
`.github/workflows`), commita como `github-actions[bot]` e cria a tag. A
`main` e as tags `prd-*` do repo de prd são protegidas por rulesets (sem
apagar, sem force-push, tag imutável).

**Alternativa descartada.** Copiar só os arquivos alterados. O snapshot já é
incremental na prática — o commit no repo de prd só contém o diff — e evita que
os dois repos divirjam por um arquivo esquecido.

**Consequências.** Produção só muda por uma promoção. O repo de prd leva
arquivos que não executam (docs, testes); aceitável no tamanho atual, com
allowlist de pastas como evolução se incomodar.

## ADR-007 — Merge na `main` = produção

**Contexto.** Sem outro revisor, aprovação manual e tag manual eram só cliques
a mais: o merge já é a decisão de publicar.

**Decisão.** `promote.yml` dispara por `workflow_run` quando o CI da `main`
termina verde em um `push`. Publica o commit validado (`head_sha`), não a ponta
da `main` naquele momento. O environment `Databricks-Prod` não tem required
reviewer.

O push na `main` só roda o CI quando mexe em `src/`, `.github/DAGs`,
`.github/workflows`, `.github/scripts`, `.github/prd` ou
`.github/requirements-ci.txt`. Merge só de documentação não publica: o README
e o `docs/` chegam ao repo de prd na próxima publicação de código.

**Consequências.** Caminho único e rápido até produção. A proteção passa a ser
o CI verde e a revisão do PR. Numa equipe, o gate de aprovação no environment
volta.

## ADR-008 — Tag imutável por publicação, rollback por redeploy

**Decisão.** Cada publicação gera `prd-AAAAMMDD-HHMM-<sha7>` (data UTC + commit
de dev de origem). Os jobs apontam para a tag (`git_tag`), não para a branch.
Rollback = `workflow_dispatch` do `deploy-prd.yml` no repo de prd com a tag
anterior.

**Consequências.** Produção roda exatamente o código publicado, mesmo que a
`main` de prd receba commits novos no meio de uma execução. O nome da tag diz
quando e de qual commit de dev veio.

## ADR-009 — Workflow de prd versionado no repo de dev

**Contexto.** O workflow do repo de prd era editado à mão lá e divergiu (faltou
`requests` e o deploy quebrou).

**Decisão.** A cópia de referência vive em `.github/prd/deploy-prd.yml` no
repo de dev, passa pelo mesmo PR que o código, e o `promote.yml` a copia para
`.github/workflows/` do repo de prd a cada publicação. Dependências Python de
CI e deploy ficam num lugar só: `.github/requirements-ci.txt`.

**Consequências.** Edição manual no repo de prd é sobrescrita na próxima
publicação (o ruleset não impede o commit, só o force-push). O PAT `PRD_DISPATCH_TOKEN` precisa de
permissão *Workflows* além de *Contents*.

## ADR-010 — Validação em camadas: CI estático + smoke test

**Decisão.**
- **CI** (`validate-pipeline.yml`, todo PR e push de código na `main`): sintaxe dos YAMLs,
  estrutura da DAG e config de cluster (`validate_dag.py`), lint das células
  Python dos notebooks (`lint_notebooks.py`) e `pytest`.
- **Smoke test** (`smoke.py`, depois de todo deploy, dev e prd): `jobs submit`
  de um notebook que grava e lê no catálogo do ambiente, com a mesma config do
  job recém-deployado.
- **Validade das credenciais** (`check-credenciais.yml`, mensal): falha quando
  um token está a menos de 60 dias de expirar.

**Consequências.** Pega erro de estrutura, sintaxe e ambiente antes da carga.
**Não** pega bug de lógica de transformação: nenhum notebook de camada roda
antes de chegar em prd. Próximo passo natural: rodar o pipeline em dev sobre
uma amostra antes de promover.

## ADR-011 — Publicação com código novo roda o pipeline de prd

**Contexto.** Com carga mensal, código novo publicado ficava até um mês sem
processar dado, e um bug só aparecia na próxima 1ª segunda-feira.

**Decisão.** Depois do deploy e do smoke test, o `deploy-prd.yml` dispara o
`MTG_PIPELINE_PRD` (`run-now --no-wait`) quando o snapshot da publicação mudou
algum arquivo (código ou não). Publicação vazia (nada mudou no snapshot) só redeploya. Rollback manual
roda só com o input `rodar` marcado.

**Consequências.** Código novo é exercitado na hora, com dado real. Rodar a
mais é seguro pela [ADR-003](#adr-003--bronze-append-only-silver-e-gold-com-merge).
Falha da carga avisa pelo e-mail de alerta do job, não pelo GitHub — o workflow
termina antes da carga.
