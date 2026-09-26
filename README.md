# pipeline-databricks-mtg-prd

Repo de **controle** de produção. Não guarda cópia do pipeline.

O código vive em
[`pipeline-databricks-mtg-dev`](https://github.com/scudellerlemos/pipeline-databricks-mtg-dev).
Aqui existe um arquivo só: `.github/workflows/deploy-prd.yml`, que faz checkout
daquele repo **numa tag** e roda o `deploy.py` dele com as variáveis de produção.
Assim não existe uma segunda cópia do pipeline pra divergir da primeira.

## Como produção é atualizada

1. No repo de código, `git tag v1.2.3 && git push --tags`.
2. O workflow `promote.yml` de lá dispara um `repository_dispatch` aqui.
3. Este workflow revalida a tag (testes + lint + DAG) e deploya os jobs
   `MTG_*_PRD` no mesmo workspace Databricks, apontando pro catálogo `mtg_prod`.

**Rollback:** Actions → *Deploy produção (MTG)* → *Run workflow* → tag anterior.

## O que precisa estar configurado

| Onde | O quê |
|---|---|
| Environment `Databricks-Prod` (aqui) | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| Secrets do repo de código | `PRD_DISPATCH_TOKEN` (PAT com `repo` neste repo) |

A configuração de produção que difere do dev (catálogo, prefixos S3) **não** está
em secret: está versionada no `env:` do workflow, e chega no cluster como
`spark_env_vars`. O `get_secret()` dos notebooks resolve
`env var > secret scope > default`.
