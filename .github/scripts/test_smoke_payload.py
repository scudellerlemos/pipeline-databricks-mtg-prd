# ponytail: o payload do smoke e montado a partir do gold.yml ja passado pelo
# apply_target. Se essa heranca quebrar, o smoke roda com o cluster/tag/config
# ERRADOS e passa - virando o pior tipo de teste: um que da verde sozinho.

import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(RAIZ, ".github", "scripts")

ALVO_PRD = {
    "MTG_JOB_SUFFIX": "_PRD",
    "MTG_GIT_TAG": "v1.0.0",
    "MTG_ENVIRONMENT": "production",
    "MTG_CATALOG_NAME": "mtg_prod",
}


def _smoke_com_env(env):
    """deploy.TARGET e smoke sao lidos no import - recarrega os dois."""
    antigo = {k: v for k, v in os.environ.items() if k.startswith("MTG_")}
    cwd = os.getcwd()
    for k in antigo:
        del os.environ[k]
    os.environ.update(env)
    sys.path.insert(0, SCRIPTS)
    for m in ("deploy", "smoke"):
        sys.modules.pop(m, None)
    try:
        os.chdir(RAIZ)  # montar_payload le .github/DAGs/gold.yml por caminho relativo
        import smoke

        return smoke
    finally:
        os.chdir(cwd)
        sys.path.remove(SCRIPTS)
        for k in [k for k in os.environ if k.startswith("MTG_")]:
            del os.environ[k]
        os.environ.update(antigo)


def test_smoke_herda_tag_e_config_do_alvo():
    smoke = _smoke_com_env(ALVO_PRD)
    cwd = os.getcwd()
    os.chdir(RAIZ)
    try:
        payload = smoke.montar_payload()
    finally:
        os.chdir(cwd)

    assert payload["run_name"] == "MTG_SMOKE_PRD"
    # roda a tag promovida, nao a main
    assert payload["git_source"]["git_tag"] == "v1.0.0"
    assert "git_branch" not in payload["git_source"]
    # e com o catalogo de prd no cluster - senao o smoke valida o mtg_dev
    env_vars = payload["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    assert env_vars["MTG_CATALOG_NAME"] == "mtg_prod"
    # a task tem que ser o smoke, nao a Gold de verdade
    assert payload["tasks"][0]["notebook_task"]["notebook_path"].endswith("smoke_deploy")
    assert payload["tasks"][0]["job_cluster_key"] == payload["job_clusters"][0]["job_cluster_key"]


def test_smoke_em_dev_fica_na_branch_e_sem_sufixo():
    smoke = _smoke_com_env({})
    cwd = os.getcwd()
    os.chdir(RAIZ)
    try:
        payload = smoke.montar_payload()
    finally:
        os.chdir(cwd)

    assert payload["run_name"] == "MTG_SMOKE"
    assert payload["git_source"]["git_branch"] == "main"


def test_o_notebook_do_smoke_existe():
    # notebook_path vai pro Databricks como caminho no repo: errar aqui so
    # aparece 4min depois, com o cluster ja de pe.
    smoke = _smoke_com_env({})
    assert os.path.exists(os.path.join(RAIZ, smoke.NOTEBOOK + ".py"))


if __name__ == "__main__":
    test_smoke_herda_tag_e_config_do_alvo()
    test_smoke_em_dev_fica_na_branch_e_sem_sufixo()
    test_o_notebook_do_smoke_existe()
    print("OK")
