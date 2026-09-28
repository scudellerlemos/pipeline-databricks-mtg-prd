# Testes do aplicar_alvo, unico ponto onde dev e prd se diferenciam (os YAMLs
# sao os mesmos). Se ele errar, o job de prd grava no catalogo mtg_dev.

import copy
import importlib.util
import os
import sys

_CAMINHO = os.path.join(os.path.dirname(__file__), "deploy.py")

ALVO_PRD = {
    # parametros do deploy - mexem no job, nao chegam no cluster
    "MTG_JOB_SUFFIX": "_PRD",
    "MTG_GIT_URL": "https://github.com/scudellerlemos/pipeline-databricks-mtg-dev",
    "MTG_GIT_TAG": "v1.0.0",
    "MTG_PAUSE_STATUS": "UNPAUSED",
    # config do ambiente - vai pro cluster, onde obter_segredo le
    "MTG_ENVIRONMENT": "production",
    "MTG_CATALOG_NAME": "mtg_prod",
    "MTG_S3_STAGE_PREFIX": "prod/stage",
    "MTG_ALERT_EMAIL": "alerta@exemplo.com",
}

JOB_BASE = {
    "name": "MTG_GOLD",
    "tasks": [{"task_key": "gold_mercado_cartas"}],
    "job_clusters": [
        {
            "job_cluster_key": "Job_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            },
        }
    ],
    "git_source": {
        "git_url": "https://github.com/scudellerlemos/pipeline-databricks-mtg-dev",
        "git_provider": "gitHub",
        "git_branch": "main",
    },
    "schedule": {"quartz_cron_expression": "0 0 6 ? * 2#1", "pause_status": "UNPAUSED"},
    "tags": {"environment": "development"},
}


def _deploy_com_ambiente(ambiente):
    """Recarrega deploy.py com o ambiente dado (ALVO e lido no import).

    Limpa todas as MTG_* antes, porque CONFIG_DO_AMBIENTE pega qualquer MTG_* da maquina.
    """
    antigo = {k: v for k, v in os.environ.items() if k.startswith("MTG_")}
    for k in antigo:
        del os.environ[k]
    os.environ.update(ambiente)
    try:
        espec = importlib.util.spec_from_file_location("deploy_sob_teste", _CAMINHO)
        modulo = importlib.util.module_from_spec(espec)
        espec.loader.exec_module(modulo)
        return modulo
    finally:
        for k in [k for k in os.environ if k.startswith("MTG_")]:
            del os.environ[k]
        os.environ.update(antigo)


def test_sem_ambiente_o_job_fica_exatamente_como_esta_no_yaml():
    deploy = _deploy_com_ambiente({})
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert job["name"] == "MTG_GOLD"
    assert job["git_source"]["git_branch"] == "main"
    assert "git_tag" not in job["git_source"]
    assert job["tags"]["environment"] == "development"
    # sem config injetada, obter_segredo no notebook cai no scope compartilhado
    assert job["job_clusters"][0]["new_cluster"]["spark_env_vars"] == {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    }


def test_alvo_prd_renomeia_e_injeta_a_config():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert job["name"] == "MTG_GOLD_PRD"
    variaveis_ambiente = job["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    assert variaveis_ambiente["MTG_CATALOG_NAME"] == "mtg_prod"
    assert variaveis_ambiente["MTG_S3_STAGE_PREFIX"] == "prod/stage"
    # ativa a trava de catalogo do obter_segredo no cluster
    assert variaveis_ambiente["MTG_ENVIRONMENT"] == "production"
    # preserva as env vars que ja estavam no cluster
    assert variaveis_ambiente["PYSPARK_PYTHON"] == "/databricks/python3/bin/python3"
    assert job["tags"]["environment"] == "production"


def test_parametros_do_deploy_nao_vazam_pro_cluster():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    variaveis_ambiente = job["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    for parametro in ("MTG_JOB_SUFFIX", "MTG_GIT_URL", "MTG_GIT_TAG", "MTG_PAUSE_STATUS"):
        assert parametro not in variaveis_ambiente


def test_tag_substitui_branch_e_nunca_convivem():
    # git_source aceita branch OU tag (os dois = erro 400 da API).
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert job["git_source"]["git_tag"] == "v1.0.0"
    assert "git_branch" not in job["git_source"]


def test_status_pausa_do_alvo_vence_o_yaml():
    deploy = _deploy_com_ambiente({**ALVO_PRD, "MTG_PAUSE_STATUS": "PAUSED"})
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert job["schedule"]["pause_status"] == "PAUSED"


def test_orquestrador_sem_cluster_e_sem_git_nao_quebra():
    # MTG_PIPELINE so tem run_job_task: nao tem job_clusters nem git_source.
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo({"name": "MTG_PIPELINE", "tasks": [{"task_key": "rodar_stage"}]})

    assert job["name"] == "MTG_PIPELINE_PRD"



# ---------------------------------------------------------------------------
# campos_criticos / diferencas: usados antes do reset (o que vai ser
# sobrescrito) e depois (conferir o que chegou).
# ---------------------------------------------------------------------------


def test_alerta_de_falha_e_injetado_em_todo_job():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert job["email_notifications"]["on_failure"] == ["alerta@exemplo.com"]
    # parametro do deploy, nao vai pro cluster
    assert "MTG_ALERT_EMAIL" not in job["job_clusters"][0]["new_cluster"]["spark_env_vars"]


def test_sem_MTG_ALERT_EMAIL_o_job_sobe_sem_bloco_de_email():
    # evita mandar on_failure: [""] pro Databricks
    deploy = _deploy_com_ambiente({k: v for k, v in ALVO_PRD.items() if k != "MTG_ALERT_EMAIL"})
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    assert "email_notifications" not in job


def test_verificacao_pega_alerta_que_sumiu():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    enviado = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    lido = copy.deepcopy(enviado)
    lido["email_notifications"] = {}

    assert deploy.diferencas(
        deploy.campos_criticos(lido), deploy.campos_criticos(enviado)
    ) == ["alerta: [] -> ['alerta@exemplo.com']"]


def test_verificacao_pega_catalogo_que_nao_chegou_no_cluster():
    # sem MTG_CATALOG_NAME no cluster, o job de prd grava no mtg_dev.
    deploy = _deploy_com_ambiente(ALVO_PRD)
    enviado = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    lido = copy.deepcopy(enviado)
    del lido["job_clusters"][0]["new_cluster"]["spark_env_vars"]["MTG_CATALOG_NAME"]

    divergencias = deploy.diferencas(
        deploy.campos_criticos(lido), deploy.campos_criticos(enviado)
    )
    assert any("config_do_ambiente" in d for d in divergencias), divergencias


def test_verificacao_pega_job_que_ficou_na_branch():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    enviado = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    lido = copy.deepcopy(enviado)
    lido["git_source"].pop("git_tag")
    lido["git_source"]["git_branch"] = "main"

    divergencias = deploy.diferencas(
        deploy.campos_criticos(lido), deploy.campos_criticos(enviado)
    )
    assert divergencias == ["git_ref: 'main' -> 'v1.0.0'"]


def test_orquestrador_embute_as_camadas_num_cluster_so():
    # MTG_PIPELINE sobe com as tasks das 4 camadas e um job_cluster so; cada
    # camada espera a anterior inteira, e so a Stage herda o retry.
    deploy = _deploy_com_ambiente({})
    raiz = os.path.join(os.path.dirname(__file__), "..", "..")
    atual = os.getcwd()
    os.chdir(raiz)
    try:
        job = deploy.carregar_config_job(".github/DAGs/pipeline.yml", "MTG_PIPELINE")
    finally:
        os.chdir(atual)

    tarefas = {t["task_key"]: t for t in job["tasks"]}
    assert not any("run_job_task" in t for t in job["tasks"])
    assert len(job["job_clusters"]) == 1
    assert {t["job_cluster_key"] for t in job["tasks"]} == {job["job_clusters"][0]["job_cluster_key"]}
    assert tarefas["stage_precos"]["max_retries"] == 2 and "depends_on" not in tarefas["stage_precos"]
    assert "max_retries" not in tarefas["bronze_precos"]
    assert {d["task_key"] for d in tarefas["gold_mercado_cartas"]["depends_on"]} == {
        k for k in tarefas if k.startswith("silver_")}
    assert job["git_source"]["git_branch"] == "main"


def test_deploy_identico_nao_acusa_nada():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    enviado = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    # defaults que a API preenche nao contam como divergencia.
    lido = copy.deepcopy(enviado)
    lido["format"] = "MULTI_TASK"
    lido["timeout_seconds"] = 0
    # email_notifications fica de fora: com alerta configurado, {} da API e divergencia real.

    assert deploy.diferencas(
        deploy.campos_criticos(lido), deploy.campos_criticos(enviado)
    ) == []


def test_campos_criticos_ignora_variavel_ambiente_que_nao_e_nossa():
    deploy = _deploy_com_ambiente(ALVO_PRD)
    job = deploy.aplicar_alvo(copy.deepcopy(JOB_BASE))

    config = deploy.campos_criticos(job)["config_do_ambiente"]
    assert "PYSPARK_PYTHON" not in config
    assert config["MTG_CATALOG_NAME"] == "mtg_prod"


def test_job_sem_git_e_sem_cluster_nao_quebra_a_verificacao():
    deploy = _deploy_com_ambiente({})
    campos = deploy.campos_criticos({"name": "MTG_PIPELINE", "tasks": []})

    assert campos["git_ref"] is None
    assert campos["config_do_ambiente"] == {}

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_sem_ambiente_o_job_fica_exatamente_como_esta_no_yaml()
    test_alvo_prd_renomeia_e_injeta_a_config()
    test_parametros_do_deploy_nao_vazam_pro_cluster()
    test_tag_substitui_branch_e_nunca_convivem()
    test_status_pausa_do_alvo_vence_o_yaml()
    test_orquestrador_sem_cluster_e_sem_git_nao_quebra()
    test_alerta_de_falha_e_injetado_em_todo_job()
    test_sem_MTG_ALERT_EMAIL_o_job_sobe_sem_bloco_de_email()
    test_verificacao_pega_alerta_que_sumiu()
    test_verificacao_pega_catalogo_que_nao_chegou_no_cluster()
    test_verificacao_pega_job_que_ficou_na_branch()
    test_orquestrador_embute_as_camadas_num_cluster_so()
    test_deploy_identico_nao_acusa_nada()
    test_campos_criticos_ignora_variavel_ambiente_que_nao_e_nossa()
    test_job_sem_git_e_sem_cluster_nao_quebra_a_verificacao()
    print("OK")
