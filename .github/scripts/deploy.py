#!/usr/bin/env python3
"""
Deploy dos jobs do pipeline no Databricks.

Sem Asset Bundles, nao ha interpolacao de job_id entre jobs. O orquestrador
(MTG_PIPELINE) chama os 4 jobs de camada via run_job_task.job_id, entao eles
sao deployados primeiro e seus job_ids substituem os placeholders
"{{MTG_STAGE_JOB_ID}}" etc. do pipeline.yml.
"""

import yaml
import json
import subprocess
import sys
import os
from datetime import datetime

# (arquivo, job_key). O orquestrador vai por ultimo: precisa dos job_ids dos outros.
DEPLOY_ORDER = [
    (".github/DAGs/stage.yml", "MTG_STAGE"),
    (".github/DAGs/bronze.yml", "MTG_BRONZE"),
    (".github/DAGs/silver.yml", "MTG_SILVER"),
    (".github/DAGs/gold.yml", "MTG_GOLD"),
    (".github/DAGs/pipeline.yml", "MTG_PIPELINE"),
]

JSON_TMP = "job_deploy.json"

# ---------------------------------------------------------------------------
# ALVO DE DEPLOY
# ---------------------------------------------------------------------------
# dev e prd usam o mesmo workspace e o mesmo codigo (o repo de prd recebe uma
# copia via promote.yml). O que diferencia os dois vem de env var do workflow
# e e aplicado no job antes do POST, nunca no YAML.
TARGET = {
    # sufixo no nome do job: "" em dev, "_PRD" em producao
    "suffix": os.environ.get("MTG_JOB_SUFFIX", ""),
    "git_url": os.environ.get("MTG_GIT_URL", ""),
    # Tag imutavel: rollback = redeployar a tag anterior. Vazio = usa o git_branch do YAML.
    "git_tag": os.environ.get("MTG_GIT_TAG", ""),
    "environment": os.environ.get("MTG_ENVIRONMENT", ""),
    # jobs reset sobrescreve as settings inteiras; pausa feita pela UI nao sobrevive ao deploy.
    "pause_status": os.environ.get("MTG_PAUSE_STATUS", ""),
    # Email de alerta de falha. Vazio = job sem email_notifications (o deploy avisa).
    "alert_email": os.environ.get("MTG_ALERT_EMAIL", ""),
}


# Env vars MTG_* que so configuram o job; nao vao pro cluster.
KNOBS_DO_DEPLOY = {
    "MTG_JOB_SUFFIX",
    "MTG_GIT_URL",
    "MTG_GIT_TAG",
    "MTG_PAUSE_STATUS",
    "MTG_ALERT_EMAIL",
}


def job_name(job_key):
    return job_key + TARGET["suffix"]


# Env vars MTG_* repassadas ao cluster (spark_env_vars).
# get_secret() resolve env var > secret > default. dev e prd dividem o mesmo
# secret scope (config de dev); o que prd sobrescreve vem daqui, versionado no workflow.
# MTG_ENVIRONMENT tambem vai: e o que ativa, no get_secret, a trava que impede
# producao de usar o catalogo mtg_dev.
CONFIG_DO_AMBIENTE = {
    k: v
    for k, v in os.environ.items()
    if k.startswith("MTG_") and k not in KNOBS_DO_DEPLOY and v
}


def apply_target(job_config):
    """Aplica o alvo (nome, config, git ref, tag, schedule) no job do YAML."""
    job_config["name"] = job_name(job_config["name"])

    if CONFIG_DO_AMBIENTE:
        for cluster in job_config.get("job_clusters", []):
            env_vars = cluster.setdefault("new_cluster", {}).setdefault("spark_env_vars", {})
            env_vars.update(CONFIG_DO_AMBIENTE)

    git_source = job_config.get("git_source")
    if git_source:
        if TARGET["git_url"]:
            git_source["git_url"] = TARGET["git_url"]
        if TARGET["git_tag"]:
            # git_source aceita branch OU tag, nunca os dois
            git_source.pop("git_branch", None)
            git_source["git_tag"] = TARGET["git_tag"]

    if TARGET["environment"]:
        job_config.setdefault("tags", {})["environment"] = TARGET["environment"]

    if TARGET["pause_status"] and "schedule" in job_config:
        job_config["schedule"]["pause_status"] = TARGET["pause_status"]

    # Vai em todos os jobs, inclusive o orquestrador: uma falha de camada gera
    # 2 emails (camada + orquestrador). Se incomodar, filtrar por "schedule" in job_config.
    if TARGET["alert_email"]:
        job_config["email_notifications"] = {
            "on_failure": [TARGET["alert_email"]],
            "no_alert_for_skipped_runs": True,
        }

    return job_config


def log(message, level="INFO"):
    """Função para logging padronizado"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def load_job_config(yaml_path, job_key, job_ids_by_key=None):
    """Le o YAML, extrai o job e (se for o orquestrador) substitui os
    placeholders de job_id pelos IDs reais ja deployados."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    if "resources" not in yaml_data or "jobs" not in yaml_data["resources"]:
        raise ValueError(f"{yaml_path}: resources.jobs não encontrado")
    if job_key not in yaml_data["resources"]["jobs"]:
        raise ValueError(f"{yaml_path}: job {job_key} não encontrado")

    job_config = yaml_data["resources"]["jobs"][job_key]

    if job_ids_by_key:
        for task in job_config.get("tasks", []):
            if "run_job_task" not in task:
                continue
            placeholder = task["run_job_task"].get("job_id", "")
            for referenced_key, referenced_id in job_ids_by_key.items():
                token = "{{" + f"{referenced_key}_JOB_ID" + "}}"
                if placeholder == token:
                    task["run_job_task"]["job_id"] = referenced_id

    return apply_target(job_config)


def write_json(job_config):
    json_content = json.dumps(job_config, indent=2, ensure_ascii=False)
    with open(JSON_TMP, "w", encoding="utf-8") as f:
        f.write(json_content)
    return json_content


def get_existing_job_id(job_name, is_new_cli=False):
    """Obtém o ID do job existente pelo nome, se houver"""
    try:
        output_flag = "json" if is_new_cli else "JSON"
        result = subprocess.run(
            ["databricks", "jobs", "list", "--output", output_flag],
            capture_output=True,
            text=True,
            check=True,
        )
        jobs_data = json.loads(result.stdout)
        jobs_list = jobs_data if isinstance(jobs_data, list) else jobs_data.get("jobs", [])
        for job in jobs_list:
            if job.get("settings", {}).get("name") == job_name:
                job_id = job.get("job_id")
                log(f"Job existente encontrado: {job_name} (ID {job_id})")
                return job_id
        log(f"Job {job_name} não existe ainda, será criado")
        return None
    except subprocess.CalledProcessError as e:
        log(f"Erro ao listar jobs: {e.stderr}", "WARN")
        return None
    except Exception as e:
        log(f"Erro inesperado ao listar jobs: {e}", "WARN")
        return None


def campos_criticos(settings):
    """Campos das settings que o deploy decide e que precisam bater apos o deploy.

    Nao compara as settings inteiras porque a API preenche defaults
    (format, timeout_seconds, ...) e o diff vira ruido.
    """
    git = settings.get("git_source") or {}
    env_vars = {}
    for cluster in settings.get("job_clusters", []):
        env_vars.update((cluster.get("new_cluster") or {}).get("spark_env_vars", {}))
    return {
        "name": settings.get("name"),
        "git_url": git.get("git_url"),
        "git_ref": git.get("git_tag") or git.get("git_branch"),
        "pause_status": (settings.get("schedule") or {}).get("pause_status"),
        "config_do_ambiente": {k: v for k, v in env_vars.items() if k.startswith("MTG_")},
        # A API devolve email_notifications: {} quando nao mandamos nada; normaliza pra [].
        "alerta": (settings.get("email_notifications") or {}).get("on_failure") or [],
        "run_job_task_ids": sorted(
            str(t["run_job_task"].get("job_id"))
            for t in settings.get("tasks", [])
            if "run_job_task" in t
        ),
    }


def diferencas(de, para):
    """['campo: valor_antigo -> valor_novo'] entre dois campos_criticos()."""
    return [f"{k}: {de[k]!r} -> {para[k]!r}" for k in para if de.get(k) != para[k]]


def fetch_job(job_id, is_new_cli):
    """Settings atuais do job, ou None se nao der pra ler."""
    try:
        cmd = ["databricks", "jobs", "get"]
        cmd += [str(job_id)] if is_new_cli else ["--job-id", str(job_id)]
        cmd += ["--output", "json" if is_new_cli else "JSON"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout).get("settings")
    except Exception as e:
        log(f"Não consegui ler o job {job_id}: {e}", "WARN")
        return None


def validate_databricks_connection():
    """Valida a conexão com o Databricks"""
    try:
        log("Testando conexão com Databricks...")
        result = subprocess.run(["databricks", "--version"], capture_output=True, text=True, check=True)
        version_output = result.stdout.strip()
        log(f"Databricks CLI version: {version_output}")

        is_new_cli = version_output.startswith("Databricks CLI v")
        is_old_cli = version_output.startswith("Version ")

        if is_old_cli:
            try:
                subprocess.run(
                    ["databricks", "jobs", "configure", "--version", "2.1"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                log("CLI antiga configurada para Jobs API 2.1")
            except subprocess.CalledProcessError as e:
                log(f"Configuração falhou: {e.stderr}", "WARN")
                os.environ["DATABRICKS_JOBS_API_VERSION"] = "2.1"

        if is_new_cli:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        else:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        log("Conexão com workspace estabelecida")

        return True, is_new_cli
    except subprocess.CalledProcessError as e:
        log(f"Erro na conexão com Databricks (exit {e.returncode}): stdout={e.stdout!r} stderr={e.stderr!r}", "ERROR")
        return False, False
    except Exception as e:
        log(f"Erro inesperado na validação: {e}", "ERROR")
        return False, False


def deploy_one_job(yaml_path, job_key, is_new_cli, job_ids_by_key):
    """Deploya (cria ou atualiza) um job e retorna seu job_id."""
    log(f"Lendo {yaml_path} ({job_key} -> {job_name(job_key)})...")
    job_config = load_job_config(yaml_path, job_key, job_ids_by_key)
    write_json(job_config)

    existing_id = get_existing_job_id(job_config["name"], is_new_cli)

    env = os.environ.copy()
    if not is_new_cli:
        env["DATABRICKS_JOBS_API_VERSION"] = "2.1"

    with open(JSON_TMP, "r", encoding="utf-8") as f:
        json_content = f.read()

    if existing_id:
        log(f"Atualizando job existente: {job_config['name']} (ID {existing_id})")
        # jobs reset sobrescreve as settings inteiras (o YAML e a fonte da verdade);
        # loga o que foi alterado pela UI e vai ser desfeito.
        atual = fetch_job(existing_id, is_new_cli)
        if atual:
            for drift in diferencas(campos_criticos(atual), campos_criticos(job_config)):
                log(f"Sobrescrevendo {job_config['name']} · {drift}", "WARN")
        if is_new_cli:
            write_json({"job_id": existing_id, "new_settings": job_config})
            result = subprocess.run(
                ["databricks", "jobs", "reset", "--json", f"@{JSON_TMP}"],
                capture_output=True, text=True, check=True, env=env,
            )
        else:
            result = subprocess.run(
                ["databricks", "jobs", "reset", "--job-id", str(existing_id), "--json-file", JSON_TMP],
                capture_output=True, text=True, check=True, env=env,
            )
        job_id = existing_id
    else:
        log(f"Criando novo job: {job_config['name']}")
        if is_new_cli:
            result = subprocess.run(
                ["databricks", "jobs", "create", "--json", f"@{JSON_TMP}"],
                capture_output=True, text=True, check=True, env=env,
            )
        else:
            result = subprocess.run(
                ["databricks", "jobs", "create", "--json-file", JSON_TMP],
                capture_output=True, text=True, check=True, env=env,
            )
        try:
            job_id = json.loads(result.stdout).get("job_id")
        except Exception:
            job_id = None

    log(f"Resposta do Databricks para {job_key}: {result.stdout.strip()[:300]}")
    if job_id is None:
        raise RuntimeError(f"Não foi possível determinar o job_id de {job_key} após o deploy")

    log(f"{job_key} -> job_id {job_id}")
    return job_id, job_config


def deploy_all():
    connection_success, is_new_cli = validate_databricks_connection()
    if not connection_success:
        return False, is_new_cli, {}

    job_ids_by_key = {}
    enviado_por_id = {}
    try:
        for yaml_path, job_key in DEPLOY_ORDER:
            job_id, job_config = deploy_one_job(yaml_path, job_key, is_new_cli, job_ids_by_key)
            job_ids_by_key[job_key] = job_id
            enviado_por_id[job_id] = job_config
        return True, is_new_cli, enviado_por_id
    except subprocess.CalledProcessError as e:
        log(f"Erro no deploy: {e}", "ERROR")
        log(f"stdout: {e.stdout}", "DEBUG")
        log(f"stderr: {e.stderr}", "ERROR")
        return False, is_new_cli, enviado_por_id
    except Exception as e:
        log(f"Erro inesperado: {e}", "ERROR")
        return False, is_new_cli, enviado_por_id


def verify_deployment(enviado_por_id, is_new_cli):
    """Rele cada job pela API e confere os campos_criticos contra o que foi enviado.

    Verifica so a configuracao do job; nao roda nada (quem sobe cluster e o smoke.py).
    """
    if not enviado_por_id:
        log("Nenhum job foi deployado", "ERROR")
        return False

    log("Relendo os jobs deployados...")
    all_ok = True
    for job_id, enviado in enviado_por_id.items():
        lido = fetch_job(job_id, is_new_cli)
        if lido is None:
            log(f"{enviado['name']} (ID {job_id}) não pôde ser lido de volta", "ERROR")
            all_ok = False
            continue

        divergencias = diferencas(campos_criticos(lido), campos_criticos(enviado))
        if divergencias:
            all_ok = False
            for d in divergencias:
                log(f"{enviado['name']} não bateu · {d}", "ERROR")
        else:
            log(f"{enviado['name']} (ID {job_id}) confere")
    return all_ok


def cleanup():
    """Limpa arquivos temporários"""
    try:
        if os.path.exists(JSON_TMP):
            os.remove(JSON_TMP)
            log("Arquivo temporário removido")
    except Exception as e:
        log(f"Erro na limpeza: {e}", "WARN")


if __name__ == "__main__":
    log("Iniciando deploy do pipeline...")
    if not TARGET["alert_email"]:
        log("MTG_ALERT_EMAIL vazio: os jobs sobem SEM alerta de falha", "WARN")
    log("=" * 60)

    try:
        deploy_success, is_new_cli, enviado_por_id = deploy_all()

        if deploy_success:
            log("Deploy executado com sucesso!")
            verify_success = verify_deployment(enviado_por_id, is_new_cli)

            if verify_success:
                log("Deploy e verificação concluídos com sucesso!")
                log("=" * 60)
                sys.exit(0)
            else:
                log("Deploy executado mas verificação falhou", "WARN")
                log("=" * 60)
                sys.exit(1)
        else:
            log("Falha no deploy!", "ERROR")
            log("=" * 60)
            sys.exit(1)

    except KeyboardInterrupt:
        log("Deploy interrompido pelo usuário", "WARN")
        sys.exit(1)
    except Exception as e:
        log(f"Erro crítico: {e}", "ERROR")
        sys.exit(1)
    finally:
        cleanup()
