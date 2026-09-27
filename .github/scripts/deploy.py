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

# (arquivo, chave_job). O orquestrador vai por ultimo: precisa dos job_ids dos outros.
ORDEM_DEPLOY = [
    (".github/DAGs/stage.yml", "MTG_STAGE"),
    (".github/DAGs/bronze.yml", "MTG_BRONZE"),
    (".github/DAGs/silver.yml", "MTG_SILVER"),
    (".github/DAGs/gold.yml", "MTG_GOLD"),
    (".github/DAGs/pipeline.yml", "MTG_PIPELINE"),
]

JSON_TEMPORARIO = "job_deploy.json"

# ---------------------------------------------------------------------------
# ALVO DE DEPLOY
# ---------------------------------------------------------------------------
# dev e prd usam o mesmo workspace e o mesmo codigo (o repo de prd recebe uma
# copia via promote.yml). O que diferencia os dois vem de env var do workflow
# e e aplicado no job antes do POST, nunca no YAML.
ALVO = {
    # sufixo no nome do job: "" em dev, "_PRD" em producao
    "sufixo": os.environ.get("MTG_JOB_SUFFIX", ""),
    "url_git": os.environ.get("MTG_GIT_URL", ""),
    # Tag imutavel: rollback = redeployar a tag anterior. Vazio = usa o git_branch do YAML.
    "tag_git": os.environ.get("MTG_GIT_TAG", ""),
    "ambiente": os.environ.get("MTG_ENVIRONMENT", ""),
    # jobs reset sobrescreve as settings inteiras; pausa feita pela UI nao sobrevive ao deploy.
    "status_pausa": os.environ.get("MTG_PAUSE_STATUS", ""),
    # Email de alerta de falha. Vazio = job sem email_notifications (o deploy avisa).
    "email_alerta": os.environ.get("MTG_ALERT_EMAIL", ""),
}


# Env vars MTG_* que so configuram o job; nao vao pro cluster.
PARAMETROS_DO_DEPLOY = {
    "MTG_JOB_SUFFIX",
    "MTG_GIT_URL",
    "MTG_GIT_TAG",
    "MTG_PAUSE_STATUS",
    "MTG_ALERT_EMAIL",
}


def nome_job(chave_job):
    return chave_job + ALVO["sufixo"]


# Env vars MTG_* repassadas ao cluster (spark_env_vars).
# obter_segredo() resolve env var > secret > default. dev e prd dividem o mesmo
# secret scope (config de dev); o que prd sobrescreve vem daqui, versionado no workflow.
# MTG_ENVIRONMENT tambem vai: e o que ativa, no obter_segredo, a trava que impede
# producao de usar o catalogo mtg_dev.
CONFIG_DO_AMBIENTE = {
    k: v
    for k, v in os.environ.items()
    if k.startswith("MTG_") and k not in PARAMETROS_DO_DEPLOY and v
}


def aplicar_alvo(config_job):
    """Aplica o alvo (nome, config, git ref, tag, schedule) no job do YAML."""
    config_job["name"] = nome_job(config_job["name"])

    if CONFIG_DO_AMBIENTE:
        for cluster in config_job.get("job_clusters", []):
            variaveis_ambiente = cluster.setdefault("new_cluster", {}).setdefault("spark_env_vars", {})
            variaveis_ambiente.update(CONFIG_DO_AMBIENTE)

    origem_git = config_job.get("git_source")
    if origem_git:
        if ALVO["url_git"]:
            origem_git["git_url"] = ALVO["url_git"]
        if ALVO["tag_git"]:
            # git_source aceita branch OU tag, nunca os dois
            origem_git.pop("git_branch", None)
            origem_git["git_tag"] = ALVO["tag_git"]

    if ALVO["ambiente"]:
        config_job.setdefault("tags", {})["environment"] = ALVO["ambiente"]

    if ALVO["status_pausa"] and "schedule" in config_job:
        config_job["schedule"]["pause_status"] = ALVO["status_pausa"]

    # Vai em todos os jobs, inclusive o orquestrador: uma falha de camada gera
    # 2 emails (camada + orquestrador). Se incomodar, filtrar por "schedule" in config_job.
    if ALVO["email_alerta"]:
        config_job["email_notifications"] = {
            "on_failure": [ALVO["email_alerta"]],
            "no_alert_for_skipped_runs": True,
        }

    return config_job


def log(mensagem, nivel="INFO"):
    """Função para logging padronizado"""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{agora}] [{nivel}] {mensagem}")


def carregar_config_job(caminho_yaml, chave_job, ids_job_por_chave=None):
    """Le o YAML, extrai o job e (se for o orquestrador) substitui os
    placeholders de job_id pelos IDs reais ja deployados."""
    with open(caminho_yaml, "r", encoding="utf-8") as f:
        dados_yaml = yaml.safe_load(f)

    if "resources" not in dados_yaml or "jobs" not in dados_yaml["resources"]:
        raise ValueError(f"{caminho_yaml}: resources.jobs não encontrado")
    if chave_job not in dados_yaml["resources"]["jobs"]:
        raise ValueError(f"{caminho_yaml}: job {chave_job} não encontrado")

    config_job = dados_yaml["resources"]["jobs"][chave_job]

    if ids_job_por_chave:
        for tarefa in config_job.get("tasks", []):
            if "run_job_task" not in tarefa:
                continue
            marcador = tarefa["run_job_task"].get("job_id", "")
            for chave_referenciada, id_referenciado in ids_job_por_chave.items():
                marcador_esperado = "{{" + f"{chave_referenciada}_JOB_ID" + "}}"
                if marcador == marcador_esperado:
                    tarefa["run_job_task"]["job_id"] = id_referenciado

    return aplicar_alvo(config_job)


def gravar_json(config_job):
    conteudo_json = json.dumps(config_job, indent=2, ensure_ascii=False)
    with open(JSON_TEMPORARIO, "w", encoding="utf-8") as f:
        f.write(conteudo_json)
    return conteudo_json


def obter_id_job_existente(nome_completo_job, cli_nova=False):
    """Obtém o ID do job existente pelo nome, se houver"""
    try:
        formato_saida = "json" if cli_nova else "JSON"
        resultado = subprocess.run(
            ["databricks", "jobs", "list", "--output", formato_saida],
            capture_output=True,
            text=True,
            check=True,
        )
        dados_jobs = json.loads(resultado.stdout)
        lista_jobs = dados_jobs if isinstance(dados_jobs, list) else dados_jobs.get("jobs", [])
        for job in lista_jobs:
            if job.get("settings", {}).get("name") == nome_completo_job:
                id_job = job.get("job_id")
                log(f"Job existente encontrado: {nome_completo_job} (ID {id_job})")
                return id_job
        log(f"Job {nome_completo_job} não existe ainda, será criado")
        return None
    except subprocess.CalledProcessError as e:
        log(f"Erro ao listar jobs: {e.stderr}", "WARN")
        return None
    except Exception as e:
        log(f"Erro inesperado ao listar jobs: {e}", "WARN")
        return None


def campos_criticos(configuracoes):
    """Campos das settings que o deploy decide e que precisam bater apos o deploy.

    Nao compara as settings inteiras porque a API preenche defaults
    (format, timeout_seconds, ...) e o diff vira ruido.
    """
    git = configuracoes.get("git_source") or {}
    variaveis_ambiente = {}
    for cluster in configuracoes.get("job_clusters", []):
        variaveis_ambiente.update((cluster.get("new_cluster") or {}).get("spark_env_vars", {}))
    return {
        "name": configuracoes.get("name"),
        "git_url": git.get("git_url"),
        "git_ref": git.get("git_tag") or git.get("git_branch"),
        "pause_status": (configuracoes.get("schedule") or {}).get("pause_status"),
        "config_do_ambiente": {k: v for k, v in variaveis_ambiente.items() if k.startswith("MTG_")},
        # A API devolve email_notifications: {} quando nao mandamos nada; normaliza pra [].
        "alerta": (configuracoes.get("email_notifications") or {}).get("on_failure") or [],
        "run_job_task_ids": sorted(
            str(t["run_job_task"].get("job_id"))
            for t in configuracoes.get("tasks", [])
            if "run_job_task" in t
        ),
    }


def diferencas(de, para):
    """['campo: valor_antigo -> valor_novo'] entre dois campos_criticos()."""
    return [f"{k}: {de[k]!r} -> {para[k]!r}" for k in para if de.get(k) != para[k]]


def buscar_job(id_job, cli_nova):
    """Settings atuais do job, ou None se nao der pra ler."""
    try:
        comando = ["databricks", "jobs", "get"]
        comando += [str(id_job)] if cli_nova else ["--job-id", str(id_job)]
        comando += ["--output", "json" if cli_nova else "JSON"]
        resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
        return json.loads(resultado.stdout).get("settings")
    except Exception as e:
        log(f"Não consegui ler o job {id_job}: {e}", "WARN")
        return None


def validar_conexao_databricks():
    """Valida a conexão com o Databricks"""
    try:
        log("Testando conexão com Databricks...")
        resultado = subprocess.run(["databricks", "--version"], capture_output=True, text=True, check=True)
        saida_versao = resultado.stdout.strip()
        log(f"Databricks CLI version: {saida_versao}")

        cli_nova = saida_versao.startswith("Databricks CLI v")
        cli_antiga = saida_versao.startswith("Version ")

        if cli_antiga:
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

        if cli_nova:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        else:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        log("Conexão com workspace estabelecida")

        return True, cli_nova
    except subprocess.CalledProcessError as e:
        log(f"Erro na conexão com Databricks (exit {e.returncode}): stdout={e.stdout!r} stderr={e.stderr!r}", "ERROR")
        return False, False
    except Exception as e:
        log(f"Erro inesperado na validação: {e}", "ERROR")
        return False, False


def deployar_job(caminho_yaml, chave_job, cli_nova, ids_job_por_chave):
    """Deploya (cria ou atualiza) um job e retorna seu job_id."""
    log(f"Lendo {caminho_yaml} ({chave_job} -> {nome_job(chave_job)})...")
    config_job = carregar_config_job(caminho_yaml, chave_job, ids_job_por_chave)
    gravar_json(config_job)

    id_existente = obter_id_job_existente(config_job["name"], cli_nova)

    ambiente_cli = os.environ.copy()
    if not cli_nova:
        ambiente_cli["DATABRICKS_JOBS_API_VERSION"] = "2.1"

    with open(JSON_TEMPORARIO, "r", encoding="utf-8") as f:
        conteudo_json = f.read()

    if id_existente:
        log(f"Atualizando job existente: {config_job['name']} (ID {id_existente})")
        # jobs reset sobrescreve as settings inteiras (o YAML e a fonte da verdade);
        # loga o que foi alterado pela UI e vai ser desfeito.
        atual = buscar_job(id_existente, cli_nova)
        if atual:
            for divergencia in diferencas(campos_criticos(atual), campos_criticos(config_job)):
                log(f"Sobrescrevendo {config_job['name']} · {divergencia}", "WARN")
        if cli_nova:
            gravar_json({"job_id": id_existente, "new_settings": config_job})
            resultado = subprocess.run(
                ["databricks", "jobs", "reset", "--json", f"@{JSON_TEMPORARIO}"],
                capture_output=True, text=True, check=True, env=ambiente_cli,
            )
        else:
            resultado = subprocess.run(
                ["databricks", "jobs", "reset", "--job-id", str(id_existente), "--json-file", JSON_TEMPORARIO],
                capture_output=True, text=True, check=True, env=ambiente_cli,
            )
        id_job = id_existente
    else:
        log(f"Criando novo job: {config_job['name']}")
        if cli_nova:
            resultado = subprocess.run(
                ["databricks", "jobs", "create", "--json", f"@{JSON_TEMPORARIO}"],
                capture_output=True, text=True, check=True, env=ambiente_cli,
            )
        else:
            resultado = subprocess.run(
                ["databricks", "jobs", "create", "--json-file", JSON_TEMPORARIO],
                capture_output=True, text=True, check=True, env=ambiente_cli,
            )
        try:
            id_job = json.loads(resultado.stdout).get("job_id")
        except Exception:
            id_job = None

    log(f"Resposta do Databricks para {chave_job}: {resultado.stdout.strip()[:300]}")
    if id_job is None:
        raise RuntimeError(f"Não foi possível determinar o job_id de {chave_job} após o deploy")

    log(f"{chave_job} -> job_id {id_job}")
    return id_job, config_job


def deployar_todos():
    conexao_ok, cli_nova = validar_conexao_databricks()
    if not conexao_ok:
        return False, cli_nova, {}

    ids_job_por_chave = {}
    enviado_por_id = {}
    try:
        for caminho_yaml, chave_job in ORDEM_DEPLOY:
            id_job, config_job = deployar_job(caminho_yaml, chave_job, cli_nova, ids_job_por_chave)
            ids_job_por_chave[chave_job] = id_job
            enviado_por_id[id_job] = config_job
        return True, cli_nova, enviado_por_id
    except subprocess.CalledProcessError as e:
        log(f"Erro no deploy: {e}", "ERROR")
        log(f"stdout: {e.stdout}", "DEBUG")
        log(f"stderr: {e.stderr}", "ERROR")
        return False, cli_nova, enviado_por_id
    except Exception as e:
        log(f"Erro inesperado: {e}", "ERROR")
        return False, cli_nova, enviado_por_id


def verificar_deploy(enviado_por_id, cli_nova):
    """Rele cada job pela API e confere os campos_criticos contra o que foi enviado.

    Verifica so a configuracao do job; nao roda nada (quem sobe cluster e o smoke.py).
    """
    if not enviado_por_id:
        log("Nenhum job foi deployado", "ERROR")
        return False

    log("Relendo os jobs deployados...")
    tudo_ok = True
    for id_job, enviado in enviado_por_id.items():
        lido = buscar_job(id_job, cli_nova)
        if lido is None:
            log(f"{enviado['name']} (ID {id_job}) não pôde ser lido de volta", "ERROR")
            tudo_ok = False
            continue

        divergencias = diferencas(campos_criticos(lido), campos_criticos(enviado))
        if divergencias:
            tudo_ok = False
            for d in divergencias:
                log(f"{enviado['name']} não bateu · {d}", "ERROR")
        else:
            log(f"{enviado['name']} (ID {id_job}) confere")
    return tudo_ok


def limpar():
    """Limpa arquivos temporários"""
    try:
        if os.path.exists(JSON_TEMPORARIO):
            os.remove(JSON_TEMPORARIO)
            log("Arquivo temporário removido")
    except Exception as e:
        log(f"Erro na limpeza: {e}", "WARN")


if __name__ == "__main__":
    log("Iniciando deploy do pipeline...")
    if not ALVO["email_alerta"]:
        log("MTG_ALERT_EMAIL vazio: os jobs sobem SEM alerta de falha", "WARN")
    log("=" * 60)

    try:
        deploy_ok, cli_nova, enviado_por_id = deployar_todos()

        if deploy_ok:
            log("Deploy executado com sucesso!")
            verificacao_ok = verificar_deploy(enviado_por_id, cli_nova)

            if verificacao_ok:
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
        limpar()
