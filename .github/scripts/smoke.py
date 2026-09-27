#!/usr/bin/env python3
"""Roda o smoke_test no ambiente recem-deployado e espera o resultado.

Fecha o unico buraco que nem o CI nem o verify_deployment alcancam: os dois
sao estaticos. O CI le YAML e roda pytest de funcao pura; o verify_deployment
rele as settings do job pela API. Nenhum dos dois sobe um cluster, entao nada
prova que o ambiente funciona ate a run mensal - que em producao nunca
aconteceu.

Nao cria job: usa `jobs submit`, que e um run avulso e some sozinho. O cluster
e o git_source saem do gold.yml ja passado pelo apply_target, entao o smoke
roda com exatamente a mesma config (tag, spark_env_vars, pool) do deploy que
acabou de acontecer - nao com uma copia que pode divergir.
"""

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import deploy  # noqa: E402  (precisa do sys.path acima)

NOTEBOOK = "src/00 - Common/Dev/smoke_deploy"
JSON_TMP = "smoke_submit.json"
TIMEOUT_S = 1800
INTERVALO_S = 20


def montar_payload():
    """Cluster e git do MTG_GOLD, task trocada pelo smoke."""
    molde = deploy.load_job_config(".github/DAGs/gold.yml", "MTG_GOLD")
    cluster_key = molde["job_clusters"][0]["job_cluster_key"]
    return {
        "run_name": "MTG_SMOKE" + deploy.TARGET["suffix"],
        "git_source": molde["git_source"],
        "job_clusters": molde["job_clusters"],
        "timeout_seconds": TIMEOUT_S,
        "tasks": [
            {
                "task_key": "smoke",
                "job_cluster_key": cluster_key,
                "notebook_task": {"notebook_path": NOTEBOOK, "source": "GIT"},
            }
        ],
    }


def cli(*args):
    r = subprocess.run(["databricks", *args], capture_output=True, text=True, check=True)
    return json.loads(r.stdout) if r.stdout.strip() else {}


def esperar(run_id):
    """Poll ate terminar. Devolve (ok, estado_legivel)."""
    limite = time.time() + TIMEOUT_S + 300
    while time.time() < limite:
        estado = cli("jobs", "get-run", str(run_id), "-o", "json").get("state", {})
        ciclo = estado.get("life_cycle_state")
        if ciclo in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            resultado = estado.get("result_state")
            detalhe = estado.get("state_message") or ""
            return resultado == "SUCCESS", f"{ciclo}/{resultado} {detalhe}".strip()
        deploy.log(f"⏳ smoke {ciclo}...")
        time.sleep(INTERVALO_S)
    return False, "o poll estourou o tempo"


def main():
    ok, is_new_cli = deploy.validate_databricks_connection()
    if not ok:
        return 1
    if not is_new_cli:
        deploy.log("❌ smoke.py precisa da CLI nova (databricks/setup-cli)", "ERROR")
        return 1

    payload = montar_payload()
    with open(JSON_TMP, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    try:
        deploy.log(f"💨 Submetendo {payload['run_name']} ({payload['git_source'].get('git_tag') or payload['git_source'].get('git_branch')})...")
        run_id = cli("jobs", "submit", "--json", f"@{JSON_TMP}", "--no-wait", "-o", "json").get("run_id")
        if not run_id:
            deploy.log("❌ jobs submit nao devolveu run_id", "ERROR")
            return 1
        deploy.log(f"🔗 run {run_id}")

        sucesso, estado = esperar(run_id)
        if sucesso:
            deploy.log(f"✅ Smoke passou · run {run_id}")
            return 0
        deploy.log(f"❌ Smoke falhou · run {run_id} · {estado}", "ERROR")
        return 1
    except subprocess.CalledProcessError as e:
        deploy.log(f"❌ Erro na CLI: {e.stderr}", "ERROR")
        return 1
    finally:
        if os.path.exists(JSON_TMP):
            os.remove(JSON_TMP)


if __name__ == "__main__":
    sys.exit(main())
