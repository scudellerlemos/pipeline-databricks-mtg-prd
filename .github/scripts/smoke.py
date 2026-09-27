#!/usr/bin/env python3
"""Roda o notebook smoke_deploy no ambiente recem-deployado e espera o resultado.

Unica verificacao que sobe cluster: CI e verificar_deploy sao estaticos.
Usa `jobs submit` (run avulso, nao cria job). Cluster e git_source vem do
gold.yml ja passado pelo aplicar_alvo, com a mesma config do deploy.
"""

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import deploy  # noqa: E402  (precisa do sys.path acima)

NOTEBOOK = "src/00 - Common/Dev/smoke_deploy"
JSON_TEMPORARIO = "smoke_submit.json"
TEMPO_LIMITE_S = 1800
INTERVALO_S = 20


def montar_payload():
    """Cluster e git do MTG_GOLD, task trocada pelo smoke."""
    molde = deploy.carregar_config_job(".github/DAGs/gold.yml", "MTG_GOLD")
    chave_cluster = molde["job_clusters"][0]["job_cluster_key"]
    return {
        "run_name": "MTG_SMOKE" + deploy.ALVO["sufixo"],
        "git_source": molde["git_source"],
        "job_clusters": molde["job_clusters"],
        "timeout_seconds": TEMPO_LIMITE_S,
        "tasks": [
            {
                "task_key": "smoke",
                "job_cluster_key": chave_cluster,
                "notebook_task": {"notebook_path": NOTEBOOK, "source": "GIT"},
            }
        ],
    }


def cli(*args):
    resultado = subprocess.run(["databricks", *args], capture_output=True, text=True, check=True)
    return json.loads(resultado.stdout) if resultado.stdout.strip() else {}


def esperar(id_execucao):
    """Poll ate terminar. Devolve (ok, estado_legivel)."""
    limite = time.time() + TEMPO_LIMITE_S + 300
    while time.time() < limite:
        estado = cli("jobs", "get-run", str(id_execucao), "-o", "json").get("state", {})
        ciclo = estado.get("life_cycle_state")
        if ciclo in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            resultado = estado.get("result_state")
            detalhe = estado.get("state_message") or ""
            return resultado == "SUCCESS", f"{ciclo}/{resultado} {detalhe}".strip()
        deploy.log(f"smoke {ciclo}...")
        time.sleep(INTERVALO_S)
    return False, "o poll estourou o tempo"


def main():
    ok, cli_nova = deploy.validar_conexao_databricks()
    if not ok:
        return 1
    if not cli_nova:
        deploy.log("smoke.py precisa da CLI nova (databricks/setup-cli)", "ERROR")
        return 1

    payload = montar_payload()
    with open(JSON_TEMPORARIO, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    try:
        deploy.log(f"Submetendo {payload['run_name']} ({payload['git_source'].get('git_tag') or payload['git_source'].get('git_branch')})...")
        id_execucao = cli("jobs", "submit", "--json", f"@{JSON_TEMPORARIO}", "--no-wait", "-o", "json").get("run_id")
        if not id_execucao:
            deploy.log("jobs submit nao devolveu run_id", "ERROR")
            return 1
        deploy.log(f"run {id_execucao}")

        sucesso, estado = esperar(id_execucao)
        if sucesso:
            deploy.log(f"Smoke passou · run {id_execucao}")
            return 0
        deploy.log(f"Smoke falhou · run {id_execucao} · {estado}", "ERROR")
        return 1
    except subprocess.CalledProcessError as e:
        deploy.log(f"Erro na CLI: {e.stderr}", "ERROR")
        return 1
    finally:
        if os.path.exists(JSON_TEMPORARIO):
            os.remove(JSON_TEMPORARIO)


if __name__ == "__main__":
    sys.exit(main())
