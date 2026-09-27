#!/usr/bin/env python3
"""
Validacao estrutural dos jobs Databricks definidos em .github/DAGs/*.yml.

Cada job vira seu proprio job no Databricks (nao usamos Databricks Asset
Bundles, entao nao ha 1 job por bundle - ha varios arquivos, cada um com um
job debaixo de resources.jobs). Este script generaliza os checks que antes
viviam hardcoded pra um unico arquivo/job (magic.yml / MTG_PIPELINE).
"""

import json
import os
import sys
import yaml

DAG_FILES = [
    (".github/DAGs/stage.yml", "MTG_STAGE"),
    (".github/DAGs/bronze.yml", "MTG_BRONZE"),
    (".github/DAGs/pipeline.yml", "MTG_PIPELINE"),
    (".github/DAGs/silver.yml", "MTG_SILVER"),
    (".github/DAGs/gold.yml", "MTG_GOLD"),
]


def load_job(yaml_path, job_key):
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if "resources" not in config or "jobs" not in config["resources"]:
        raise ValueError(f"{yaml_path}: resources.jobs nao encontrado")
    if job_key not in config["resources"]["jobs"]:
        raise ValueError(f"{yaml_path}: job {job_key} nao encontrado")
    return config["resources"]["jobs"][job_key]


def validate_structure(yaml_path, job_key, job):
    required_fields = ["name", "tasks"]
    for field in required_fields:
        if field not in job:
            raise ValueError(f"{yaml_path} ({job_key}): campo obrigatorio ausente: {field}")

    if not job["tasks"]:
        raise ValueError(f"{yaml_path} ({job_key}): nenhuma task definida")

    task_keys = [task["task_key"] for task in job["tasks"]]
    for task in job["tasks"]:
        for dep in task.get("depends_on", []):
            if dep["task_key"] not in task_keys:
                raise ValueError(
                    f"{yaml_path} ({job_key}): task {task['task_key']} depende de "
                    f"task inexistente: {dep['task_key']}"
                )


def has_notebook_tasks(job):
    return any("notebook_task" in task for task in job["tasks"])


def resolve_notebook_file(notebook_path):
    if notebook_path.endswith(".ipynb"):
        notebook_path = notebook_path[:-6]
    if os.path.exists(f"{notebook_path}.py"):
        return f"{notebook_path}.py"
    return f"{notebook_path}.ipynb"


def validate_notebook_paths(yaml_path, job_key, job):
    missing = []
    for task in job["tasks"]:
        if "notebook_task" not in task:
            continue
        notebook_path = task["notebook_task"]["notebook_path"]
        if notebook_path.endswith(".ipynb"):
            notebook_path = notebook_path[:-6]
        if not (os.path.exists(f"{notebook_path}.ipynb") or os.path.exists(f"{notebook_path}.py")):
            missing.append(f"{notebook_path}.ipynb")
    if missing:
        raise ValueError(f"{yaml_path} ({job_key}): notebooks ausentes: {missing}")


def validate_notebook_syntax(yaml_path, job_key, job):
    invalid = []
    for task in job["tasks"]:
        if "notebook_task" not in task:
            continue
        notebook_path = task["notebook_task"]["notebook_path"]
        notebook_file = resolve_notebook_file(notebook_path)
        if notebook_file.endswith(".py"):
            try:
                with open(notebook_file, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                if first_line != "# Databricks notebook source":
                    invalid.append(f"{notebook_file} - sem header 'Databricks notebook source'")
            except Exception as e:
                invalid.append(f"{notebook_file} - {e}")
            continue
        try:
            with open(notebook_file, "r", encoding="utf-8") as f:
                notebook_data = json.load(f)
            if "cells" not in notebook_data:
                invalid.append(f"{notebook_file} - sem cells")
                continue
            code_cells = [c for c in notebook_data["cells"] if c.get("cell_type") == "code"]
            if not code_cells:
                invalid.append(f"{notebook_file} - sem code cells")
        except json.JSONDecodeError:
            invalid.append(f"{notebook_file} - JSON invalido")
        except Exception as e:
            invalid.append(f"{notebook_file} - {e}")
    if invalid:
        raise ValueError(f"{yaml_path} ({job_key}): notebooks invalidos: {invalid}")


def validate_cluster_config(yaml_path, job_key, job):
    if not has_notebook_tasks(job):
        return  # job so-orquestrador (run_job_task) nao roda notebook, nao precisa de cluster
    if "job_clusters" not in job:
        raise ValueError(f"{yaml_path} ({job_key}): job_clusters ausente")
    for cluster in job["job_clusters"]:
        if "new_cluster" not in cluster:
            raise ValueError(f"{yaml_path} ({job_key}): cluster sem new_cluster")
        cluster_config = cluster["new_cluster"]
        if "spark_version" not in cluster_config:
            raise ValueError(f"{yaml_path} ({job_key}): campo de cluster ausente: spark_version")
        # node de compute vem de node_type_id direto OU de um instance pool
        if "node_type_id" not in cluster_config and "instance_pool_id" not in cluster_config:
            raise ValueError(
                f"{yaml_path} ({job_key}): cluster sem node_type_id nem instance_pool_id"
            )


def validate_git_config(yaml_path, job_key, job):
    if not has_notebook_tasks(job):
        return  # job so-orquestrador nao le notebook via git_source
    if "git_source" not in job:
        raise ValueError(f"{yaml_path} ({job_key}): git_source ausente")
    for field in ["git_url", "git_provider", "git_branch"]:
        if field not in job["git_source"]:
            raise ValueError(f"{yaml_path} ({job_key}): campo git ausente: {field}")


CHECKS = [
    validate_structure,
    validate_notebook_paths,
    validate_notebook_syntax,
    validate_cluster_config,
    validate_git_config,
]


def main():
    errors = []
    report = []

    for yaml_path, job_key in DAG_FILES:
        try:
            job = load_job(yaml_path, job_key)
        except ValueError as e:
            errors.append(str(e))
            continue

        for check in CHECKS:
            try:
                check(yaml_path, job_key, job)
            except ValueError as e:
                errors.append(str(e))

        deps = [
            f"{task['task_key']} -> {[d['task_key'] for d in task['depends_on']]}"
            for task in job["tasks"]
            if "depends_on" in task
        ]
        report.append(f"{job_key} ({yaml_path}): {len(job['tasks'])} tasks; " + "; ".join(deps))

    if errors:
        print("Falhas de validacao:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    print("Todos os jobs em .github/DAGs/*.yml sao validos.")
    for line in report:
        print(f"  - {line}")


if __name__ == "__main__":
    main()
