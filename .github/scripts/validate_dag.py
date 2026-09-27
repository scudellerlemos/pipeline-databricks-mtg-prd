#!/usr/bin/env python3
"""
Validacao estrutural dos jobs Databricks definidos em .github/DAGs/*.yml.

Cada arquivo tem um job em resources.jobs, deployado como job separado.
"""

import json
import os
import sys
import yaml

ARQUIVOS_DAG = [
    (".github/DAGs/stage.yml", "MTG_STAGE"),
    (".github/DAGs/bronze.yml", "MTG_BRONZE"),
    (".github/DAGs/pipeline.yml", "MTG_PIPELINE"),
    (".github/DAGs/silver.yml", "MTG_SILVER"),
    (".github/DAGs/gold.yml", "MTG_GOLD"),
]


def carregar_job(caminho_yaml, chave_job):
    with open(caminho_yaml, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if "resources" not in config or "jobs" not in config["resources"]:
        raise ValueError(f"{caminho_yaml}: resources.jobs nao encontrado")
    if chave_job not in config["resources"]["jobs"]:
        raise ValueError(f"{caminho_yaml}: job {chave_job} nao encontrado")
    return config["resources"]["jobs"][chave_job]


def validar_estrutura(caminho_yaml, chave_job, job):
    campos_obrigatorios = ["name", "tasks"]
    for campo in campos_obrigatorios:
        if campo not in job:
            raise ValueError(f"{caminho_yaml} ({chave_job}): campo obrigatorio ausente: {campo}")

    if not job["tasks"]:
        raise ValueError(f"{caminho_yaml} ({chave_job}): nenhuma task definida")

    chaves_tarefas = [tarefa["task_key"] for tarefa in job["tasks"]]
    for tarefa in job["tasks"]:
        for dependencia in tarefa.get("depends_on", []):
            if dependencia["task_key"] not in chaves_tarefas:
                raise ValueError(
                    f"{caminho_yaml} ({chave_job}): task {tarefa['task_key']} depende de "
                    f"task inexistente: {dependencia['task_key']}"
                )


def tem_tarefas_notebook(job):
    return any("notebook_task" in tarefa for tarefa in job["tasks"])


def resolver_arquivo_notebook(caminho_notebook):
    if caminho_notebook.endswith(".ipynb"):
        caminho_notebook = caminho_notebook[:-6]
    if os.path.exists(f"{caminho_notebook}.py"):
        return f"{caminho_notebook}.py"
    return f"{caminho_notebook}.ipynb"


def validar_caminhos_notebooks(caminho_yaml, chave_job, job):
    ausentes = []
    for tarefa in job["tasks"]:
        if "notebook_task" not in tarefa:
            continue
        caminho_notebook = tarefa["notebook_task"]["notebook_path"]
        if caminho_notebook.endswith(".ipynb"):
            caminho_notebook = caminho_notebook[:-6]
        if not (os.path.exists(f"{caminho_notebook}.ipynb") or os.path.exists(f"{caminho_notebook}.py")):
            ausentes.append(f"{caminho_notebook}.ipynb")
    if ausentes:
        raise ValueError(f"{caminho_yaml} ({chave_job}): notebooks ausentes: {ausentes}")


def validar_sintaxe_notebooks(caminho_yaml, chave_job, job):
    invalidos = []
    for tarefa in job["tasks"]:
        if "notebook_task" not in tarefa:
            continue
        caminho_notebook = tarefa["notebook_task"]["notebook_path"]
        arquivo_notebook = resolver_arquivo_notebook(caminho_notebook)
        if arquivo_notebook.endswith(".py"):
            try:
                with open(arquivo_notebook, "r", encoding="utf-8") as f:
                    primeira_linha = f.readline().strip()
                if primeira_linha != "# Databricks notebook source":
                    invalidos.append(f"{arquivo_notebook} - sem header 'Databricks notebook source'")
            except Exception as e:
                invalidos.append(f"{arquivo_notebook} - {e}")
            continue
        try:
            with open(arquivo_notebook, "r", encoding="utf-8") as f:
                dados_notebook = json.load(f)
            if "cells" not in dados_notebook:
                invalidos.append(f"{arquivo_notebook} - sem cells")
                continue
            celulas_codigo = [c for c in dados_notebook["cells"] if c.get("cell_type") == "code"]
            if not celulas_codigo:
                invalidos.append(f"{arquivo_notebook} - sem code cells")
        except json.JSONDecodeError:
            invalidos.append(f"{arquivo_notebook} - JSON invalido")
        except Exception as e:
            invalidos.append(f"{arquivo_notebook} - {e}")
    if invalidos:
        raise ValueError(f"{caminho_yaml} ({chave_job}): notebooks invalidos: {invalidos}")


def validar_config_cluster(caminho_yaml, chave_job, job):
    if not tem_tarefas_notebook(job):
        return  # job so-orquestrador (run_job_task) nao roda notebook, nao precisa de cluster
    if "job_clusters" not in job:
        raise ValueError(f"{caminho_yaml} ({chave_job}): job_clusters ausente")
    for cluster in job["job_clusters"]:
        if "new_cluster" not in cluster:
            raise ValueError(f"{caminho_yaml} ({chave_job}): cluster sem new_cluster")
        config_cluster = cluster["new_cluster"]
        if "spark_version" not in config_cluster:
            raise ValueError(f"{caminho_yaml} ({chave_job}): campo de cluster ausente: spark_version")
        # node de compute vem de node_type_id direto OU de um instance pool
        if "node_type_id" not in config_cluster and "instance_pool_id" not in config_cluster:
            raise ValueError(
                f"{caminho_yaml} ({chave_job}): cluster sem node_type_id nem instance_pool_id"
            )


def validar_config_git(caminho_yaml, chave_job, job):
    if not tem_tarefas_notebook(job):
        return  # job so-orquestrador nao le notebook via git_source
    if "git_source" not in job:
        raise ValueError(f"{caminho_yaml} ({chave_job}): git_source ausente")
    for campo in ["git_url", "git_provider", "git_branch"]:
        if campo not in job["git_source"]:
            raise ValueError(f"{caminho_yaml} ({chave_job}): campo git ausente: {campo}")


VALIDACOES = [
    validar_estrutura,
    validar_caminhos_notebooks,
    validar_sintaxe_notebooks,
    validar_config_cluster,
    validar_config_git,
]


def main():
    erros = []
    relatorio = []

    for caminho_yaml, chave_job in ARQUIVOS_DAG:
        try:
            job = carregar_job(caminho_yaml, chave_job)
        except ValueError as e:
            erros.append(str(e))
            continue

        for validacao in VALIDACOES:
            try:
                validacao(caminho_yaml, chave_job, job)
            except ValueError as e:
                erros.append(str(e))

        dependencias = [
            f"{tarefa['task_key']} -> {[d['task_key'] for d in tarefa['depends_on']]}"
            for tarefa in job["tasks"]
            if "depends_on" in tarefa
        ]
        relatorio.append(f"{chave_job} ({caminho_yaml}): {len(job['tasks'])} tasks; " + "; ".join(dependencias))

    if erros:
        print("Falhas de validacao:")
        for e in erros:
            print(f"  - {e}")
        sys.exit(1)

    print("Todos os jobs em .github/DAGs/*.yml sao validos.")
    for linha in relatorio:
        print(f"  - {linha}")


if __name__ == "__main__":
    main()
