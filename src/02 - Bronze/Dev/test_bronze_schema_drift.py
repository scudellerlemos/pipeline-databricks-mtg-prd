# Testa a lógica de bronze_utils.py (idempotência e listagem da Stage) em Python
# puro: bronze_utils depende de pyspark/dbutils/%run e não importa fora do
# Databricks. Limitação: as funções abaixo são cópias - mudou lá, atualizar aqui.


def normalizar_caminho(caminho):
    """Espelho de bronze_utils.normalizar_caminho."""
    caminho = caminho.split("://", 1)[-1]
    if ".parquet/" in caminho:
        caminho = caminho.split(".parquet/", 1)[0] + ".parquet"
    return caminho


def filtrar_arquivos_stage(nomes, conteudos=None):
    """Espelho do filtro de listar_arquivos_stage. conteudos mapeia o nome de
    um diretório para o que há dentro dele."""
    conteudos = conteudos or {}
    saida = []
    for n in nomes:
        if not n.rstrip("/").endswith(".parquet"):
            continue
        internos = conteudos.get(n)
        if internos is not None and not any(i.endswith(".parquet") for i in internos):
            continue
        saida.append(n)
    return saida


def encontrar_arquivos_novos(todos_arquivos_stage, arquivos_ja_carregados):
    """Espelho do filtro de idempotência de executar_ingestao_bronze."""
    ja_carregados = set(arquivos_ja_carregados)
    return [f for f in todos_arquivos_stage if normalizar_caminho(f) not in ja_carregados]


def test_sem_arquivos_novos_quando_tudo_ja_carregado():
    todos_arquivos = ["s3://b/stage/2026_09_14_cards.parquet"]
    ja_carregados = {"b/stage/2026_09_14_cards.parquet"}
    assert encontrar_arquivos_novos(todos_arquivos, ja_carregados) == []


def test_so_arquivos_nao_vistos_sao_novos():
    todos_arquivos = [
        "s3://b/stage/2026_09_13_cards.parquet",
        "s3://b/stage/2026_09_14_cards.parquet",
    ]
    ja_carregados = {"b/stage/2026_09_13_cards.parquet"}
    assert encontrar_arquivos_novos(todos_arquivos, ja_carregados) == ["s3://b/stage/2026_09_14_cards.parquet"]


def test_reexecucao_no_mesmo_dia_nao_faz_nada():
    # O nome do arquivo da Stage inclui o dia, então a 2a run do dia não gera arquivo novo.
    todos_arquivos = ["s3://b/stage/2026_09_14_cards.parquet"]
    ja_carregados = {"b/stage/2026_09_14_cards.parquet"}
    assert encontrar_arquivos_novos(todos_arquivos, ja_carregados) == []


def test_esquema_de_uri_diferente_nao_reprocessa():
    # dbutils.fs.ls devolve s3:// e _metadata.file_path pode vir s3a:// pro mesmo arquivo.
    todos_arquivos = ["s3://b/stage/2026_09_14_cards.parquet"]
    ja_carregados = {normalizar_caminho("s3a://b/stage/2026_09_14_cards.parquet")}
    assert encontrar_arquivos_novos(todos_arquivos, ja_carregados) == []


def caminho_tabela_stage(caminho_s3_stage, nome_tabela_stage):
    """Espelho do caminho_tabela de listar_arquivos_stage: uma subpasta por tabela da Stage."""
    return f"{caminho_s3_stage}/{nome_tabela_stage}"


def test_caminho_tabela_stage_e_subpasta_por_tabela():
    assert caminho_tabela_stage("s3://b/stage", "cards") == "s3://b/stage/cards"
    assert caminho_tabela_stage("s3://b/stage", "card_prices") == "s3://b/stage/card_prices"


def test_filtro_arquivos_stage_aceita_diretorios():
    # dbutils.fs.ls devolve diretório com "/" no final.
    nomes = ["2026_09_15_cards.parquet/", "_SUCCESS", "2026_09_15_cards.parquet.crc"]
    assert filtrar_arquivos_stage(nomes) == ["2026_09_15_cards.parquet/"]


def test_diretorio_de_escrita_nao_commitada_e_ignorado():
    # Diretório só com o marcador _started_* é escrita não commitada; lê-lo
    # derruba a run com UNABLE_TO_INFER_SCHEMA.
    nomes = ["ok.parquet/", "quebrado.parquet/"]
    conteudos = {
        "ok.parquet/": ["part-00000-x.snappy.parquet", "_SUCCESS"],
        "quebrado.parquet/": ["_started_5021188249195991183"],
    }
    assert filtrar_arquivos_stage(nomes, conteudos) == ["ok.parquet/"]


def test_normalizar_caminho_corta_arquivo_parte_no_diretorio_parquet():
    arquivo_parte = "s3://b/stage/cards/2026_09_15_cards.parquet/part-00000-x.snappy.parquet"
    diretorio = "s3://b/stage/cards/2026_09_15_cards.parquet"
    assert normalizar_caminho(arquivo_parte) == normalizar_caminho(diretorio)


def test_arquivo_parte_ja_carregado_marca_diretorio_como_nao_novo():
    # Bronze guarda o part-file (s3a://), a Stage lista o diretório (s3://).
    ja_carregados = {normalizar_caminho(
        "s3a://b/stage/cards/2026_09_15_cards.parquet/part-00000-x.snappy.parquet"
    )}
    arquivos_stage = ["s3://b/stage/cards/2026_09_15_cards.parquet"]
    assert encontrar_arquivos_novos(arquivos_stage, ja_carregados) == []


if __name__ == "__main__":
    test_sem_arquivos_novos_quando_tudo_ja_carregado()
    test_so_arquivos_nao_vistos_sao_novos()
    test_reexecucao_no_mesmo_dia_nao_faz_nada()
    test_esquema_de_uri_diferente_nao_reprocessa()
    test_caminho_tabela_stage_e_subpasta_por_tabela()
    test_filtro_arquivos_stage_aceita_diretorios()
    test_diretorio_de_escrita_nao_commitada_e_ignorado()
    test_normalizar_caminho_corta_arquivo_parte_no_diretorio_parquet()
    test_arquivo_parte_ja_carregado_marca_diretorio_como_nao_novo()
    print("OK")
