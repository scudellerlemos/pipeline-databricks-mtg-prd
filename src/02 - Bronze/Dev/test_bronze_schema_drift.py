# Testa a lógica de bronze_utils.py (idempotência e diff de schema) em Python
# puro: bronze_utils depende de pyspark/dbutils/%run e não importa fora do
# Databricks. Limitação: as funções abaixo são cópias - mudou lá, atualizar aqui.


def normalize_path(path):
    """Mirrors bronze_utils.normalize_path."""
    path = path.split("://", 1)[-1]
    if ".parquet/" in path:
        path = path.split(".parquet/", 1)[0] + ".parquet"
    return path


def list_stage_files_filter(names, contents=None):
    """Mirrors list_stage_files' filter. contents maps a directory name to
    what's inside it."""
    contents = contents or {}
    out = []
    for n in names:
        if not n.rstrip("/").endswith(".parquet"):
            continue
        inner = contents.get(n)
        if inner is not None and not any(i.endswith(".parquet") for i in inner):
            continue
        out.append(n)
    return out


def find_new_files(all_stage_files, already_loaded_files):
    """Mirrors run_bronze_ingestion's idempotency filter."""
    already = set(already_loaded_files)
    return [f for f in all_stage_files if normalize_path(f) not in already]


def diff_schema(existing_fields, incoming_fields):
    """Mirrors log_schema_diff's classification: new/missing/type-changed."""
    new_cols = sorted(c for c in incoming_fields if c not in existing_fields)
    missing_cols = sorted(c for c in existing_fields if c not in incoming_fields)
    type_changed = sorted(
        c for c in incoming_fields
        if c in existing_fields and incoming_fields[c] != existing_fields[c]
    )
    return {"new": new_cols, "missing": missing_cols, "type_changed": type_changed}


def test_no_new_files_when_everything_already_loaded():
    all_files = ["s3://b/stage/2026_09_14_cards.parquet"]
    already = {"b/stage/2026_09_14_cards.parquet"}
    assert find_new_files(all_files, already) == []


def test_only_unseen_files_are_new():
    all_files = [
        "s3://b/stage/2026_09_13_cards.parquet",
        "s3://b/stage/2026_09_14_cards.parquet",
    ]
    already = {"b/stage/2026_09_13_cards.parquet"}
    assert find_new_files(all_files, already) == ["s3://b/stage/2026_09_14_cards.parquet"]


def test_rerun_same_day_is_noop():
    # O nome do arquivo da Stage inclui o dia, então a 2a run do dia não gera arquivo novo.
    all_files = ["s3://b/stage/2026_09_14_cards.parquet"]
    already = {"b/stage/2026_09_14_cards.parquet"}
    assert find_new_files(all_files, already) == []


def test_scheme_mismatch_does_not_cause_reprocessing():
    # dbutils.fs.ls devolve s3:// e _metadata.file_path pode vir s3a:// pro mesmo arquivo.
    all_files = ["s3://b/stage/2026_09_14_cards.parquet"]
    already = {normalize_path("s3a://b/stage/2026_09_14_cards.parquet")}
    assert find_new_files(all_files, already) == []


def test_schema_diff_detects_new_and_missing_columns():
    existing = {"id": "StringType()", "name": "StringType()"}
    incoming = {"id": "StringType()", "set": "StringType()"}
    result = diff_schema(existing, incoming)
    assert result["new"] == ["set"]
    assert result["missing"] == ["name"]
    assert result["type_changed"] == []


def test_schema_diff_detects_type_change():
    existing = {"cmc": "DoubleType()"}
    incoming = {"cmc": "StringType()"}
    result = diff_schema(existing, incoming)
    assert result["type_changed"] == ["cmc"]


def test_schema_diff_first_load_is_all_new():
    result = diff_schema({}, {"id": "StringType()", "name": "StringType()"})
    assert result["new"] == ["id", "name"]
    assert result["missing"] == []


def stage_table_path(s3_stage_path, stage_table_name):
    """Mirrors list_stage_files' table_path: one subfolder per Stage table."""
    return f"{s3_stage_path}/{stage_table_name}"


def test_stage_table_path_is_per_table_subfolder():
    assert stage_table_path("s3://b/stage", "cards") == "s3://b/stage/cards"
    assert stage_table_path("s3://b/stage", "card_prices") == "s3://b/stage/card_prices"


def test_list_stage_files_filter_matches_directory_entries():
    # dbutils.fs.ls devolve diretório com "/" no final.
    names = ["2026_09_15_cards.parquet/", "_SUCCESS", "2026_09_15_cards.parquet.crc"]
    assert list_stage_files_filter(names) == ["2026_09_15_cards.parquet/"]


def test_uncommitted_write_directory_is_skipped():
    # Diretório só com o marcador _started_* é escrita não commitada; lê-lo
    # derruba a run com UNABLE_TO_INFER_SCHEMA.
    names = ["ok.parquet/", "quebrado.parquet/"]
    contents = {
        "ok.parquet/": ["part-00000-x.snappy.parquet", "_SUCCESS"],
        "quebrado.parquet/": ["_started_5021188249195991183"],
    }
    assert list_stage_files_filter(names, contents) == ["ok.parquet/"]


def test_normalize_path_truncates_part_file_to_parquet_dir():
    part_file = "s3://b/stage/cards/2026_09_15_cards.parquet/part-00000-x.snappy.parquet"
    directory = "s3://b/stage/cards/2026_09_15_cards.parquet"
    assert normalize_path(part_file) == normalize_path(directory)


def test_already_loaded_part_file_marks_directory_as_not_new():
    # Bronze guarda o part-file (s3a://), a Stage lista o diretório (s3://).
    already_loaded = {normalize_path(
        "s3a://b/stage/cards/2026_09_15_cards.parquet/part-00000-x.snappy.parquet"
    )}
    stage_files = ["s3://b/stage/cards/2026_09_15_cards.parquet"]
    assert find_new_files(stage_files, already_loaded) == []


if __name__ == "__main__":
    test_no_new_files_when_everything_already_loaded()
    test_only_unseen_files_are_new()
    test_rerun_same_day_is_noop()
    test_scheme_mismatch_does_not_cause_reprocessing()
    test_schema_diff_detects_new_and_missing_columns()
    test_schema_diff_detects_type_change()
    test_schema_diff_first_load_is_all_new()
    test_stage_table_path_is_per_table_subfolder()
    test_list_stage_files_filter_matches_directory_entries()
    test_uncommitted_write_directory_is_skipped()
    test_normalize_path_truncates_part_file_to_parquet_dir()
    test_already_loaded_part_file_marks_directory_as_not_new()
    print("OK")
