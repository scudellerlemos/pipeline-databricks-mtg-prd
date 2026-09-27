# ponytail: pure-logic self-check for bronze_utils.py's non-trivial bits -
# idempotency (which stage files are "new") and schema-diff logging. Can't
# import bronze_utils.py directly (pyspark + live dbutils/%run required), so
# this mirrors just the decision logic in plain Python.


def normalize_path(path):
    """Mirrors bronze_utils.normalize_path: strip URI scheme and truncate to
    the ".parquet" directory level (df.write.save() always writes a
    directory - _metadata.file_path points at a part-file inside it)."""
    path = path.split("://", 1)[-1]
    if ".parquet/" in path:
        path = path.split(".parquet/", 1)[0] + ".parquet"
    return path


def list_stage_files_filter(names, contents=None):
    """Mirrors list_stage_files' filter: dbutils.fs.ls names a directory with
    a trailing "/" (Spark's .save(path) always writes `path` as a directory),
    so the check must strip it before comparing the ".parquet" suffix.

    contents maps a directory name to what's inside it. A directory with no
    part-file is a write that started and never committed (only the
    _started_* commit marker is left); it must be skipped, or read.parquet
    fails the whole Bronze run with UNABLE_TO_INFER_SCHEMA."""
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
    """Mirrors run_bronze_ingestion's idempotency filter: files present in
    Stage but not yet reflected by any source_file already in Bronze."""
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
    # already_loaded_files espelha o retorno (já normalizado, sem esquema de
    # URI) de get_already_loaded_files.
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
    # 2nd run same day: Stage's save_to_parquet já pulou a escrita de um
    # arquivo novo (nome do arquivo inclui o dia), então a Bronze também
    # não vê arquivo novo.
    all_files = ["s3://b/stage/2026_09_14_cards.parquet"]
    already = {"b/stage/2026_09_14_cards.parquet"}
    assert find_new_files(all_files, already) == []


def test_scheme_mismatch_does_not_cause_reprocessing():
    # dbutils.fs.ls() pode devolver s3:// enquanto _metadata.file_path (já
    # normalizado em get_already_loaded_files) devolveu s3a:// pro mesmo
    # arquivo - sem normalize_path, isto reprocessaria e duplicaria histórico.
    all_files = ["s3://b/stage/2026_09_14_cards.parquet"]
    already = {"b/stage/2026_09_14_cards.parquet"}  # já normalizado (sem esquema)
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
    """Mirrors list_stage_files' table_path: each Stage table now has its
    own subfolder instead of a shared flat directory filtered by suffix."""
    return f"{s3_stage_path}/{stage_table_name}"


def test_stage_table_path_is_per_table_subfolder():
    assert stage_table_path("s3://b/stage", "cards") == "s3://b/stage/cards"
    assert stage_table_path("s3://b/stage", "card_prices") == "s3://b/stage/card_prices"


def test_list_stage_files_filter_matches_directory_entries():
    # dbutils.fs.ls nomeia diretório com "/" no final - sem rstrip, o filtro
    # nunca batia e list_stage_files devolvia sempre [] (bug real: toda run
    # caía no branch idempotente "nada a fazer", mesmo com dado novo).
    names = ["2026_09_15_cards.parquet/", "_SUCCESS", "2026_09_15_cards.parquet.crc"]
    assert list_stage_files_filter(names) == ["2026_09_15_cards.parquet/"]


def test_uncommitted_write_directory_is_skipped():
    # Bug real: 2022_04_16_card_prices.parquet/ ficou no S3 com só o marcador
    # _started_* (escrita que não commitou, mascarada por um falso sucesso na
    # Stage). Entrava em new_files e derrubava a Bronze inteira com
    # UNABLE_TO_INFER_SCHEMA.
    names = ["ok.parquet/", "quebrado.parquet/"]
    contents = {
        "ok.parquet/": ["part-00000-x.snappy.parquet", "_SUCCESS"],
        "quebrado.parquet/": ["_started_5021188249195991183"],
    }
    assert list_stage_files_filter(names, contents) == ["ok.parquet/"]


def test_normalize_path_truncates_part_file_to_parquet_dir():
    # _metadata.file_path aponta pro part-file dentro do diretório ".parquet";
    # list_stage_files devolve o diretório em si - sem truncar, nunca bateriam.
    part_file = "s3://b/stage/cards/2026_09_15_cards.parquet/part-00000-x.snappy.parquet"
    directory = "s3://b/stage/cards/2026_09_15_cards.parquet"
    assert normalize_path(part_file) == normalize_path(directory)


def test_already_loaded_part_file_marks_directory_as_not_new():
    # Reproduz o fluxo real: get_already_loaded_files devolve o part-file
    # (via _metadata.file_path); list_stage_files devolve o diretório. Depois
    # da normalização, o mesmo arquivo da Stage não deve ser visto como novo.
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
