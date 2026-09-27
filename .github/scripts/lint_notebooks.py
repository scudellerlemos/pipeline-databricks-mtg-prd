#!/usr/bin/env python3
"""Static lint of the Python code inside notebooks (undefined names, syntax errors) with ruff."""
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

RAIZ_REPO = Path(__file__).resolve().parents[2]
# Magics whose cell is entirely non-Python. Matched as exact tokens (%run is
# handled in processar_celula; a prefix match on "%r" would catch it).
MAGICS_IGNORADOS = {"%sql", "%scala", "%md", "%sh", "%fs", "%r"}
REGEX_RUN = re.compile(r"^%run\s+(.+)$")
CABECALHO_NOTEBOOK = "# Databricks notebook source"
REGEX_SEPARADOR_COMANDO = re.compile(r"^# COMMAND -+$")
REGEX_LINHA_MAGIC = re.compile(r"^# MAGIC ?(.*)$")
REGEX_IMPORT_ESTRELA = re.compile(r"^from\s+(\S+)\s+import\s+\*\s*$")
STUB_DATABRICKS = "spark = dbutils = display = displayHTML = sqlContext = table = None\n"
# `import *` disables ruff's undefined-name check (F821) for the whole file, so
# star imports are replaced by stubs of the module's public names. Lists copied
# from each module's __all__ (pyspark isn't installed in CI); a name from a newer
# pyspark release missing here shows up as undefined, refresh the list.
# countDistinct is a deprecated alias not in __all__, added by hand.
_NOMES_FUNCOES = ['countDistinct', 'AnalyzeArgument', 'AnalyzeResult', 'ArrowUDFType', 'OrderingColumn', 'PandasUDFType', 'PartitioningColumn', 'SelectedColumn', 'SkipRestOfInputTableException', 'UserDefinedFunction', 'UserDefinedTableFunction', 'abs', 'acos', 'acosh', 'add_months', 'aes_decrypt', 'aes_encrypt', 'aggregate', 'any_value', 'approx_count_distinct', 'approx_percentile', 'array', 'array_agg', 'array_append', 'array_compact', 'array_contains', 'array_distinct', 'array_except', 'array_insert', 'array_intersect', 'array_join', 'array_max', 'array_min', 'array_position', 'array_prepend', 'array_remove', 'array_repeat', 'array_size', 'array_sort', 'array_union', 'arrays_overlap', 'arrays_zip', 'arrow_udf', 'arrow_udtf', 'asc', 'asc_nulls_first', 'asc_nulls_last', 'ascii', 'asin', 'asinh', 'assert_true', 'atan', 'atan2', 'atanh', 'avg', 'base64', 'bin', 'bit_and', 'bit_count', 'bit_get', 'bit_length', 'bit_or', 'bit_xor', 'bitmap_and_agg', 'bitmap_bit_position', 'bitmap_bucket_number', 'bitmap_construct_agg', 'bitmap_count', 'bitmap_or_agg', 'bitwise_not', 'bool_and', 'bool_or', 'broadcast', 'bround', 'btrim', 'bucket', 'call_function', 'call_udf', 'cardinality', 'cbrt', 'ceil', 'ceiling', 'char', 'char_length', 'character_length', 'coalesce', 'col', 'collate', 'collation', 'collect_list', 'collect_set', 'column', 'concat', 'concat_ws', 'contains', 'conv', 'convert_timezone', 'corr', 'cos', 'cosh', 'cot', 'count', 'count_distinct', 'count_if', 'count_min_sketch', 'covar_pop', 'covar_samp', 'crc32', 'create_map', 'csc', 'cume_dist', 'curdate', 'current_catalog', 'current_database', 'current_date', 'current_path', 'current_schema', 'current_time', 'current_timestamp', 'current_timezone', 'current_user', 'date_add', 'date_diff', 'date_format', 'date_from_unix_date', 'date_part', 'date_sub', 'date_trunc', 'dateadd', 'datediff', 'datepart', 'day', 'dayname', 'dayofmonth', 'dayofweek', 'dayofyear', 'days', 'decode', 'degrees', 'dense_rank', 'desc', 'desc_nulls_first', 'desc_nulls_last', 'e', 'element_at', 'elt', 'encode', 'endswith', 'equal_null', 'every', 'exists', 'exp', 'explode', 'explode_outer', 'expm1', 'expr', 'extract', 'factorial', 'filter', 'find_in_set', 'first', 'first_value', 'flatten', 'floor', 'forall', 'format_number', 'format_string', 'from_csv', 'from_json', 'from_unixtime', 'from_utc_timestamp', 'from_xml', 'get', 'get_json_object', 'getbit', 'greatest', 'grouping', 'grouping_id', 'hash', 'hex', 'histogram_numeric', 'hll_sketch_agg', 'hll_sketch_estimate', 'hll_union', 'hll_union_agg', 'hour', 'hours', 'hypot', 'ifnull', 'ilike', 'initcap', 'inline', 'inline_outer', 'input_file_block_length', 'input_file_block_start', 'input_file_name', 'instr', 'is_valid_utf8', 'is_valid_variant', 'is_variant_null', 'isnan', 'isnotnull', 'isnull', 'java_method', 'json_array_length', 'json_object_keys', 'json_tuple', 'kll_merge_agg_bigint', 'kll_merge_agg_double', 'kll_merge_agg_float', 'kll_sketch_agg_bigint', 'kll_sketch_agg_double', 'kll_sketch_agg_float', 'kll_sketch_get_n_bigint', 'kll_sketch_get_n_double', 'kll_sketch_get_n_float', 'kll_sketch_get_quantile_bigint', 'kll_sketch_get_quantile_double', 'kll_sketch_get_quantile_float', 'kll_sketch_get_rank_bigint', 'kll_sketch_get_rank_double', 'kll_sketch_get_rank_float', 'kll_sketch_merge_bigint', 'kll_sketch_merge_double', 'kll_sketch_merge_float', 'kll_sketch_to_string_bigint', 'kll_sketch_to_string_double', 'kll_sketch_to_string_float', 'kurtosis', 'lag', 'last', 'last_day', 'last_value', 'lcase', 'lead', 'least', 'left', 'length', 'levenshtein', 'like', 'listagg', 'listagg_distinct', 'lit', 'ln', 'localtimestamp', 'locate', 'log', 'log10', 'log1p', 'log2', 'lower', 'lpad', 'ltrim', 'make_date', 'make_dt_interval', 'make_interval', 'make_time', 'make_timestamp', 'make_timestamp_ltz', 'make_timestamp_ntz', 'make_valid_utf8', 'make_ym_interval', 'map_concat', 'map_contains_key', 'map_entries', 'map_filter', 'map_from_arrays', 'map_from_entries', 'map_keys', 'map_values', 'map_zip_with', 'mask', 'max', 'max_by', 'md5', 'mean', 'median', 'min', 'min_by', 'minute', 'mode', 'monotonically_increasing_id', 'month', 'monthname', 'months', 'months_between', 'named_struct', 'nanvl', 'negate', 'negative', 'next_day', 'now', 'nth_value', 'ntile', 'nullif', 'nullifzero', 'nvl', 'nvl2', 'octet_length', 'overlay', 'pandas_udf', 'parse_json', 'parse_url', 'percent_rank', 'percentile', 'percentile_approx', 'pi', 'pmod', 'posexplode', 'posexplode_outer', 'position', 'positive', 'pow', 'power', 'printf', 'product', 'quarter', 'quote', 'radians', 'raise_error', 'rand', 'randn', 'randstr', 'rank', 'reduce', 'reflect', 'regexp', 'regexp_count', 'regexp_extract', 'regexp_extract_all', 'regexp_instr', 'regexp_like', 'regexp_replace', 'regexp_substr', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'repeat', 'replace', 'reverse', 'right', 'rint', 'rlike', 'round', 'row_number', 'rpad', 'rtrim', 'schema_of_csv', 'schema_of_json', 'schema_of_variant', 'schema_of_variant_agg', 'schema_of_xml', 'sec', 'second', 'sentences', 'sequence', 'session_user', 'session_window', 'sha', 'sha1', 'sha2', 'shiftleft', 'shiftright', 'shiftrightunsigned', 'shuffle', 'sign', 'signum', 'sin', 'sinh', 'size', 'skewness', 'slice', 'some', 'sort_array', 'soundex', 'spark_partition_id', 'split', 'split_part', 'sqrt', 'st_asbinary', 'st_geogfromwkb', 'st_geomfromwkb', 'st_setsrid', 'st_srid', 'stack', 'startswith', 'std', 'stddev', 'stddev_pop', 'stddev_samp', 'str_to_map', 'string_agg', 'string_agg_distinct', 'struct', 'substr', 'substring', 'substring_index', 'sum', 'sum_distinct', 'tan', 'tanh', 'theta_difference', 'theta_intersection', 'theta_intersection_agg', 'theta_sketch_agg', 'theta_sketch_estimate', 'theta_union', 'theta_union_agg', 'time_bucket', 'time_diff', 'time_from_micros', 'time_from_millis', 'time_from_seconds', 'time_to_micros', 'time_to_millis', 'time_to_seconds', 'time_trunc', 'timestamp_add', 'timestamp_diff', 'timestamp_micros', 'timestamp_millis', 'timestamp_seconds', 'to_binary', 'to_char', 'to_csv', 'to_date', 'to_json', 'to_number', 'to_time', 'to_timestamp', 'to_timestamp_ltz', 'to_timestamp_ntz', 'to_unix_timestamp', 'to_utc_timestamp', 'to_varchar', 'to_variant_object', 'to_xml', 'transform', 'transform_keys', 'transform_values', 'translate', 'trim', 'trunc', 'try_add', 'try_aes_decrypt', 'try_avg', 'try_divide', 'try_element_at', 'try_make_interval', 'try_make_timestamp', 'try_make_timestamp_ltz', 'try_make_timestamp_ntz', 'try_mod', 'try_multiply', 'try_parse_json', 'try_parse_url', 'try_reflect', 'try_subtract', 'try_sum', 'try_to_binary', 'try_to_date', 'try_to_number', 'try_to_time', 'try_to_timestamp', 'try_url_decode', 'try_validate_utf8', 'try_variant_get', 'tuple_difference_double', 'tuple_difference_integer', 'tuple_difference_theta_double', 'tuple_difference_theta_integer', 'tuple_intersection_agg_double', 'tuple_intersection_agg_integer', 'tuple_intersection_double', 'tuple_intersection_integer', 'tuple_intersection_theta_double', 'tuple_intersection_theta_integer', 'tuple_sketch_agg_double', 'tuple_sketch_agg_integer', 'tuple_sketch_estimate_double', 'tuple_sketch_estimate_integer', 'tuple_sketch_summary_double', 'tuple_sketch_summary_integer', 'tuple_sketch_theta_double', 'tuple_sketch_theta_integer', 'tuple_union_agg_double', 'tuple_union_agg_integer', 'tuple_union_double', 'tuple_union_integer', 'tuple_union_theta_double', 'tuple_union_theta_integer', 'typeof', 'ucase', 'udf', 'udtf', 'unbase64', 'unhex', 'uniform', 'unix_date', 'unix_micros', 'unix_millis', 'unix_seconds', 'unix_timestamp', 'unwrap_udt', 'upper', 'url_decode', 'url_encode', 'user', 'validate_utf8', 'var_pop', 'var_samp', 'variance', 'variant_get', 'version', 'weekday', 'weekofyear', 'when', 'width_bucket', 'window', 'window_time', 'xpath', 'xpath_boolean', 'xpath_double', 'xpath_float', 'xpath_int', 'xpath_long', 'xpath_number', 'xpath_short', 'xpath_string', 'xxhash64', 'year', 'years', 'zeroifnull', 'zip_with']
_NOMES_TIPOS = ['ArrayType', 'BinaryType', 'BooleanType', 'ByteType', 'CalendarIntervalType', 'CharType', 'DataType', 'DateType', 'DayTimeIntervalType', 'DecimalType', 'DoubleType', 'FloatType', 'Geography', 'GeographyType', 'Geometry', 'GeometryType', 'IntegerType', 'LongType', 'MapType', 'NullType', 'Row', 'ShortType', 'StringType', 'StructField', 'StructType', 'TimeType', 'TimestampNTZType', 'TimestampType', 'VarcharType', 'VariantType', 'VariantVal', 'YearMonthIntervalType']
STUBS_IMPORT_ESTRELA = {
    "pyspark.sql.functions": " = ".join(_NOMES_FUNCOES) + " = None\n",
    "pyspark.sql.types": " = ".join(_NOMES_TIPOS) + " = None\n",
}


def resolver_alvo_run(caminho_notebook, referencia):
    referencia = referencia.strip().strip("\"'")
    candidato = (caminho_notebook.parent / referencia).with_suffix(".py")
    return candidato if candidato.exists() else None


def substituir_imports_estrela(texto):
    saida = []
    for linha in texto.splitlines():
        indentacao = linha[: len(linha) - len(linha.lstrip())]
        casamento_estrela = REGEX_IMPORT_ESTRELA.match(linha.lstrip())
        stub = STUBS_IMPORT_ESTRELA.get(casamento_estrela.group(1)) if casamento_estrela else None
        saida.append(indentacao + stub.rstrip("\n") if stub else linha)
    return "\n".join(saida)


def processar_celula(fonte, caminho_notebook, visitados, trechos):
    primeira_linha = next((l for l in fonte.splitlines() if l.strip()), "")
    primeiro_token = primeira_linha.lstrip().split(None, 1)[0] if primeira_linha.strip() else ""
    if primeiro_token in MAGICS_IGNORADOS or primeiro_token.startswith("%%"):
        return  # whole cell is non-Python (%sql, %md, ...)

    mantidas = []
    for linha in fonte.splitlines():
        sem_indentacao = linha.lstrip()
        casamento_run = REGEX_RUN.match(sem_indentacao)
        if casamento_run:
            # %run inlines the target notebook at runtime; inline it here too so
            # the names it defines (e.g. obter_segredo) aren't flagged undefined.
            alvo = resolver_alvo_run(caminho_notebook, casamento_run.group(1))
            if alvo and alvo not in visitados:
                visitados.add(alvo)
                trechos.append(substituir_imports_estrela(alvo.read_text(encoding="utf-8")))
            continue
        if sem_indentacao.startswith("%"):
            continue  # drop other inline magics
        mantidas.append(linha)
    trechos.append(substituir_imports_estrela("\n".join(mantidas)))


def iterar_celulas_formato_fonte(texto):
    # Databricks "source format" .py: cells split on "# COMMAND ----------",
    # magic lines prefixed "# MAGIC ". Strips the prefix so cells look like .ipynb cells.
    linhas = texto.splitlines()
    if linhas and linhas[0].strip() == CABECALHO_NOTEBOOK:
        linhas = linhas[1:]
    linhas_celula = []
    for linha in linhas:
        if REGEX_SEPARADOR_COMANDO.match(linha):
            yield "\n".join(linhas_celula)
            linhas_celula = []
            continue
        casamento_magic = REGEX_LINHA_MAGIC.match(linha)
        linhas_celula.append(casamento_magic.group(1) if casamento_magic else linha)
    yield "\n".join(linhas_celula)


def eh_notebook_formato_fonte(caminho):
    try:
        primeira_linha = caminho.read_text(encoding="utf-8").splitlines()[0]
    except IndexError:
        return False
    return primeira_linha.strip() == CABECALHO_NOTEBOOK


def extrair_python(caminho_notebook, visitados):
    trechos = []
    if caminho_notebook.suffix == ".py":
        for fonte_celula in iterar_celulas_formato_fonte(caminho_notebook.read_text(encoding="utf-8")):
            processar_celula(fonte_celula, caminho_notebook, visitados, trechos)
        return "\n\n".join(trechos)

    notebook = json.loads(caminho_notebook.read_text(encoding="utf-8"))
    for celula in notebook.get("cells", []):
        if celula.get("cell_type") != "code":
            continue
        fonte = "".join(celula.get("source", []))
        processar_celula(fonte, caminho_notebook, visitados, trechos)
    return "\n\n".join(trechos)


def main():
    notebooks = sorted(RAIZ_REPO.glob("src/**/*.ipynb")) + sorted(
        p for p in RAIZ_REPO.glob("src/**/*.py") if eh_notebook_formato_fonte(p)
    )
    falhou = False
    with tempfile.TemporaryDirectory() as dir_temp:
        for caminho_nb in notebooks:
            codigo = STUB_DATABRICKS + extrair_python(caminho_nb, {caminho_nb})
            arquivo_temp = Path(dir_temp) / (caminho_nb.stem + ".py")
            arquivo_temp.write_text(codigo, encoding="utf-8")
            resultado = subprocess.run(
                ["ruff", "check", "--select=F821,F822,F823", "--quiet", str(arquivo_temp)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if resultado.stdout.strip():
                print(f"FAIL {caminho_nb.relative_to(RAIZ_REPO)}")
                print(resultado.stdout)
                falhou = True
    if falhou:
        sys.exit(1)
    print(f"OK: {len(notebooks)} notebooks passed static lint (ruff F821/F822/F823)")


if __name__ == "__main__":
    main()
