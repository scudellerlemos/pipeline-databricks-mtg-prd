#!/usr/bin/env python3
"""AUD-10: statically lints the real Python code inside notebooks (undefined
names, syntax errors) instead of only validating YAML/JSON structure.
"""
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# exact magic tokens whose cell is entirely non-Python (%run is handled
# separately below, not here - "%run".startswith("%r") would else misfire)
SKIP_MAGICS = {"%sql", "%scala", "%md", "%sh", "%fs", "%r"}
RUN_RE = re.compile(r"^%run\s+(.+)$")
NOTEBOOK_SOURCE_HEADER = "# Databricks notebook source"
COMMAND_SEP_RE = re.compile(r"^# COMMAND -+$")
MAGIC_LINE_RE = re.compile(r"^# MAGIC ?(.*)$")
STAR_IMPORT_RE = re.compile(r"^from\s+(\S+)\s+import\s+\*\s*$")
DATABRICKS_STUB = "spark = dbutils = display = displayHTML = sqlContext = table = None\n"
# `import *` disables pyflakes' undefined-name check for the WHOLE file (it
# can't know what names the star brings in), so F821 silently checks nothing
# on every notebook here - they all wildcard-import these two modules.
# Fix: drop the star import and stub its public names as locals instead, so
# ruff can actually flag genuinely undefined names. Lists taken from each
# module's own __all__ (pyspark not installed in this CI); a name added to a
# newer pyspark release before this list is refreshed would false-positive
# as undefined here.
# countDistinct: deprecated camelCase alias still used in this repo's
# notebooks and still defined in pyspark, but dropped from __all__ (only
# count_distinct is exported now) - added back manually so it isn't
# flagged as undefined.
_FUNCTIONS_ALL = ['countDistinct', 'AnalyzeArgument', 'AnalyzeResult', 'ArrowUDFType', 'OrderingColumn', 'PandasUDFType', 'PartitioningColumn', 'SelectedColumn', 'SkipRestOfInputTableException', 'UserDefinedFunction', 'UserDefinedTableFunction', 'abs', 'acos', 'acosh', 'add_months', 'aes_decrypt', 'aes_encrypt', 'aggregate', 'any_value', 'approx_count_distinct', 'approx_percentile', 'array', 'array_agg', 'array_append', 'array_compact', 'array_contains', 'array_distinct', 'array_except', 'array_insert', 'array_intersect', 'array_join', 'array_max', 'array_min', 'array_position', 'array_prepend', 'array_remove', 'array_repeat', 'array_size', 'array_sort', 'array_union', 'arrays_overlap', 'arrays_zip', 'arrow_udf', 'arrow_udtf', 'asc', 'asc_nulls_first', 'asc_nulls_last', 'ascii', 'asin', 'asinh', 'assert_true', 'atan', 'atan2', 'atanh', 'avg', 'base64', 'bin', 'bit_and', 'bit_count', 'bit_get', 'bit_length', 'bit_or', 'bit_xor', 'bitmap_and_agg', 'bitmap_bit_position', 'bitmap_bucket_number', 'bitmap_construct_agg', 'bitmap_count', 'bitmap_or_agg', 'bitwise_not', 'bool_and', 'bool_or', 'broadcast', 'bround', 'btrim', 'bucket', 'call_function', 'call_udf', 'cardinality', 'cbrt', 'ceil', 'ceiling', 'char', 'char_length', 'character_length', 'coalesce', 'col', 'collate', 'collation', 'collect_list', 'collect_set', 'column', 'concat', 'concat_ws', 'contains', 'conv', 'convert_timezone', 'corr', 'cos', 'cosh', 'cot', 'count', 'count_distinct', 'count_if', 'count_min_sketch', 'covar_pop', 'covar_samp', 'crc32', 'create_map', 'csc', 'cume_dist', 'curdate', 'current_catalog', 'current_database', 'current_date', 'current_path', 'current_schema', 'current_time', 'current_timestamp', 'current_timezone', 'current_user', 'date_add', 'date_diff', 'date_format', 'date_from_unix_date', 'date_part', 'date_sub', 'date_trunc', 'dateadd', 'datediff', 'datepart', 'day', 'dayname', 'dayofmonth', 'dayofweek', 'dayofyear', 'days', 'decode', 'degrees', 'dense_rank', 'desc', 'desc_nulls_first', 'desc_nulls_last', 'e', 'element_at', 'elt', 'encode', 'endswith', 'equal_null', 'every', 'exists', 'exp', 'explode', 'explode_outer', 'expm1', 'expr', 'extract', 'factorial', 'filter', 'find_in_set', 'first', 'first_value', 'flatten', 'floor', 'forall', 'format_number', 'format_string', 'from_csv', 'from_json', 'from_unixtime', 'from_utc_timestamp', 'from_xml', 'get', 'get_json_object', 'getbit', 'greatest', 'grouping', 'grouping_id', 'hash', 'hex', 'histogram_numeric', 'hll_sketch_agg', 'hll_sketch_estimate', 'hll_union', 'hll_union_agg', 'hour', 'hours', 'hypot', 'ifnull', 'ilike', 'initcap', 'inline', 'inline_outer', 'input_file_block_length', 'input_file_block_start', 'input_file_name', 'instr', 'is_valid_utf8', 'is_valid_variant', 'is_variant_null', 'isnan', 'isnotnull', 'isnull', 'java_method', 'json_array_length', 'json_object_keys', 'json_tuple', 'kll_merge_agg_bigint', 'kll_merge_agg_double', 'kll_merge_agg_float', 'kll_sketch_agg_bigint', 'kll_sketch_agg_double', 'kll_sketch_agg_float', 'kll_sketch_get_n_bigint', 'kll_sketch_get_n_double', 'kll_sketch_get_n_float', 'kll_sketch_get_quantile_bigint', 'kll_sketch_get_quantile_double', 'kll_sketch_get_quantile_float', 'kll_sketch_get_rank_bigint', 'kll_sketch_get_rank_double', 'kll_sketch_get_rank_float', 'kll_sketch_merge_bigint', 'kll_sketch_merge_double', 'kll_sketch_merge_float', 'kll_sketch_to_string_bigint', 'kll_sketch_to_string_double', 'kll_sketch_to_string_float', 'kurtosis', 'lag', 'last', 'last_day', 'last_value', 'lcase', 'lead', 'least', 'left', 'length', 'levenshtein', 'like', 'listagg', 'listagg_distinct', 'lit', 'ln', 'localtimestamp', 'locate', 'log', 'log10', 'log1p', 'log2', 'lower', 'lpad', 'ltrim', 'make_date', 'make_dt_interval', 'make_interval', 'make_time', 'make_timestamp', 'make_timestamp_ltz', 'make_timestamp_ntz', 'make_valid_utf8', 'make_ym_interval', 'map_concat', 'map_contains_key', 'map_entries', 'map_filter', 'map_from_arrays', 'map_from_entries', 'map_keys', 'map_values', 'map_zip_with', 'mask', 'max', 'max_by', 'md5', 'mean', 'median', 'min', 'min_by', 'minute', 'mode', 'monotonically_increasing_id', 'month', 'monthname', 'months', 'months_between', 'named_struct', 'nanvl', 'negate', 'negative', 'next_day', 'now', 'nth_value', 'ntile', 'nullif', 'nullifzero', 'nvl', 'nvl2', 'octet_length', 'overlay', 'pandas_udf', 'parse_json', 'parse_url', 'percent_rank', 'percentile', 'percentile_approx', 'pi', 'pmod', 'posexplode', 'posexplode_outer', 'position', 'positive', 'pow', 'power', 'printf', 'product', 'quarter', 'quote', 'radians', 'raise_error', 'rand', 'randn', 'randstr', 'rank', 'reduce', 'reflect', 'regexp', 'regexp_count', 'regexp_extract', 'regexp_extract_all', 'regexp_instr', 'regexp_like', 'regexp_replace', 'regexp_substr', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'repeat', 'replace', 'reverse', 'right', 'rint', 'rlike', 'round', 'row_number', 'rpad', 'rtrim', 'schema_of_csv', 'schema_of_json', 'schema_of_variant', 'schema_of_variant_agg', 'schema_of_xml', 'sec', 'second', 'sentences', 'sequence', 'session_user', 'session_window', 'sha', 'sha1', 'sha2', 'shiftleft', 'shiftright', 'shiftrightunsigned', 'shuffle', 'sign', 'signum', 'sin', 'sinh', 'size', 'skewness', 'slice', 'some', 'sort_array', 'soundex', 'spark_partition_id', 'split', 'split_part', 'sqrt', 'st_asbinary', 'st_geogfromwkb', 'st_geomfromwkb', 'st_setsrid', 'st_srid', 'stack', 'startswith', 'std', 'stddev', 'stddev_pop', 'stddev_samp', 'str_to_map', 'string_agg', 'string_agg_distinct', 'struct', 'substr', 'substring', 'substring_index', 'sum', 'sum_distinct', 'tan', 'tanh', 'theta_difference', 'theta_intersection', 'theta_intersection_agg', 'theta_sketch_agg', 'theta_sketch_estimate', 'theta_union', 'theta_union_agg', 'time_bucket', 'time_diff', 'time_from_micros', 'time_from_millis', 'time_from_seconds', 'time_to_micros', 'time_to_millis', 'time_to_seconds', 'time_trunc', 'timestamp_add', 'timestamp_diff', 'timestamp_micros', 'timestamp_millis', 'timestamp_seconds', 'to_binary', 'to_char', 'to_csv', 'to_date', 'to_json', 'to_number', 'to_time', 'to_timestamp', 'to_timestamp_ltz', 'to_timestamp_ntz', 'to_unix_timestamp', 'to_utc_timestamp', 'to_varchar', 'to_variant_object', 'to_xml', 'transform', 'transform_keys', 'transform_values', 'translate', 'trim', 'trunc', 'try_add', 'try_aes_decrypt', 'try_avg', 'try_divide', 'try_element_at', 'try_make_interval', 'try_make_timestamp', 'try_make_timestamp_ltz', 'try_make_timestamp_ntz', 'try_mod', 'try_multiply', 'try_parse_json', 'try_parse_url', 'try_reflect', 'try_subtract', 'try_sum', 'try_to_binary', 'try_to_date', 'try_to_number', 'try_to_time', 'try_to_timestamp', 'try_url_decode', 'try_validate_utf8', 'try_variant_get', 'tuple_difference_double', 'tuple_difference_integer', 'tuple_difference_theta_double', 'tuple_difference_theta_integer', 'tuple_intersection_agg_double', 'tuple_intersection_agg_integer', 'tuple_intersection_double', 'tuple_intersection_integer', 'tuple_intersection_theta_double', 'tuple_intersection_theta_integer', 'tuple_sketch_agg_double', 'tuple_sketch_agg_integer', 'tuple_sketch_estimate_double', 'tuple_sketch_estimate_integer', 'tuple_sketch_summary_double', 'tuple_sketch_summary_integer', 'tuple_sketch_theta_double', 'tuple_sketch_theta_integer', 'tuple_union_agg_double', 'tuple_union_agg_integer', 'tuple_union_double', 'tuple_union_integer', 'tuple_union_theta_double', 'tuple_union_theta_integer', 'typeof', 'ucase', 'udf', 'udtf', 'unbase64', 'unhex', 'uniform', 'unix_date', 'unix_micros', 'unix_millis', 'unix_seconds', 'unix_timestamp', 'unwrap_udt', 'upper', 'url_decode', 'url_encode', 'user', 'validate_utf8', 'var_pop', 'var_samp', 'variance', 'variant_get', 'version', 'weekday', 'weekofyear', 'when', 'width_bucket', 'window', 'window_time', 'xpath', 'xpath_boolean', 'xpath_double', 'xpath_float', 'xpath_int', 'xpath_long', 'xpath_number', 'xpath_short', 'xpath_string', 'xxhash64', 'year', 'years', 'zeroifnull', 'zip_with']
_TYPES_ALL = ['ArrayType', 'BinaryType', 'BooleanType', 'ByteType', 'CalendarIntervalType', 'CharType', 'DataType', 'DateType', 'DayTimeIntervalType', 'DecimalType', 'DoubleType', 'FloatType', 'Geography', 'GeographyType', 'Geometry', 'GeometryType', 'IntegerType', 'LongType', 'MapType', 'NullType', 'Row', 'ShortType', 'StringType', 'StructField', 'StructType', 'TimeType', 'TimestampNTZType', 'TimestampType', 'VarcharType', 'VariantType', 'VariantVal', 'YearMonthIntervalType']
STAR_IMPORT_STUBS = {
    "pyspark.sql.functions": " = ".join(_FUNCTIONS_ALL) + " = None\n",
    "pyspark.sql.types": " = ".join(_TYPES_ALL) + " = None\n",
}


def resolve_run_target(notebook_path, ref):
    ref = ref.strip().strip("\"'")
    candidate = (notebook_path.parent / ref).with_suffix(".py")
    return candidate if candidate.exists() else None


def substitute_star_imports(text):
    out = []
    for line in text.splitlines():
        indent = line[: len(line) - len(line.lstrip())]
        star_match = STAR_IMPORT_RE.match(line.lstrip())
        stub = STAR_IMPORT_STUBS.get(star_match.group(1)) if star_match else None
        out.append(indent + stub.rstrip("\n") if stub else line)
    return "\n".join(out)


def process_cell(source, notebook_path, visited, chunks):
    first_line = next((l for l in source.splitlines() if l.strip()), "")
    first_token = first_line.lstrip().split(None, 1)[0] if first_line.strip() else ""
    if first_token in SKIP_MAGICS or first_token.startswith("%%"):
        return  # whole cell is non-Python (%sql, %md, ...)

    kept = []
    for line in source.splitlines():
        stripped = line.lstrip()
        run_match = RUN_RE.match(stripped)
        if run_match:
            # %run inlines another notebook/source-linked .py into this
            # namespace at runtime (Databricks Repos) - mirror that here
            # so names it defines (e.g. get_secret) aren't flagged undefined.
            target = resolve_run_target(notebook_path, run_match.group(1))
            if target and target not in visited:
                visited.add(target)
                chunks.append(substitute_star_imports(target.read_text(encoding="utf-8")))
            continue
        if stripped.startswith("%"):
            continue  # drop other inline magics
        kept.append(line)
    chunks.append(substitute_star_imports("\n".join(kept)))


def iter_source_format_cells(text):
    # Databricks "source format" .py: header line, cells split on
    # "# COMMAND ----------", non-Python magic lines prefixed "# MAGIC ".
    # Strip that prefix back off so each cell's text matches the shape an
    # .ipynb cell's source already has, and process_cell needs no changes.
    lines = text.splitlines()
    if lines and lines[0].strip() == NOTEBOOK_SOURCE_HEADER:
        lines = lines[1:]
    cell_lines = []
    for line in lines:
        if COMMAND_SEP_RE.match(line):
            yield "\n".join(cell_lines)
            cell_lines = []
            continue
        magic_match = MAGIC_LINE_RE.match(line)
        cell_lines.append(magic_match.group(1) if magic_match else line)
    yield "\n".join(cell_lines)


def is_source_format_notebook(path):
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except IndexError:
        return False
    return first_line.strip() == NOTEBOOK_SOURCE_HEADER


def extract_python(notebook_path, visited):
    chunks = []
    if notebook_path.suffix == ".py":
        for cell_source in iter_source_format_cells(notebook_path.read_text(encoding="utf-8")):
            process_cell(cell_source, notebook_path, visited, chunks)
        return "\n\n".join(chunks)

    nb = json.loads(notebook_path.read_text(encoding="utf-8"))
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        process_cell(source, notebook_path, visited, chunks)
    return "\n\n".join(chunks)


def main():
    notebooks = sorted(REPO_ROOT.glob("src/**/*.ipynb")) + sorted(
        p for p in REPO_ROOT.glob("src/**/*.py") if is_source_format_notebook(p)
    )
    failed = False
    with tempfile.TemporaryDirectory() as tmp:
        for nb_path in notebooks:
            code = DATABRICKS_STUB + extract_python(nb_path, {nb_path})
            tmp_file = Path(tmp) / (nb_path.stem + ".py")
            tmp_file.write_text(code, encoding="utf-8")
            result = subprocess.run(
                ["ruff", "check", "--select=F821,F822,F823", "--quiet", str(tmp_file)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if result.stdout.strip():
                print(f"FAIL {nb_path.relative_to(REPO_ROOT)}")
                print(result.stdout)
                failed = True
    if failed:
        sys.exit(1)
    print(f"OK: {len(notebooks)} notebooks passed static lint (ruff F821/F822/F823)")


if __name__ == "__main__":
    main()
