from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Dict, Iterable, List

from pyspark.sql import Window
from pyspark.sql import functions as F

from tests.v4_pipeline_selector import resolve_pipeline_path


SUMMARY_HISTORY_LEN = 36
LATEST_HISTORY_LEN = 72
HISTORY_COLS = [
    "actual_payment_am_history",
    "balance_am_history",
    "credit_limit_am_history",
    "past_due_am_history",
    "payment_rating_cd_history",
    "days_past_due_history",
    "asset_class_cd_4in_history",
]
DELETE_CODES = ("1", "4")


def ensure_dependency_stubs() -> None:
    try:
        import boto3  # noqa: F401
    except Exception:
        boto3_stub = types.ModuleType("boto3")

        def _unsupported_client(*args, **kwargs):
            raise RuntimeError("boto3 client should not be used in v4 local tests")

        boto3_stub.client = _unsupported_client  # type: ignore[attr-defined]
        sys.modules["boto3"] = boto3_stub

    try:
        from dateutil.relativedelta import relativedelta  # noqa: F401
    except Exception:
        dateutil_mod = types.ModuleType("dateutil")
        relativedelta_mod = types.ModuleType("dateutil.relativedelta")

        class relativedelta:  # type: ignore
            def __init__(self, months: int = 0):
                self.months = months

        relativedelta_mod.relativedelta = relativedelta  # type: ignore[attr-defined]
        dateutil_mod.relativedelta = relativedelta_mod  # type: ignore[attr-defined]
        sys.modules["dateutil"] = dateutil_mod
        sys.modules["dateutil.relativedelta"] = relativedelta_mod


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_v4_as_summary_inc(cache_name: str) -> Any:
    ensure_dependency_stubs()
    cached = sys.modules.get(cache_name)
    if cached is not None:
        sys.modules["summary_inc"] = cached
        sys.modules["summary_inc_v2_unbounded"] = cached
        sys.modules["summary_inc_v4"] = cached
        return cached

    v4_path = resolve_pipeline_path(repo_root(), default_script="summary_inc_v4.1.py")
    spec = importlib.util.spec_from_file_location(cache_name, v4_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {v4_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[cache_name] = module
    sys.modules["summary_inc"] = module
    sys.modules["summary_inc_v2_unbounded"] = module
    sys.modules["summary_inc_v4"] = module
    spec.loader.exec_module(module)
    return module


def load_python_module(module_path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def pad_history_array(values: Iterable[Any] | None, length: int = LATEST_HISTORY_LEN) -> List[Any]:
    arr = list(values or [])
    if len(arr) >= length:
        return arr[:length]
    return arr + [None] * (length - len(arr))


def pad_latest_rows(rows: List[Dict[str, Any]], latest_len: int = LATEST_HISTORY_LEN) -> List[Dict[str, Any]]:
    padded: List[Dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        for col_name in HISTORY_COLS:
            if col_name in new_row:
                new_row[col_name] = pad_history_array(new_row[col_name], latest_len)
        padded.append(new_row)
    return padded


def write_summary_rows_v4(tu, spark, table: str, rows: List[Dict[str, Any]], latest_len: int = LATEST_HISTORY_LEN) -> None:
    if table.endswith(".latest_summary"):
        tu.write_summary_rows(spark, table, pad_latest_rows(rows, latest_len))
    else:
        tu.write_summary_rows(spark, table, rows)


def assert_latest_matches_summary_v4(
    spark,
    summary_table: str,
    latest_table: str,
    summary_len: int = SUMMARY_HISTORY_LEN,
    latest_len: int = LATEST_HISTORY_LEN,
) -> None:
    summary_df = spark.table(summary_table)
    latest_df = spark.table(latest_table)

    expected_df = (
        summary_df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("cons_acct_key").orderBy(F.col("rpt_as_of_mo").desc(), F.col("base_ts").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    expected_rows = {
        int(row["cons_acct_key"]): row.asDict(recursive=True)
        for row in expected_df.collect()
    }
    latest_rows = {
        int(row["cons_acct_key"]): row.asDict(recursive=True)
        for row in latest_df.collect()
    }

    missing_keys = sorted(set(expected_rows) - set(latest_rows))
    extra_keys = sorted(set(latest_rows) - set(expected_rows))
    mismatches = []

    common_keys = sorted(set(expected_rows) & set(latest_rows))
    for acct in common_keys:
        summary_row = expected_rows[acct]
        latest_row = latest_rows[acct]
        row_mismatches = []

        for col_name, summary_val in summary_row.items():
            latest_val = latest_row.get(col_name)
            if col_name in HISTORY_COLS:
                summary_hist = list(summary_val or [])
                latest_hist = list(latest_val or [])
                if len(summary_hist) != summary_len:
                    row_mismatches.append(f"{col_name}:summary_len={len(summary_hist)}")
                    continue
                if len(latest_hist) != latest_len:
                    row_mismatches.append(f"{col_name}:latest_len={len(latest_hist)}")
                    continue
                if summary_hist != latest_hist[:summary_len]:
                    row_mismatches.append(f"{col_name}:prefix")
            else:
                if latest_val != summary_val:
                    row_mismatches.append(col_name)

        if row_mismatches:
            mismatches.append(
                {
                    "cons_acct_key": acct,
                    "rpt_as_of_mo": summary_row.get("rpt_as_of_mo"),
                    "base_ts": summary_row.get("base_ts"),
                    "cols": row_mismatches[:8],
                }
            )

    if missing_keys or extra_keys or mismatches:
        raise AssertionError(
            "latest_summary is inconsistent with summary latest rows under V4 contract "
            f"(missing_keys={missing_keys[:5]}, extra_keys={extra_keys[:5]}, mismatches={mismatches[:5]})"
        )
