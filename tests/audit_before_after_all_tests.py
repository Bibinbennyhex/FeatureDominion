"""
Run all tests and capture summary/latest_summary before/after snapshots
around each main_pipeline.run_pipeline invocation.
"""

import json
import os
import runpy
import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

import test_utils
import backfill_soft_delete_from_accounts as backfill_job
from tests.suite_manifest import CORE_TESTS


TEST_FILES = CORE_TESTS + ["test_aggressive_idempotency.py"]


CURRENT_TEST = {"name": None}
PIPELINE_CALLS = []
BACKFILL_CALLS = []
TEST_RESULTS = []


def _snapshot_table(spark, table_name: str):
    if not spark.catalog.tableExists(table_name):
        return {
            "exists": False,
            "rows": 0,
            "accounts": 0,
            "max_month": None,
            "sample": None,
        }

    df = spark.table(table_name)
    rows = df.count()
    accounts = df.select("cons_acct_key").distinct().count() if "cons_acct_key" in df.columns else None
    max_month = (
        df.agg(F.max("rpt_as_of_mo").alias("max_month")).first()["max_month"]
        if "rpt_as_of_mo" in df.columns
        else None
    )

    sample = None
    if rows > 0 and "cons_acct_key" in df.columns:
        sample_cols = [c for c in [
            "cons_acct_key",
            "rpt_as_of_mo",
            "balance_am_history",
            "payment_rating_cd_history",
        ] if c in df.columns]
        row = df.select(*sample_cols).orderBy("cons_acct_key", "rpt_as_of_mo").limit(1).collect()[0]
        sample = row.asDict()
        for arr_col in ["balance_am_history", "payment_rating_cd_history"]:
            if arr_col in sample and sample[arr_col] is not None:
                sample[arr_col] = sample[arr_col][:6]

    return {
        "exists": True,
        "rows": rows,
        "accounts": accounts,
        "max_month": max_month,
        "sample": sample,
    }


def _snapshot_pair(spark, config):
    return {
        "summary": _snapshot_table(spark, config["destination_table"]),
        "latest_summary": _snapshot_table(spark, config["latest_history_table"]),
        "destination_table": config["destination_table"],
        "latest_table": config["latest_history_table"],
    }


ORIG_RUN_PIPELINE = test_utils.main_pipeline.run_pipeline
ORIG_BACKFILL_MERGE = backfill_job._merge_case_tables_chunked


def _wrapped_run_pipeline(spark, config, *args, **kwargs):
    test_name = CURRENT_TEST["name"] or "UNKNOWN_TEST"
    before = _snapshot_pair(spark, config)
    err = None
    started = datetime.now(timezone.utc).isoformat()
    try:
        return ORIG_RUN_PIPELINE(spark, config, *args, **kwargs)
    except Exception as exc:
        err = str(exc).split("\n")[0]
        raise
    finally:
        after = _snapshot_pair(spark, config)
        PIPELINE_CALLS.append(
            {
                "test": test_name,
                "call_type": "pipeline",
                "started_utc": started,
                "before": before,
                "after": after,
                "error": err,
            }
        )


def _wrapped_backfill_merge_case_tables_chunked(spark, history_cols, grid_specs, *args, **kwargs):
    test_name = CURRENT_TEST["name"] or "UNKNOWN_TEST"
    cfg = {
        "destination_table": backfill_job.SUMMARY_TABLE,
        "latest_history_table": backfill_job.LATEST_SUMMARY_TABLE,
    }
    before = _snapshot_pair(spark, cfg)
    err = None
    started = datetime.now(timezone.utc).isoformat()
    try:
        return ORIG_BACKFILL_MERGE(spark, history_cols, grid_specs, *args, **kwargs)
    except Exception as exc:
        err = str(exc).split("\n")[0]
        raise
    finally:
        after = _snapshot_pair(spark, cfg)
        BACKFILL_CALLS.append(
            {
                "test": test_name,
                "call_type": "standalone_backfill_soft_delete",
                "started_utc": started,
                "before": before,
                "after": after,
                "error": err,
            }
        )


def _aggregate_test_result(test_file: str):
    calls = [c for c in PIPELINE_CALLS if c["test"] == test_file]
    backfill_calls = [c for c in BACKFILL_CALLS if c["test"] == test_file]
    all_calls = calls + backfill_calls

    if not all_calls:
        return {
            "test": test_file,
            "pipeline_calls": 0,
            "backfill_calls": 0,
            "summary_rows_before": None,
            "summary_rows_after": None,
            "latest_rows_before": None,
            "latest_rows_after": None,
            "summary_max_month_after": None,
            "latest_max_month_after": None,
            "call_errors": 0,
        }

    first = all_calls[0]
    last = all_calls[-1]
    return {
        "test": test_file,
        "pipeline_calls": len(calls),
        "backfill_calls": len(backfill_calls),
        "summary_rows_before": first["before"]["summary"]["rows"],
        "summary_rows_after": last["after"]["summary"]["rows"],
        "latest_rows_before": first["before"]["latest_summary"]["rows"],
        "latest_rows_after": last["after"]["latest_summary"]["rows"],
        "summary_max_month_after": last["after"]["summary"]["max_month"],
        "latest_max_month_after": last["after"]["latest_summary"]["max_month"],
        "call_errors": sum(1 for c in all_calls if c["error"]),
    }


def main():
    tests_dir = Path(__file__).resolve().parent
    os.chdir(str(tests_dir))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir_env = os.environ.get("AUDIT_OUTPUT_DIR", "").strip()
    output_dir = Path(output_dir_env) if output_dir_env else tests_dir / "artifacts" / "audits" / f"run_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    test_utils.main_pipeline.run_pipeline = _wrapped_run_pipeline
    backfill_job._merge_case_tables_chunked = _wrapped_backfill_merge_case_tables_chunked
    try:
        for test_file in TEST_FILES:
            CURRENT_TEST["name"] = test_file
            path = str(tests_dir / test_file)
            status = "PASS"
            error = ""
            try:
                runpy.run_path(path, run_name="__main__")
            except Exception as exc:
                status = "FAIL"
                error = str(exc).split("\n")[0]
                traceback.print_exc()

            agg = _aggregate_test_result(test_file)
            agg["status"] = status
            agg["error"] = error
            TEST_RESULTS.append(agg)
            print(f"[AUDIT] {test_file} -> {status}")
    finally:
        test_utils.main_pipeline.run_pipeline = ORIG_RUN_PIPELINE
        backfill_job._merge_case_tables_chunked = ORIG_BACKFILL_MERGE

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "results": TEST_RESULTS,
        "calls": PIPELINE_CALLS,
        "backfill_calls": BACKFILL_CALLS,
    }
    out_json = output_dir / "audit_before_after_results.json"
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    latest_pointer = tests_dir / "_audit_before_after_results.json"
    latest_pointer.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    print("\n=== AUDIT SUMMARY ===")
    print("test | status | calls(pipeline/backfill) | summary_before->after | latest_before->after | call_errors")
    for r in TEST_RESULTS:
        print(
            f"{r['test']} | {r['status']} | {r['pipeline_calls']}/{r['backfill_calls']} | "
            f"{r['summary_rows_before']}->{r['summary_rows_after']} | "
            f"{r['latest_rows_before']}->{r['latest_rows_after']} | "
            f"{r['call_errors']}"
        )
    print(f"\nSaved: {out_json}")


if __name__ == "__main__":
    main()
