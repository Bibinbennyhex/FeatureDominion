"""
Audit-style integration test for main/backfill_soft_delete_from_accounts.py.

Outputs a per-run folder with CSV snapshots for review:
- accounts_all_base.csv
- hist_rpt_dt_base.csv
- summary_before.csv
- latest_summary_before.csv
- summary_after.csv
- latest_summary_after.csv
"""

from datetime import date, datetime, timezone
import csv
import os
import sys

from pyspark.sql import functions as F

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_accounts as backfill_job
from test_utils import (
    build_source_row,
    build_summary_row,
    create_spark_session,
    fetch_single_row,
    history,
    load_main_test_config,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


LOG_ROOT = os.path.join(
    os.path.dirname(__file__),
    "artifacts",
    "audits",
    "backfill_soft_delete_audit_logs",
)


def _normalize_cell(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, list):
        return str(v)
    return "" if v is None else str(v)


def _write_df_csv(df, output_path: str, order_cols):
    rows = [r.asDict(recursive=True) for r in df.select(*order_cols).collect()]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=order_cols)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _normalize_cell(row.get(k)) for k in order_cols})


def _write_hist_rows(spark, table: str, rows):
    if not rows:
        return
    df = spark.createDataFrame(
        rows,
        schema=(
            "cons_acct_key BIGINT, "
            "soft_del_cd STRING, "
            "acct_dt DATE, "
            "base_ts TIMESTAMP, "
            "insert_dt TIMESTAMP, "
            "update_dt TIMESTAMP, "
            "insert_time STRING, "
            "update_time STRING"
        ),
    )
    df.writeTo(table).append()


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def run_test():
    spark = create_spark_session("main_backfill_soft_delete_audit_export_test")
    config = load_main_test_config("main_backfill_soft_delete_audit_export")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_dir = os.path.join(LOG_ROOT, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    try:
        print(f"[AUDIT] Output folder: {run_dir}")
        reset_tables(spark, config)

        # Point standalone script constants to isolated test namespace.
        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)

        seed_9101 = [
            build_summary_row(
                cons_acct_key=9101,
                rpt_as_of_mo="2025-12",
                base_ts=seed_ts,
                balance=5000,
                actual_payment=500,
                balance_history=history({0: 5000}),
                payment_history=history({0: 500}),
                credit_history=history({0: 10000}),
                past_due_history=history({0: 0}),
                rating_history=history({0: "0"}),
                dpd_history=history({0: 0}),
                asset_history=history({0: "A"}),
            ),
            build_summary_row(
                cons_acct_key=9101,
                rpt_as_of_mo="2026-01",
                base_ts=seed_ts,
                balance=5100,
                actual_payment=510,
                balance_history=history({0: 5100, 1: 5000}),
                payment_history=history({0: 510, 1: 500}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
            build_summary_row(
                cons_acct_key=9101,
                rpt_as_of_mo="2026-02",
                base_ts=seed_ts,
                balance=5200,
                actual_payment=520,
                balance_history=history({0: 5200, 1: 5100, 2: 5000}),
                payment_history=history({0: 520, 1: 510, 2: 500}),
                credit_history=history({0: 10000, 1: 10000, 2: 10000}),
                past_due_history=history({0: 0, 1: 0, 2: 0}),
                rating_history=history({0: "0", 1: "0", 2: "0"}),
                dpd_history=history({0: 0, 1: 0, 2: 0}),
                asset_history=history({0: "A", 1: "A", 2: "A"}),
            ),
        ]

        seed_9102 = [
            build_summary_row(
                cons_acct_key=9102,
                rpt_as_of_mo="2026-01",
                base_ts=seed_ts,
                balance=1000,
                actual_payment=100,
                balance_history=history({0: 1000}),
                payment_history=history({0: 100}),
                credit_history=history({0: 5000}),
                past_due_history=history({0: 0}),
                rating_history=history({0: "0"}),
                dpd_history=history({0: 0}),
                asset_history=history({0: "A"}),
            ),
            build_summary_row(
                cons_acct_key=9102,
                rpt_as_of_mo="2026-02",
                base_ts=seed_ts,
                balance=1100,
                actual_payment=110,
                balance_history=history({0: 1100, 1: 1000}),
                payment_history=history({0: 110, 1: 100}),
                credit_history=history({0: 5000, 1: 5000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
        ]

        all_summary_rows = seed_9101 + seed_9102
        write_summary_rows(spark, config["destination_table"], all_summary_rows)
        latest_rows = [
            [r for r in seed_9101 if r["rpt_as_of_mo"] == "2026-02"][0],
            [r for r in seed_9102 if r["rpt_as_of_mo"] == "2026-02"][0],
        ]
        write_summary_rows(spark, config["latest_history_table"], latest_rows)

        source_rows = [
            build_source_row(9101, "2025-12", datetime(2026, 2, 15, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            build_source_row(9101, "2026-01", datetime(2026, 2, 15, 11, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            build_source_row(9101, "2026-01", datetime(2026, 2, 15, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            build_source_row(9102, "2026-02", datetime(2026, 2, 16, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        hist_rows = [
            # Same keys as summary/accounts to validate hist merge is effective.
            {
                "cons_acct_key": 9101,
                "soft_del_cd": "",
                "acct_dt": date(2025, 12, 20),
                "base_ts": datetime(2026, 2, 17, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 17, 0, 0, 0),
                "update_dt": datetime(2026, 2, 17, 0, 0, 0),
                "insert_time": "10:00:00",
                "update_time": "10:30:00",
            },
            {
                "cons_acct_key": 9101,
                "soft_del_cd": "",
                "acct_dt": date(2026, 1, 20),
                "base_ts": datetime(2026, 2, 17, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 17, 0, 0, 0),
                "update_dt": datetime(2026, 2, 17, 0, 0, 0),
                "insert_time": "10:00:00",
                "update_time": "10:45:00",
            },
            # '1' is now treated as delete as well in acct_dt/soft_del resolution.
            {
                "cons_acct_key": 9102,
                "soft_del_cd": "1",
                "acct_dt": date(2026, 2, 25),
                "base_ts": datetime(2026, 2, 17, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 17, 0, 0, 0),
                "update_dt": datetime(2026, 2, 17, 0, 0, 0),
                "insert_time": "11:00:00",
                "update_time": "11:15:00",
            },
        ]
        _write_hist_rows(spark, config["hist_rpt_dt_table"], hist_rows)

        # Export base input and before snapshots.
        accounts_all_df = spark.table(config["source_table"]).orderBy("cons_acct_key", "rpt_as_of_mo", "base_ts")
        hist_base_df = spark.table(config["hist_rpt_dt_table"]).orderBy("cons_acct_key", "acct_dt", "base_ts")
        summary_before_df = spark.table(config["destination_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")
        latest_before_df = spark.table(config["latest_history_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")

        _write_df_csv(accounts_all_df, os.path.join(run_dir, "accounts_all_base.csv"), accounts_all_df.columns)
        _write_df_csv(hist_base_df, os.path.join(run_dir, "hist_rpt_dt_base.csv"), hist_base_df.columns)
        _write_df_csv(summary_before_df, os.path.join(run_dir, "summary_before.csv"), summary_before_df.columns)
        _write_df_csv(latest_before_df, os.path.join(run_dir, "latest_summary_before.csv"), latest_before_df.columns)

        print("[AUDIT] accounts_all (base):")
        accounts_all_df.show(200, truncate=False)
        print("[AUDIT] hist_rpt_dt (base):")
        hist_base_df.show(200, truncate=False)
        print("[AUDIT] summary (before):")
        summary_before_df.show(200, truncate=False)
        print("[AUDIT] latest_summary (before):")
        latest_before_df.show(200, truncate=False)

        # Execute standalone backfill flow.
        pre_backfill_changed = backfill_job._backfill_acct_dt_and_soft_delete_from_hist_rpt(spark)
        _assert_equal(pre_backfill_changed, True, "Expected hist_rpt pre-backfill to apply changes")

        # Validate hist-rpt effect before delete-array backfill stage.
        pre_s_9101_2025_12 = fetch_single_row(spark, config["destination_table"], 9101, "2025-12")
        pre_s_9101_2026_01 = fetch_single_row(spark, config["destination_table"], 9101, "2026-01")
        pre_s_9102_2026_02 = fetch_single_row(spark, config["destination_table"], 9102, "2026-02")
        pre_l_9102_2026_02 = fetch_single_row(spark, config["latest_history_table"], 9102, "2026-02")

        _assert_equal(pre_s_9101_2025_12["acct_dt"], date(2025, 12, 20), "Hist should update summary acct_dt for 9101/2025-12")
        _assert_equal(pre_s_9101_2025_12["soft_del_cd"], "", "Hist should keep 9101/2025-12 active pre-delete-stage")
        _assert_equal(pre_s_9101_2026_01["acct_dt"], date(2026, 1, 20), "Hist should update summary acct_dt for 9101/2026-01")
        _assert_equal(pre_s_9101_2026_01["soft_del_cd"], "", "Hist should keep 9101/2026-01 active pre-delete-stage")
        _assert_equal(pre_s_9102_2026_02["acct_dt"], None, "Both-delete logic (source=4, hist=1) should null acct_dt")
        _assert_equal(pre_s_9102_2026_02["soft_del_cd"], "4", "Both-delete logic should retain source delete code in summary")
        _assert_equal(pre_l_9102_2026_02["acct_dt"], None, "Both-delete logic should apply to latest_summary month row")
        _assert_equal(pre_l_9102_2026_02["soft_del_cd"], "4", "Both-delete logic should retain source delete code in latest_summary")

        print("[AUDIT] summary (after hist_rpt pre-backfill, before delete-array stage):")
        spark.table(config["destination_table"]).orderBy("cons_acct_key", "rpt_as_of_mo").show(200, truncate=False)

        has_updates, history_cols, grid_specs = backfill_job._prepare_case_tables(spark)
        if has_updates:
            backfill_job._merge_case_tables_chunked(spark, history_cols, grid_specs)

        # Export after snapshots.
        summary_after_df = spark.table(config["destination_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")
        latest_after_df = spark.table(config["latest_history_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")

        _write_df_csv(summary_after_df, os.path.join(run_dir, "summary_after.csv"), summary_after_df.columns)
        _write_df_csv(latest_after_df, os.path.join(run_dir, "latest_summary_after.csv"), latest_after_df.columns)

        print("[AUDIT] summary (after):")
        summary_after_df.show(200, truncate=False)
        print("[AUDIT] latest_summary (after):")
        latest_after_df.show(200, truncate=False)

        # Basic correctness checks.
        s_9101_2025_12 = fetch_single_row(spark, config["destination_table"], 9101, "2025-12")
        s_9101_2026_01 = fetch_single_row(spark, config["destination_table"], 9101, "2026-01")
        s_9101_2026_02 = fetch_single_row(spark, config["destination_table"], 9101, "2026-02")
        l_9101_2026_02 = fetch_single_row(spark, config["latest_history_table"], 9101, "2026-02")
        l_9102_2026_01 = fetch_single_row(spark, config["latest_history_table"], 9102, "2026-01")

        _assert_equal(s_9101_2025_12["soft_del_cd"], "1", "Deleted month 2025-12 should be flagged")
        _assert_equal(s_9101_2026_01["soft_del_cd"], "1", "Deleted month 2026-01 should use latest delete winner")
        _assert_equal(s_9101_2026_02["balance_am_history"][1], None, "Future index 1 should be nullified")
        _assert_equal(s_9101_2026_02["balance_am_history"][2], None, "Future index 2 should be nullified")
        _assert_equal(l_9101_2026_02["balance_am_history"][1], None, "latest_summary index 1 should be nullified")
        _assert_equal(l_9101_2026_02["balance_am_history"][2], None, "latest_summary index 2 should be nullified")
        _assert_equal(l_9102_2026_01["rpt_as_of_mo"], "2026-01", "latest_summary should reconstruct to prior non-deleted month")

        print(f"[PASS] test_backfill_soft_delete_audit_export | audit_dir={run_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
