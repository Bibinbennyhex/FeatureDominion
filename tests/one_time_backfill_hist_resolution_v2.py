"""
Tie-break and precedence checks for acct_dt/soft_del resolution in backfill v2.
"""

from datetime import date, datetime
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_accounts_v2 as backfill_job
from pyspark.sql import functions as F
from test_utils import (
    build_source_row,
    build_summary_row,
    create_spark_session,
    load_main_test_config,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


def _assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected={expected}, actual={actual}")


def _write_hist_rows(spark, table, rows):
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


def _row(df, key, month):
    out = (
        df.filter((F.col("cons_acct_key") == key) & (F.col("rpt_as_of_mo") == month))
        .select("cons_acct_key", "rpt_as_of_mo", "acct_dt", "soft_del_cd")
        .collect()
    )
    if len(out) != 1:
        raise AssertionError(f"Expected one row for {key}/{month}, got {len(out)}")
    return out[0]


def run_test():
    spark = create_spark_session("one_time_backfill_hist_resolution_v2")
    config = load_main_test_config("main_one_time_backfill_hist_resolution_v2")
    try:
        reset_tables(spark, config)
        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_rows = [
            build_summary_row(1301, "2026-02", datetime(2026, 2, 1, 0, 0, 0), balance=13010, actual_payment=130),
            build_summary_row(1302, "2026-02", datetime(2026, 2, 1, 0, 0, 0), balance=13020, actual_payment=131),
        ]
        write_summary_rows(spark, config["destination_table"], seed_rows)
        write_summary_rows(spark, config["latest_history_table"], seed_rows)

        src_1301 = build_source_row(1301, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=13110, actual_payment=230, soft_del_cd="")
        src_1301["acct_dt"] = date(2026, 2, 10)
        src_1302 = build_source_row(1302, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=13120, actual_payment=231, soft_del_cd="4")
        src_1302["acct_dt"] = date(2026, 2, 11)
        write_source_rows(spark, config["source_table"], [src_1301, src_1302])

        _write_hist_rows(
            spark,
            config["hist_rpt_dt_table"],
            [
                # same base_ts, tie resolved by insert/update date+time order
                {
                    "cons_acct_key": 1301,
                    "soft_del_cd": "",
                    "acct_dt": date(2026, 2, 18),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "09:00:00",
                    "update_time": "09:00:00",
                },
                {
                    "cons_acct_key": 1301,
                    "soft_del_cd": "",
                    "acct_dt": date(2026, 2, 19),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "10:00:00",
                    "update_time": "10:00:00",
                },
                # both source and hist are deleted -> acct_dt should become NULL, soft_del from source retained
                {
                    "cons_acct_key": 1302,
                    "soft_del_cd": "4",
                    "acct_dt": date(2026, 2, 25),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "11:00:00",
                    "update_time": "11:00:00",
                },
            ],
        )

        backfill_job.run_backfill_v2(spark)

        s_df = spark.table(config["destination_table"])
        l_df = spark.table(config["latest_history_table"])

        s1301 = _row(s_df, 1301, "2026-02")
        s1302 = _row(s_df, 1302, "2026-02")
        l1301 = _row(l_df, 1301, "2026-02")
        l1302 = _row(l_df, 1302, "2026-02")

        _assert_eq(s1301["acct_dt"], date(2026, 2, 19), "summary 1301 acct_dt")
        _assert_eq(l1301["acct_dt"], date(2026, 2, 19), "latest 1301 acct_dt")
        _assert_eq(s1302["acct_dt"], None, "summary 1302 acct_dt for both-delete")
        _assert_eq(l1302["acct_dt"], None, "latest 1302 acct_dt for both-delete")
        _assert_eq(s1302["soft_del_cd"], "4", "summary 1302 soft_del")
        _assert_eq(l1302["soft_del_cd"], "4", "latest 1302 soft_del")

        print("[PASS] one_time_backfill_hist_resolution_v2")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

