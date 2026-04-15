"""
Smoke test for backfill_soft_delete_from_accounts_v2 against original summary/latest schemas.
"""

from datetime import date, datetime
import os
import sys

from pyspark.sql import functions as F

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_accounts_v2 as backfill_job
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


def _assert_eq(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


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


def run_test():
    spark = create_spark_session("one_time_backfill_smoke_v2")
    config = load_main_test_config("main_one_time_backfill_smoke_v2")
    try:
        reset_tables(spark, config)

        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)
        summary_rows = [
            build_summary_row(1101, "2026-02", seed_ts, balance=11010, actual_payment=110, soft_del_cd=""),
            build_summary_row(1102, "2026-01", seed_ts, balance=21010, actual_payment=210, soft_del_cd=""),
            build_summary_row(
                1102,
                "2026-02",
                seed_ts,
                balance=21020,
                actual_payment=220,
                soft_del_cd="",
                balance_history=history({0: 21020, 1: 21010}),
                payment_history=history({0: 220, 1: 210}),
            ),
            build_summary_row(
                1102,
                "2026-03",
                seed_ts,
                balance=21030,
                actual_payment=230,
                soft_del_cd="",
                balance_history=history({0: 21030, 1: 21020, 2: 21010}),
                payment_history=history({0: 230, 1: 220, 2: 210}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], summary_rows)
        write_summary_rows(
            spark,
            config["latest_history_table"],
            [
                build_summary_row(1101, "2026-02", seed_ts, balance=11010, actual_payment=110, soft_del_cd=""),
                build_summary_row(
                    1102,
                    "2026-03",
                    seed_ts,
                    balance=21030,
                    actual_payment=230,
                    soft_del_cd="",
                    balance_history=history({0: 21030, 1: 21020, 2: 21010}),
                    payment_history=history({0: 230, 1: 220, 2: 210}),
                ),
            ],
        )

        src_1101 = build_source_row(1101, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=11111, actual_payment=111)
        src_1101["acct_dt"] = date(2026, 2, 10)
        src_1102 = build_source_row(1102, "2026-01", datetime(2026, 2, 16, 0, 0, 0), balance=1, actual_payment=1, soft_del_cd="4")
        write_source_rows(spark, config["source_table"], [src_1101, src_1102])

        _write_hist_rows(
            spark,
            config["hist_rpt_dt_table"],
            [
                {
                    "cons_acct_key": 1101,
                    "soft_del_cd": "",
                    "acct_dt": date(2026, 2, 18),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "10:00:00",
                    "update_time": "11:00:00",
                }
            ],
        )

        result = backfill_job.run_backfill_v2(spark)
        if not result["hist_updates"]:
            raise AssertionError("Expected hist/scalar updates to be applied")
        if not result["delete_updates"]:
            raise AssertionError("Expected delete updates to be applied")

        s1101 = fetch_single_row(spark, config["destination_table"], 1101, "2026-02")
        s1102_202601 = fetch_single_row(spark, config["destination_table"], 1102, "2026-01")
        s1102_202603 = fetch_single_row(spark, config["destination_table"], 1102, "2026-03")
        l1102_202603 = fetch_single_row(spark, config["latest_history_table"], 1102, "2026-03")

        _assert_eq(s1101["acct_dt"], date(2026, 2, 18), "acct_dt should resolve from hist")
        _assert_eq(s1102_202601["soft_del_cd"], "4", "deleted month should be flagged")
        _assert_eq(s1102_202603["balance_am_history"][2], None, "future summary array index for deleted month should nullify")
        _assert_eq(l1102_202603["balance_am_history"][2], None, "future latest array index for deleted month should nullify")

        sample = (
            spark.table(config["destination_table"])
            .filter(F.col("cons_acct_key").isin(1101, 1102))
            .orderBy("cons_acct_key", "rpt_as_of_mo")
            .select("cons_acct_key", "rpt_as_of_mo", "acct_dt", "soft_del_cd", "base_ts")
        )
        print("[SMOKE] Summary sample after run:")
        sample.show(truncate=False)

        print("[PASS] one_time_backfill_smoke_v2")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

