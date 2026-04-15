"""
Focused test for backfill_soft_delete_from_deletion_flagger.py
- Verifies soft_del_cd updates
- Verifies future array/grid nullification
- Verifies latest_summary reconstruction when latest month is deleted
- Verifies base_ts is unchanged by this utility
"""

from datetime import datetime, date
import os
import sys

from pyspark.sql import functions as F

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_deletion_flagger as job
from test_utils import (
    build_summary_row,
    create_spark_session,
    fetch_single_row,
    history,
    load_main_test_config,
    reset_tables,
    write_summary_rows,
)


def _assert_eq(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def _print_snapshot(spark, summary_table: str, latest_table: str, title: str):
    print(f"\n===== {title} :: SUMMARY =====")
    (
        spark.table(summary_table)
        .filter(F.col("cons_acct_key").isin(3001, 3002))
        .orderBy("cons_acct_key", "rpt_as_of_mo")
        .select(
            "cons_acct_key",
            "rpt_as_of_mo",
            "soft_del_cd",
            "base_ts",
            "balance_am_history",
            "payment_history_grid",
        )
        .show(truncate=False)
    )

    print(f"\n===== {title} :: LATEST_SUMMARY =====")
    (
        spark.table(latest_table)
        .filter(F.col("cons_acct_key").isin(3001, 3002))
        .orderBy("cons_acct_key", "rpt_as_of_mo")
        .select(
            "cons_acct_key",
            "rpt_as_of_mo",
            "soft_del_cd",
            "base_ts",
            "balance_am_history",
            "payment_history_grid",
        )
        .show(truncate=False)
    )


def _create_deletion_flagger_table(spark, table_name: str):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            acct_dt DATE,
            soft_del_cd STRING
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
        """
    )


def _write_deletion_flagger_rows(spark, table_name: str):
    rows = [
        (3001, "2026-01", date(2026, 1, 1), "4"),  # past-month delete -> future patch expected
        (3002, "2026-03", date(2026, 3, 1), "4"),  # latest-month delete -> latest reconstruction expected
    ]
    df = spark.createDataFrame(rows, "cons_acct_key BIGINT, rpt_as_of_mo STRING, acct_dt DATE, soft_del_cd STRING")
    df.writeTo(table_name).append()


def run_test():
    spark = create_spark_session("test_backfill_soft_delete_from_deletion_flagger")
    config = load_main_test_config("main_backfill_soft_delete_from_deletion_flagger")
    try:
        reset_tables(spark, config)

        deletion_flagger_table = f"primary_catalog.main_backfill_soft_delete_from_deletion_flagger.deletion_flagger"
        _create_deletion_flagger_table(spark, deletion_flagger_table)

        # Seed summary + latest_summary
        ts_1 = datetime(2026, 1, 2, 0, 0, 0)
        ts_2 = datetime(2026, 2, 2, 0, 0, 0)
        ts_3 = datetime(2026, 3, 2, 0, 0, 0)

        summary_rows = [
            # Account 3001: delete month=2026-01; should null pos1 in 2026-02 and pos2 in 2026-03
            build_summary_row(3001, "2026-01", ts_1, balance=1100, actual_payment=100, payment_rating="1"),
            build_summary_row(
                3001,
                "2026-02",
                ts_2,
                balance=1200,
                actual_payment=200,
                payment_rating="2",
                balance_history=history({0: 1200, 1: 1100}),
                payment_history=history({0: 200, 1: 100}),
                rating_history=history({0: "2", 1: "1"}),
            ),
            build_summary_row(
                3001,
                "2026-03",
                ts_3,
                balance=1300,
                actual_payment=300,
                payment_rating="3",
                balance_history=history({0: 1300, 1: 1200, 2: 1100}),
                payment_history=history({0: 300, 1: 200, 2: 100}),
                rating_history=history({0: "3", 1: "2", 2: "1"}),
            ),
            # Account 3002: delete latest month=2026-03; latest should reconstruct to 2026-02
            build_summary_row(3002, "2026-02", ts_2, balance=2200, actual_payment=220, payment_rating="2"),
            build_summary_row(
                3002,
                "2026-03",
                ts_3,
                balance=2300,
                actual_payment=230,
                payment_rating="3",
                balance_history=history({0: 2300, 1: 2200}),
                payment_history=history({0: 230, 1: 220}),
                rating_history=history({0: "3", 1: "2"}),
            ),
        ]

        latest_rows = [
            build_summary_row(
                3001,
                "2026-03",
                ts_3,
                balance=1300,
                actual_payment=300,
                payment_rating="3",
                balance_history=history({0: 1300, 1: 1200, 2: 1100}),
                payment_history=history({0: 300, 1: 200, 2: 100}),
                rating_history=history({0: "3", 1: "2", 2: "1"}),
            ),
            build_summary_row(
                3002,
                "2026-03",
                ts_3,
                balance=2300,
                actual_payment=230,
                payment_rating="3",
                balance_history=history({0: 2300, 1: 2200}),
                payment_history=history({0: 230, 1: 220}),
                rating_history=history({0: "3", 1: "2"}),
            ),
        ]

        write_summary_rows(spark, config["destination_table"], summary_rows)
        write_summary_rows(spark, config["latest_history_table"], latest_rows)
        _write_deletion_flagger_rows(spark, deletion_flagger_table)

        # Before snapshot
        _print_snapshot(spark, config["destination_table"], config["latest_history_table"], "BEFORE")

        # Track base_ts before (must remain unchanged after run)
        before_s_3001_2026_02 = fetch_single_row(spark, config["destination_table"], 3001, "2026-02")["base_ts"]
        before_s_3001_2026_03 = fetch_single_row(spark, config["destination_table"], 3001, "2026-03")["base_ts"]
        before_l_3001_2026_03 = fetch_single_row(spark, config["latest_history_table"], 3001, "2026-03")["base_ts"]

        # Point script to this isolated test namespace and run
        job.DELETION_FLAGGER_TABLE = deletion_flagger_table
        job.SUMMARY_TABLE = config["destination_table"]
        job.LATEST_SUMMARY_TABLE = config["latest_history_table"]

        job.ensure_target_columns(spark)
        case_month_df, case_future_df, history_cols, grid_specs = job.build_case_inputs(spark)
        if case_month_df is None and case_future_df is None:
            raise AssertionError("Expected deletion updates but none were generated")
        job.merge_summary_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)
        job.merge_latest_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)

        # After snapshot
        _print_snapshot(spark, config["destination_table"], config["latest_history_table"], "AFTER")

        # Assertions
        s_3001_2026_01 = fetch_single_row(spark, config["destination_table"], 3001, "2026-01")
        s_3001_2026_02 = fetch_single_row(spark, config["destination_table"], 3001, "2026-02")
        s_3001_2026_03 = fetch_single_row(spark, config["destination_table"], 3001, "2026-03")
        l_3001_2026_03 = fetch_single_row(spark, config["latest_history_table"], 3001, "2026-03")

        _assert_eq(s_3001_2026_01["soft_del_cd"], "4", "Deleted month soft_del flag")
        _assert_eq(s_3001_2026_02["balance_am_history"][1], None, "Future month index-1 null patch in summary")
        _assert_eq(s_3001_2026_03["balance_am_history"][2], None, "Future month index-2 null patch in summary")
        _assert_eq(l_3001_2026_03["balance_am_history"][2], None, "Future month index-2 null patch in latest")

        # latest month delete for 3002 should reconstruct to 2026-02
        l_3002 = (
            spark.table(config["latest_history_table"])
            .filter(F.col("cons_acct_key") == 3002)
            .collect()
        )
        if len(l_3002) != 1:
            raise AssertionError(f"Expected one latest row for 3002, got {len(l_3002)}")
        _assert_eq(l_3002[0]["rpt_as_of_mo"], "2026-02", "Latest reconstruction month for 3002")

        # base_ts must not change
        _assert_eq(fetch_single_row(spark, config["destination_table"], 3001, "2026-02")["base_ts"], before_s_3001_2026_02, "summary base_ts unchanged (3001,2026-02)")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 3001, "2026-03")["base_ts"], before_s_3001_2026_03, "summary base_ts unchanged (3001,2026-03)")
        _assert_eq(fetch_single_row(spark, config["latest_history_table"], 3001, "2026-03")["base_ts"], before_l_3001_2026_03, "latest base_ts unchanged (3001,2026-03)")

        print("\n[PASS] test_backfill_soft_delete_from_deletion_flagger")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

