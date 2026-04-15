"""
Multi-delete validation for backfill_soft_delete_from_deletion_flagger.py

Covers:
1) Same account, continuous deletes
2) Same account, non-continuous/random deletes
3) Multi-delete where latest month is also deleted (latest reconstruction)
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


def _seed_account_rows(cons_acct_key: int, balance_base: int, pay_base: int, history_len: int = 36):
    rows = []
    months = [f"2026-{m:02d}" for m in range(1, 7)]  # 2026-01..2026-06
    for i, month in enumerate(months):
        ts = datetime(2026, i + 1, 2, 0, 0, 0)
        bal_cur = balance_base + (i * 100)
        pay_cur = pay_base + i
        rating_cur = str((i + 1) % 7)

        bal_positions = {pos: balance_base + ((i - pos) * 100) for pos in range(i + 1)}
        pay_positions = {pos: pay_base + (i - pos) for pos in range(i + 1)}
        rating_positions = {pos: str(((i - pos) + 1) % 7) for pos in range(i + 1)}
        if history_len > 36:
            bal_positions[36] = balance_base - 999
            pay_positions[36] = pay_base - 99
            rating_positions[36] = "Z"

        bal_hist = history(bal_positions, length=history_len)
        pay_hist = history(pay_positions, length=history_len)
        rating_hist = history(rating_positions, length=history_len)

        rows.append(
            build_summary_row(
                cons_acct_key,
                month,
                ts,
                balance=bal_cur,
                actual_payment=pay_cur,
                payment_rating=rating_cur,
                balance_history=bal_hist,
                payment_history=pay_hist,
                rating_history=rating_hist,
            )
        )
    return rows


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


def _write_deletion_rows(spark, table_name: str):
    rows = [
        # 4001: continuous deletes
        (4001, "2026-02", date(2026, 2, 1), "4"),
        (4001, "2026-03", date(2026, 3, 1), "4"),
        # 4002: non-continuous/random deletes
        (4002, "2026-01", date(2026, 1, 1), "4"),
        (4002, "2026-04", date(2026, 4, 1), "4"),
        # 4003: latest month included in multi-delete
        (4003, "2026-05", date(2026, 5, 1), "4"),
        (4003, "2026-06", date(2026, 6, 1), "4"),
    ]
    df = spark.createDataFrame(
        rows,
        "cons_acct_key BIGINT, rpt_as_of_mo STRING, acct_dt DATE, soft_del_cd STRING",
    )
    df.writeTo(table_name).append()


def _print_before_after(spark, summary_table: str, latest_table: str, stage: str):
    print(f"\n===== {stage} :: SUMMARY (selected) =====")
    (
        spark.table(summary_table)
        .filter(F.col("cons_acct_key").isin(4001, 4002, 4003))
        .filter(F.col("rpt_as_of_mo").isin("2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"))
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

    print(f"\n===== {stage} :: LATEST_SUMMARY =====")
    (
        spark.table(latest_table)
        .filter(F.col("cons_acct_key").isin(4001, 4002, 4003))
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


def run_test():
    spark = create_spark_session("test_backfill_soft_delete_from_deletion_flagger_multidelete")
    config = load_main_test_config("main_backfill_soft_delete_from_deletion_flagger_multidelete")
    try:
        reset_tables(spark, config)

        deletion_flagger_table = (
            "primary_catalog.main_backfill_soft_delete_from_deletion_flagger_multidelete.deletion_flagger"
        )
        _create_deletion_flagger_table(spark, deletion_flagger_table)

        rows_4001 = _seed_account_rows(4001, balance_base=1000, pay_base=100, history_len=36)
        rows_4002 = _seed_account_rows(4002, balance_base=2000, pay_base=200, history_len=36)
        rows_4003 = _seed_account_rows(4003, balance_base=3000, pay_base=300, history_len=36)
        latest_rows_4001 = _seed_account_rows(4001, balance_base=1000, pay_base=100, history_len=72)
        latest_rows_4002 = _seed_account_rows(4002, balance_base=2000, pay_base=200, history_len=72)
        latest_rows_4003 = _seed_account_rows(4003, balance_base=3000, pay_base=300, history_len=72)

        all_summary_rows = rows_4001 + rows_4002 + rows_4003
        write_summary_rows(spark, config["destination_table"], all_summary_rows)

        latest_rows = [
            next(r for r in latest_rows_4001 if r["rpt_as_of_mo"] == "2026-06"),
            next(r for r in latest_rows_4002 if r["rpt_as_of_mo"] == "2026-06"),
            next(r for r in latest_rows_4003 if r["rpt_as_of_mo"] == "2026-06"),
        ]
        write_summary_rows(spark, config["latest_history_table"], latest_rows)
        _write_deletion_rows(spark, deletion_flagger_table)

        _print_before_after(spark, config["destination_table"], config["latest_history_table"], "BEFORE")

        # capture base_ts to confirm no direct base_ts mutation by script
        base_before_s_4001_202606 = fetch_single_row(spark, config["destination_table"], 4001, "2026-06")["base_ts"]
        base_before_l_4001_202606 = fetch_single_row(spark, config["latest_history_table"], 4001, "2026-06")["base_ts"]

        # run job functions directly on isolated test namespace
        job.DELETION_FLAGGER_TABLE = deletion_flagger_table
        job.SUMMARY_TABLE = config["destination_table"]
        job.LATEST_SUMMARY_TABLE = config["latest_history_table"]

        job.ensure_target_columns(spark)
        case_month_df, case_future_df, history_cols, grid_specs = job.build_case_inputs(spark)
        if case_month_df is None and case_future_df is None:
            raise AssertionError("Expected generated updates for multi-delete scenario")
        job.merge_summary_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)
        job.merge_latest_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)

        _print_before_after(spark, config["destination_table"], config["latest_history_table"], "AFTER")

        # 4001 continuous delete: 2026-02 and 2026-03 -> in 2026-06 row, indexes 4 and 3 must be NULL
        s_4001_202606 = fetch_single_row(spark, config["destination_table"], 4001, "2026-06")
        _assert_eq(s_4001_202606["balance_am_history"][3], None, "4001 continuous delete index-3 null")
        _assert_eq(s_4001_202606["balance_am_history"][4], None, "4001 continuous delete index-4 null")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 4001, "2026-02")["soft_del_cd"], "4", "4001 2026-02 flagged")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 4001, "2026-03")["soft_del_cd"], "4", "4001 2026-03 flagged")

        # 4002 non-continuous delete: 2026-01 and 2026-04 -> in 2026-06 row, indexes 5 and 2 must be NULL
        s_4002_202606 = fetch_single_row(spark, config["destination_table"], 4002, "2026-06")
        _assert_eq(s_4002_202606["balance_am_history"][2], None, "4002 random delete index-2 null")
        _assert_eq(s_4002_202606["balance_am_history"][5], None, "4002 random delete index-5 null")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 4002, "2026-01")["soft_del_cd"], "4", "4002 2026-01 flagged")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 4002, "2026-04")["soft_del_cd"], "4", "4002 2026-04 flagged")

        # 4003 latest included in multi-delete: latest_summary should reconstruct to 2026-04
        latest_4003 = (
            spark.table(config["latest_history_table"])
            .filter(F.col("cons_acct_key") == 4003)
            .collect()
        )
        if len(latest_4003) != 1:
            raise AssertionError(f"Expected exactly 1 latest row for 4003, got {len(latest_4003)}")
        _assert_eq(latest_4003[0]["rpt_as_of_mo"], "2026-04", "4003 reconstructed latest month")

        # base_ts unchanged for rows that remained same month targets
        _assert_eq(
            fetch_single_row(spark, config["destination_table"], 4001, "2026-06")["base_ts"],
            base_before_s_4001_202606,
            "summary base_ts unchanged (4001, 2026-06)",
        )
        _assert_eq(
            fetch_single_row(spark, config["latest_history_table"], 4001, "2026-06")["base_ts"],
            base_before_l_4001_202606,
            "latest base_ts unchanged (4001, 2026-06)",
        )

        # Contract check: latest_summary keeps 72-length arrays and preserves tail (index 36).
        latest_4001_post = fetch_single_row(spark, config["latest_history_table"], 4001, "2026-06")
        _assert_eq(len(latest_4001_post["balance_am_history"]), 72, "latest 4001 balance history length")
        _assert_eq(len(latest_4001_post["actual_payment_am_history"]), 72, "latest 4001 payment history length")
        _assert_eq(len(latest_4001_post["payment_rating_cd_history"]), 72, "latest 4001 rating history length")
        _assert_eq(latest_4001_post["balance_am_history"][36], 1, "latest 4001 balance tail preserved")
        _assert_eq(latest_4001_post["actual_payment_am_history"][36], 1, "latest 4001 payment tail preserved")
        _assert_eq(latest_4001_post["payment_rating_cd_history"][36], "Z", "latest 4001 rating tail preserved")

        print("\n[PASS] test_backfill_soft_delete_from_deletion_flagger_multidelete")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
