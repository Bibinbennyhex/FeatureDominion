"""
Idempotency regression test for main/summary_inc.py.

Verifies that running the same batch twice does not change outputs.
"""

import json
from datetime import datetime

from pyspark.sql import functions as F

from test_utils import (
    assert_watermark_tracker_consistent,
    build_source_row,
    build_summary_row,
    create_spark_session,
    history,
    load_main_test_config,
    main_pipeline,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def _table_snapshot(spark, table_name):
    rows = (
        spark.table(table_name)
        .orderBy(F.col("cons_acct_key").asc(), F.col("rpt_as_of_mo").asc(), F.col("base_ts").asc())
        .collect()
    )
    as_dicts = [r.asDict(recursive=True) for r in rows]
    return json.dumps(as_dicts, default=str, sort_keys=True)


def _assert_latest_unique_per_account(spark, latest_table):
    dup_count = (
        spark.table(latest_table)
        .groupBy("cons_acct_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    _assert_equal(dup_count, 0, "latest_summary should have max 1 row per account")


def run_test():
    spark = create_spark_session("main_summary_idempotency_test")
    config = load_main_test_config("main_idempotency")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        batch_ts = datetime(2026, 2, 1, 12, 0, 0)

        print("[SETUP] Seeding existing summary/latest data...")
        existing_rows = [
            build_summary_row(
                cons_acct_key=2001,
                rpt_as_of_mo="2025-12",
                base_ts=existing_ts,
                balance=5000,
                actual_payment=500,
                balance_history=history({0: 5000, 1: 4500}),
                payment_history=history({0: 500, 1: 450}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
            build_summary_row(
                cons_acct_key=3001,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=4700,
                actual_payment=470,
                balance_history=history({0: 4700, 1: 4600}),
                payment_history=history({0: 470, 1: 460}),
                credit_history=history({0: 12000, 1: 12000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], existing_rows)
        write_summary_rows(spark, config["latest_history_table"], existing_rows)

        print("[SETUP] Loading one mixed batch (Case I/II/III/IV)...")
        source_rows = [
            # Case I: new account, single month
            build_source_row(1001, "2026-01", batch_ts, balance=3000, actual_payment=300),
            # Case II: existing account, forward month
            build_source_row(2001, "2026-01", batch_ts, balance=5200, actual_payment=520),
            # Case III: existing account, backfill month
            build_source_row(3001, "2025-12", batch_ts, balance=4800, actual_payment=480),
            # Case IV: new account with multiple months
            build_source_row(4001, "2025-12", batch_ts, balance=7000, actual_payment=700),
            build_source_row(4001, "2026-01", batch_ts, balance=6800, actual_payment=680),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        print("[RUN] First pipeline execution...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        summary_count_1 = spark.table(config["destination_table"]).count()
        latest_count_1 = spark.table(config["latest_history_table"]).count()
        summary_snapshot_1 = _table_snapshot(spark, config["destination_table"])
        latest_snapshot_1 = _table_snapshot(spark, config["latest_history_table"])
        _assert_latest_unique_per_account(spark, config["latest_history_table"])

        print("[RUN] Second pipeline execution with same source batch...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        summary_count_2 = spark.table(config["destination_table"]).count()
        latest_count_2 = spark.table(config["latest_history_table"]).count()
        summary_snapshot_2 = _table_snapshot(spark, config["destination_table"])
        latest_snapshot_2 = _table_snapshot(spark, config["latest_history_table"])
        _assert_latest_unique_per_account(spark, config["latest_history_table"])

        print("[ASSERT] Verifying no output drift on rerun...")
        _assert_equal(summary_count_2, summary_count_1, "summary row count changed on rerun")
        _assert_equal(latest_count_2, latest_count_1, "latest_summary row count changed on rerun")
        _assert_equal(summary_snapshot_2, summary_snapshot_1, "summary data changed on rerun")
        _assert_equal(latest_snapshot_2, latest_snapshot_1, "latest_summary data changed on rerun")

        print("[PASS] test_idempotency")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
