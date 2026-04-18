"""
Regression test: latest_summary must match true latest rows from summary.

This test intentionally targets a backfill-only batch to verify whether
latest_summary is kept in sync when summary rows are modified via Case III.
"""

from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

from tests.v4_contract_utils import assert_latest_matches_summary_v4

from test_utils import (
    assert_watermark_tracker_consistent,
    build_source_row,
    build_summary_row,
    create_spark_session,
    fetch_single_row,
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


def _assert_latest_matches_summary(spark, summary_table: str, latest_table: str):
    assert_latest_matches_summary_v4(spark, summary_table, latest_table)


def run_test():
    spark = create_spark_session("main_latest_summary_consistency_test")
    config = load_main_test_config("main_latest_consistency")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        old_ts = datetime(2026, 1, 5, 8, 0, 0)
        new_backfill_ts = datetime(2026, 2, 10, 9, 30, 0)

        print("[SETUP] Seeding existing summary/latest data...")
        summary_rows = [
            build_summary_row(
                cons_acct_key=5001,
                rpt_as_of_mo="2025-12",
                base_ts=old_ts,
                balance=5000,
                actual_payment=500,
                balance_history=history({0: 5000}),
                payment_history=history({0: 500}),
                credit_history=history({0: 11000}),
                past_due_history=history({0: 0}),
                rating_history=history({0: "0"}),
                dpd_history=history({0: 0}),
                asset_history=history({0: "A"}),
            ),
            build_summary_row(
                cons_acct_key=5001,
                rpt_as_of_mo="2026-01",
                base_ts=old_ts,
                balance=5100,
                actual_payment=510,
                balance_history=history({0: 5100, 1: 5000}),
                payment_history=history({0: 510, 1: 500}),
                credit_history=history({0: 11000, 1: 11000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], summary_rows)
        write_summary_rows(spark, config["latest_history_table"], [summary_rows[-1]])

        print("[SETUP] Loading one backfill input record...")
        source_rows = [
            build_source_row(
                cons_acct_key=5001,
                rpt_as_of_mo="2025-12",
                base_ts=new_backfill_ts,
                balance=4900,
                actual_payment=490,
            )
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        print("[RUN] Executing main pipeline...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Sanity-checking summary was updated by backfill...")
        jan_row = fetch_single_row(spark, config["destination_table"], 5001, "2026-01")
        _assert_equal(jan_row["balance_am_history"][1], 4900, "Case III patched future history index")
        _assert_equal(jan_row["base_ts"], new_backfill_ts, "Case III propagated base_ts")

        print("[ASSERT] Validating latest_summary consistency with summary...")
        _assert_latest_matches_summary(
            spark,
            config["destination_table"],
            config["latest_history_table"],
        )

        print("[PASS] test_latest_summary_consistency")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
