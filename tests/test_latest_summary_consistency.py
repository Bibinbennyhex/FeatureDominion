"""
Regression test: latest_summary must match true latest rows from summary.

This test intentionally targets a backfill-only batch to verify whether
latest_summary is kept in sync when summary rows are modified via Case III.
"""

from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

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
    summary_df = spark.table(summary_table)
    latest_df = spark.table(latest_table)

    w = Window.partitionBy("cons_acct_key").orderBy(
        F.col("rpt_as_of_mo").desc(),
        F.col("base_ts").desc(),
    )

    expected_latest_df = (
        summary_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    select_cols = latest_df.columns
    expected_latest_df = expected_latest_df.select(*select_cols)
    latest_df = latest_df.select(*select_cols)

    missing_in_latest = expected_latest_df.exceptAll(latest_df)
    extra_in_latest = latest_df.exceptAll(expected_latest_df)

    missing_count = missing_in_latest.count()
    extra_count = extra_in_latest.count()

    if missing_count > 0 or extra_count > 0:
        sample_missing = [
            r.asDict()
            for r in missing_in_latest.select("cons_acct_key", "rpt_as_of_mo", "base_ts").limit(5).collect()
        ]
        sample_extra = [
            r.asDict()
            for r in extra_in_latest.select("cons_acct_key", "rpt_as_of_mo", "base_ts").limit(5).collect()
        ]
        raise AssertionError(
            "latest_summary is inconsistent with summary latest rows "
            f"(missing={missing_count}, extra={extra_count}, "
            f"missing_samples={sample_missing}, extra_samples={sample_extra})"
        )


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
