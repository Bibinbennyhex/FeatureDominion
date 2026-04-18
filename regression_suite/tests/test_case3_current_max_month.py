"""
Regression test for same-month Case III with no existing history tail.

Invariant under test:
- Existing summary row has only index 0 populated (index 1..N are NULL)
- Incoming source row is same month (Case III)
- summary and latest_summary should match on rolling histories/grid
  (scalar columns can differ by design in current latest_summary Case III update).
"""

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


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def run_test():
    spark = create_spark_session("main_case3_current_max_month_test")
    config = load_main_test_config("main_case3_current_max_month")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 15, 0, 0, 0)

        print("[SETUP] Seeding summary/latest...")
        existing_rows = [
            # Account 111: current max month exists, but no historical tail (index 1..N = NULL)
            build_summary_row(
                cons_acct_key=111,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=5000,
                actual_payment=500,
            ),
            # Account 222: behind at 2025-12
            build_summary_row(
                cons_acct_key=222,
                rpt_as_of_mo="2025-12",
                base_ts=existing_ts,
                balance=4500,
                actual_payment=450,
                balance_history=history({0: 4500}),
                payment_history=history({0: 450}),
                credit_history=history({0: 9000}),
                past_due_history=history({0: 0}),
                rating_history=history({0: "0"}),
                dpd_history=history({0: 0}),
                asset_history=history({0: "A"}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], existing_rows)
        write_summary_rows(spark, config["latest_history_table"], existing_rows)

        print("[SETUP] Loading source batch...")
        source_rows = [
            # Same as account max month => expected CASE_III
            build_source_row(111, "2026-01", source_ts, balance=5100, actual_payment=510),
            # Forward wrt account max month => expected CASE_II
            build_source_row(222, "2026-01", source_ts, balance=4700, actual_payment=470),
            # New account => expected CASE_I
            build_source_row(333, "2026-01", source_ts, balance=3000, actual_payment=300),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        print("[RUN] Executing pipeline...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Validating case outcomes and summary/latest consistency...")
        summary = spark.table(config["destination_table"]).filter(F.col("cons_acct_key").isin(111, 222, 333))
        latest = spark.table(config["latest_history_table"]).filter(F.col("cons_acct_key").isin(111, 222, 333))

        # Base shape checks
        _assert_true(
            summary.filter((F.col("cons_acct_key") == 111) & (F.col("rpt_as_of_mo") == "2026-01")).count() == 1,
            "acct 111 should have one summary row for 2026-01",
        )
        _assert_true(
            summary.filter((F.col("cons_acct_key") == 222) & (F.col("rpt_as_of_mo") == "2026-01")).count() == 1,
            "acct 222 should have one summary row for 2026-01",
        )
        _assert_true(
            summary.filter((F.col("cons_acct_key") == 333) & (F.col("rpt_as_of_mo") == "2026-01")).count() == 1,
            "acct 333 should have one summary row for 2026-01",
        )

        # latest_summary should be one-row-per-account for tested keys
        dup_latest = latest.groupBy("cons_acct_key").count().filter(F.col("count") != 1).count()
        _assert_true(dup_latest == 0, "latest_summary should have one row per tested account")

        # Consistency check for same-month Case III account (111):
        # summary 36-array should be a prefix of latest_summary 72-array.
        s111 = (
            summary
            .filter((F.col("cons_acct_key") == 111) & (F.col("rpt_as_of_mo") == "2026-01"))
            .select(
                "cons_acct_key",
                "rpt_as_of_mo",
                "actual_payment_am_history",
                "balance_am_history",
                "credit_limit_am_history",
                "past_due_am_history",
                "payment_rating_cd_history",
                "days_past_due_history",
                "asset_class_cd_4in_history",
                "payment_history_grid",
            )
        )
        l111 = (
            latest
            .filter(F.col("cons_acct_key") == 111)
            .select(
                "cons_acct_key",
                "rpt_as_of_mo",
                "actual_payment_am_history",
                "balance_am_history",
                "credit_limit_am_history",
                "past_due_am_history",
                "payment_rating_cd_history",
                "days_past_due_history",
                "asset_class_cd_4in_history",
                "payment_history_grid",
            )
        )
        s111_row = s111.collect()[0]
        l111_row = l111.collect()[0]
        for col_name in [
            "actual_payment_am_history",
            "balance_am_history",
            "credit_limit_am_history",
            "past_due_am_history",
            "payment_rating_cd_history",
            "days_past_due_history",
            "asset_class_cd_4in_history",
        ]:
            s_hist = list(s111_row[col_name] or [])
            l_hist = list(l111_row[col_name] or [])
            _assert_true(len(s_hist) == 36, f"{col_name} summary length expected 36")
            _assert_true(len(l_hist) == 72, f"{col_name} latest length expected 72")
            _assert_true(
                s_hist == l_hist[:36],
                f"{col_name} summary should match latest prefix for same-month Case III",
            )

        # Tail should remain NULL (index 1) under this invariant.
        _assert_true(
            s111_row["balance_am_history"][1] is None
            and s111_row["actual_payment_am_history"][1] is None,
            "same-month Case III no-tail invariant violated: index 1 should remain NULL",
        )

        print("[PASS] test_case3_current_max_month")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
