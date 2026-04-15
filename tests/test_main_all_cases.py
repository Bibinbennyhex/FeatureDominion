"""
Integration test for main/summary_inc.py using all four case types.
"""

from datetime import datetime

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


def run_test():
    spark = create_spark_session("main_summary_all_cases_test")
    config = load_main_test_config("main_test")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)

        print("[SETUP] Seeding existing summary/latest data...")
        existing_summary_rows = [
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
        write_summary_rows(spark, config["destination_table"], existing_summary_rows)

        existing_latest_rows = [
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
        write_summary_rows(spark, config["latest_history_table"], existing_latest_rows)

        batch_ts = datetime(2026, 2, 1, 12, 0, 0)

        print("[SETUP] Loading incoming source batch...")
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

        print("[RUN] Executing main pipeline...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Validating outputs...")

        row_2001_2026_01 = fetch_single_row(spark, config["destination_table"], 2001, "2026-01")
        _assert_equal(row_2001_2026_01["balance_am_history"][0], 5200, "Case II current month value")
        _assert_equal(row_2001_2026_01["balance_am_history"][1], 5000, "Case II shifted previous month value")

        row_3001_2025_12 = fetch_single_row(spark, config["destination_table"], 3001, "2025-12")
        _assert_equal(row_3001_2025_12["balance_am_history"][0], 4800, "Case III backfill month row created")

        row_3001_2026_01 = fetch_single_row(spark, config["destination_table"], 3001, "2026-01")
        _assert_equal(row_3001_2026_01["balance_am_history"][1], 4800, "Case III propagated to future row")
        _assert_equal(
            row_3001_2026_01["base_ts"],
            batch_ts,
            "Case III base_ts propagated with GREATEST(existing, backfill)",
        )

        row_4001_2026_01 = fetch_single_row(spark, config["destination_table"], 4001, "2026-01")
        _assert_equal(row_4001_2026_01["balance_am_history"][0], 6800, "Case IV current month value")
        _assert_equal(row_4001_2026_01["balance_am_history"][1], 7000, "Case IV previous month value")

        latest_2001 = fetch_single_row(spark, config["latest_history_table"], 2001, "2026-01")
        _assert_equal(latest_2001["rpt_as_of_mo"], "2026-01", "latest_summary updated for Case II")

        latest_4001 = fetch_single_row(spark, config["latest_history_table"], 4001, "2026-01")
        _assert_equal(latest_4001["rpt_as_of_mo"], "2026-01", "latest_summary updated for Case IV")

        print("[PASS] test_main_all_cases")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
