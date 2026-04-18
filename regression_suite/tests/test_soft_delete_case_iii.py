"""
Integration test for Case III soft-delete behavior in main/summary_inc.py.

Scenarios covered:
1) Existing month soft-delete updates summary.soft_del_cd and preserves deleted-month arrays.
2) Future months for the same account are patched (deleted month position -> NULL).
3) Non-existing delete month is ignored.
4) If deleted month is current latest month, latest_summary is reconstructed to prior non-deleted month.
5) Same-batch delete/non-delete duplicates follow dedup winner by latest timestamps.
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


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def run_test():
    spark = create_spark_session("main_soft_delete_case_iii_test")
    config = load_main_test_config("main_soft_delete_case_iii")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        existing_ts = datetime(2026, 2, 1, 0, 0, 0)

        print("[SETUP] Seeding summary/latest...")
        seed_rows = [
            build_summary_row(
                cons_acct_key=7001,
                rpt_as_of_mo="2025-12",
                base_ts=existing_ts,
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
                cons_acct_key=7001,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=5200,
                actual_payment=520,
                balance_history=history({0: 5200, 1: 5000}),
                payment_history=history({0: 520, 1: 500}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
            build_summary_row(
                cons_acct_key=7001,
                rpt_as_of_mo="2026-02",
                base_ts=existing_ts,
                balance=5400,
                actual_payment=540,
                balance_history=history({0: 5400, 1: 5200, 2: 5000}),
                payment_history=history({0: 540, 1: 520, 2: 500}),
                credit_history=history({0: 10000, 1: 10000, 2: 10000}),
                past_due_history=history({0: 0, 1: 0, 2: 0}),
                rating_history=history({0: "0", 1: "0", 2: "0"}),
                dpd_history=history({0: 0, 1: 0, 2: 0}),
                asset_history=history({0: "A", 1: "A", 2: "A"}),
            ),
            build_summary_row(
                cons_acct_key=7002,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
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
                cons_acct_key=7002,
                rpt_as_of_mo="2026-02",
                base_ts=existing_ts,
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
        write_summary_rows(spark, config["destination_table"], seed_rows)

        latest_rows = [
            row for row in seed_rows
            if (row["cons_acct_key"], row["rpt_as_of_mo"]) in {(7001, "2026-02"), (7002, "2026-02")}
        ]
        write_summary_rows(spark, config["latest_history_table"], latest_rows)

        # Snapshot pre-delete arrays for deleted month row to validate preservation.
        pre_delete_7001_2026_01 = fetch_single_row(spark, config["destination_table"], 7001, "2026-01")
        expected_deleted_month_history = list(pre_delete_7001_2026_01["balance_am_history"])

        delete_winner_ts = datetime(2026, 2, 15, 12, 0, 0)
        delete_loser_ts = datetime(2026, 2, 15, 11, 0, 0)
        latest_delete_ts = datetime(2026, 2, 16, 10, 0, 0)

        print("[SETUP] Loading soft-delete source batch...")
        source_rows = [
            # Duplicate for same account+month: later delete should win.
            build_source_row(7001, "2026-01", delete_loser_ts, balance=9998, actual_payment=998, soft_del_cd=""),
            build_source_row(7001, "2026-01", delete_winner_ts, balance=9999, actual_payment=999, soft_del_cd="1"),
            # Non-existing summary month delete should be ignored.
            build_source_row(7001, "2024-12", delete_winner_ts, balance=1111, actual_payment=111, soft_del_cd="1"),
            # Latest-month delete (account 7002) should force latest_summary reconstruction.
            build_source_row(7002, "2026-02", latest_delete_ts, balance=2222, actual_payment=222, soft_del_cd="4"),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        print("[RUN] Executing pipeline...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Validating Case III soft-delete behavior...")

        # 1) Deleted month row exists and is flagged; arrays stay unchanged.
        s_7001_2026_01 = fetch_single_row(spark, config["destination_table"], 7001, "2026-01")
        _assert_equal(s_7001_2026_01["soft_del_cd"], "1", "Deleted month row should be soft-deleted")
        _assert_equal(
            s_7001_2026_01["balance_am_history"],
            expected_deleted_month_history,
            "Deleted month history arrays must be preserved",
        )

        # 2) Future month patches: deleted position (index=1 for 2026-02 vs 2026-01) becomes NULL.
        s_7001_2026_02 = fetch_single_row(spark, config["destination_table"], 7001, "2026-02")
        _assert_equal(s_7001_2026_02["balance_am_history"][0], 5400, "Future month current index should remain")
        _assert_equal(s_7001_2026_02["balance_am_history"][1], None, "Future month deleted position should be NULL")
        _assert_equal(s_7001_2026_02["balance_am_history"][2], 5000, "Future month other positions should remain")

        # latest_summary for account 7001 keeps month but should carry patched arrays.
        l_7001 = fetch_single_row(spark, config["latest_history_table"], 7001, "2026-02")
        _assert_equal(l_7001["balance_am_history"][1], None, "latest_summary should include future patch arrays")

        # 3) Non-existing month delete ignored.
        ignored_count = (
            spark.table(config["destination_table"])
            .filter("cons_acct_key = 7001 AND rpt_as_of_mo = '2024-12'")
            .count()
        )
        _assert_equal(ignored_count, 0, "Non-existing delete month should be ignored")

        # 4) Latest-month delete reconstructs latest_summary to prior non-deleted month.
        s_7002_2026_02 = fetch_single_row(spark, config["destination_table"], 7002, "2026-02")
        _assert_equal(s_7002_2026_02["soft_del_cd"], "4", "Deleted latest month should be flagged in summary")
        l_7002 = fetch_single_row(spark, config["latest_history_table"], 7002, "2026-01")
        _assert_equal(l_7002["rpt_as_of_mo"], "2026-01", "latest_summary should reconstruct to prior month")

        # 5) One-row-per-account invariant on latest_summary.
        dup_latest = (
            spark.table(config["latest_history_table"])
            .filter("cons_acct_key IN (7001, 7002)")
            .groupBy("cons_acct_key")
            .count()
            .filter("count != 1")
            .count()
        )
        _assert_true(dup_latest == 0, "latest_summary should keep one row per account")

        print("[PASS] test_soft_delete_case_iii")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
