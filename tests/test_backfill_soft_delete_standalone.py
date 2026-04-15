"""
Direct integration test for main/backfill_soft_delete_from_accounts.py.

Validates standalone backfill behavior (without summary_inc pipeline):
1) Deletes update month rows in summary (soft_del_cd + base_ts).
2) Future month rolling arrays are nullified at deleted positions.
3) latest_summary receives patched arrays for matching latest month.
4) Latest-month delete reconstructs latest_summary to prior non-deleted row.
5) Multi-delete overlap does not fail latest_summary merge cardinality.
"""

from datetime import datetime
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_accounts as backfill_job
from test_utils import (
    assert_deletion_aware_invariants,
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


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def run_test():
    spark = create_spark_session("main_backfill_soft_delete_standalone_test")
    config = load_main_test_config("main_backfill_soft_delete_standalone")

    try:
        reset_tables(spark, config)

        # Point standalone script to isolated test namespace.
        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)

        # Account 8101: overlapping deletes for 2025-12 and 2026-01.
        seed_8101 = [
            build_summary_row(
                cons_acct_key=8101,
                rpt_as_of_mo="2025-12",
                base_ts=seed_ts,
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
                cons_acct_key=8101,
                rpt_as_of_mo="2026-01",
                base_ts=seed_ts,
                balance=5100,
                actual_payment=510,
                balance_history=history({0: 5100, 1: 5000}),
                payment_history=history({0: 510, 1: 500}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
            build_summary_row(
                cons_acct_key=8101,
                rpt_as_of_mo="2026-02",
                base_ts=seed_ts,
                balance=5200,
                actual_payment=520,
                balance_history=history({0: 5200, 1: 5100, 2: 5000}),
                payment_history=history({0: 520, 1: 510, 2: 500}),
                credit_history=history({0: 10000, 1: 10000, 2: 10000}),
                past_due_history=history({0: 0, 1: 0, 2: 0}),
                rating_history=history({0: "0", 1: "0", 2: "0"}),
                dpd_history=history({0: 0, 1: 0, 2: 0}),
                asset_history=history({0: "A", 1: "A", 2: "A"}),
            ),
        ]

        # Account 8102: latest month delete for latest_summary reconstruction.
        seed_8102 = [
            build_summary_row(
                cons_acct_key=8102,
                rpt_as_of_mo="2026-01",
                base_ts=seed_ts,
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
                cons_acct_key=8102,
                rpt_as_of_mo="2026-02",
                base_ts=seed_ts,
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

        all_summary_rows = seed_8101 + seed_8102
        write_summary_rows(spark, config["destination_table"], all_summary_rows)

        latest_rows = [
            [r for r in seed_8101 if r["rpt_as_of_mo"] == "2026-02"][0],
            [r for r in seed_8102 if r["rpt_as_of_mo"] == "2026-02"][0],
        ]
        write_summary_rows(spark, config["latest_history_table"], latest_rows)

        source_rows = [
            build_source_row(8101, "2025-12", datetime(2026, 2, 15, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            build_source_row(8101, "2026-01", datetime(2026, 2, 15, 11, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            # Duplicate same account+month with older ts should lose in dedup.
            build_source_row(8101, "2026-01", datetime(2026, 2, 15, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            build_source_row(8102, "2026-02", datetime(2026, 2, 16, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        has_updates, history_cols, grid_specs = backfill_job._prepare_case_tables(spark)
        _assert_equal(has_updates, True, "Expected soft-delete updates to be detected")
        backfill_job._merge_case_tables_chunked(spark, history_cols, grid_specs)

        s_8101_2025_12 = fetch_single_row(spark, config["destination_table"], 8101, "2025-12")
        s_8101_2026_01 = fetch_single_row(spark, config["destination_table"], 8101, "2026-01")
        s_8101_2026_02 = fetch_single_row(spark, config["destination_table"], 8101, "2026-02")
        l_8101_2026_02 = fetch_single_row(spark, config["latest_history_table"], 8101, "2026-02")
        l_8102_2026_01 = fetch_single_row(spark, config["latest_history_table"], 8102, "2026-01")

        _assert_equal(s_8101_2025_12["soft_del_cd"], "1", "Deleted month 2025-12 should be flagged")
        _assert_equal(s_8101_2026_01["soft_del_cd"], "1", "Deleted month 2026-01 should use latest delete winner")
        _assert_equal(s_8101_2026_02["balance_am_history"][1], None, "Future index 1 should be nullified")
        _assert_equal(s_8101_2026_02["balance_am_history"][2], None, "Future index 2 should be nullified")
        _assert_equal(l_8101_2026_02["balance_am_history"][1], None, "latest_summary index 1 should be nullified")
        _assert_equal(l_8101_2026_02["balance_am_history"][2], None, "latest_summary index 2 should be nullified")
        _assert_equal(l_8102_2026_01["rpt_as_of_mo"], "2026-01", "latest_summary should reconstruct to prior non-deleted month")
        assert_deletion_aware_invariants(spark, config)

        print("[PASS] test_backfill_soft_delete_standalone")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
