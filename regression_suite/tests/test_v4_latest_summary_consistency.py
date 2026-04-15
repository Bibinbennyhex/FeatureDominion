from datetime import datetime
from pathlib import Path
import sys

from tests.v4_contract_utils import (
    LATEST_HISTORY_LEN,
    SUMMARY_HISTORY_LEN,
    assert_latest_matches_summary_v4,
    load_v4_as_summary_inc,
    pad_latest_rows,
)


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def test_v4_latest_summary_consistency():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_latest_summary_consistency")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_latest_summary_consistency")
    config = tu.load_main_test_config("main_latest_consistency")
    config["history_length"] = SUMMARY_HISTORY_LEN
    config["latest_history_window_months"] = LATEST_HISTORY_LEN
    config["validate_latest_history_window"] = True

    try:
        print("[SETUP] Resetting tables...")
        tu.reset_tables(spark, config)

        old_ts = datetime(2026, 1, 5, 8, 0, 0)
        new_backfill_ts = datetime(2026, 2, 10, 9, 30, 0)

        print("[SETUP] Seeding existing summary/latest data...")
        summary_rows = [
            tu.build_summary_row(
                cons_acct_key=5001,
                rpt_as_of_mo="2025-12",
                base_ts=old_ts,
                balance=5000,
                actual_payment=500,
                balance_history=tu.history({0: 5000}),
                payment_history=tu.history({0: 500}),
                credit_history=tu.history({0: 11000}),
                past_due_history=tu.history({0: 0}),
                rating_history=tu.history({0: "0"}),
                dpd_history=tu.history({0: 0}),
                asset_history=tu.history({0: "A"}),
            ),
            tu.build_summary_row(
                cons_acct_key=5001,
                rpt_as_of_mo="2026-01",
                base_ts=old_ts,
                balance=5100,
                actual_payment=510,
                balance_history=tu.history({0: 5100, 1: 5000}),
                payment_history=tu.history({0: 510, 1: 500}),
                credit_history=tu.history({0: 11000, 1: 11000}),
                past_due_history=tu.history({0: 0, 1: 0}),
                rating_history=tu.history({0: "0", 1: "0"}),
                dpd_history=tu.history({0: 0, 1: 0}),
                asset_history=tu.history({0: "A", 1: "A"}),
            ),
        ]
        tu.write_summary_rows(spark, config["destination_table"], summary_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows([summary_rows[-1]]))

        print("[SETUP] Loading one backfill input record...")
        tu.write_source_rows(
            spark,
            config["source_table"],
            [
                tu.build_source_row(
                    cons_acct_key=5001,
                    rpt_as_of_mo="2025-12",
                    base_ts=new_backfill_ts,
                    balance=4900,
                    actual_payment=490,
                )
            ],
        )

        print("[RUN] Executing main pipeline...")
        module.cleanup(spark)
        module.run_pipeline(spark, config)
        tu.assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Sanity-checking summary was updated by backfill...")
        jan_row = tu.fetch_single_row(spark, config["destination_table"], 5001, "2026-01")
        _assert_equal(jan_row["balance_am_history"][1], 4900, "Case III patched future history index")
        _assert_equal(jan_row["base_ts"], new_backfill_ts, "Case III propagated base_ts")

        print("[ASSERT] Validating latest_summary consistency with summary...")
        assert_latest_matches_summary_v4(spark, config["destination_table"], config["latest_history_table"])

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_latest_summary_consistency()
    print("[PASS] test_v4_latest_summary_consistency.py")

