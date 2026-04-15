from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import (
    HISTORY_COLS,
    LATEST_HISTORY_LEN,
    SUMMARY_HISTORY_LEN,
    load_v4_as_summary_inc,
    pad_latest_rows,
)


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_current_max_month():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_current_max_month")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_current_max_month")
    config = tu.load_main_test_config("main_case3_current_max_month")
    config["history_length"] = SUMMARY_HISTORY_LEN
    config["latest_history_window_months"] = LATEST_HISTORY_LEN
    config["validate_latest_history_window"] = True

    try:
        print("[SETUP] Resetting tables...")
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 15, 0, 0, 0)

        print("[SETUP] Seeding summary/latest...")
        existing_rows = [
            tu.build_summary_row(
                cons_acct_key=111,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=5000,
                actual_payment=500,
            ),
            tu.build_summary_row(
                cons_acct_key=222,
                rpt_as_of_mo="2025-12",
                base_ts=existing_ts,
                balance=4500,
                actual_payment=450,
                balance_history=tu.history({0: 4500}),
                payment_history=tu.history({0: 450}),
                credit_history=tu.history({0: 9000}),
                past_due_history=tu.history({0: 0}),
                rating_history=tu.history({0: "0"}),
                dpd_history=tu.history({0: 0}),
                asset_history=tu.history({0: "A"}),
            ),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(existing_rows))

        print("[SETUP] Loading source batch...")
        source_rows = [
            tu.build_source_row(111, "2026-01", source_ts, balance=5100, actual_payment=510),
            tu.build_source_row(222, "2026-01", source_ts, balance=4700, actual_payment=470),
            tu.build_source_row(333, "2026-01", source_ts, balance=3000, actual_payment=300),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        print("[RUN] Executing pipeline...")
        module.cleanup(spark)
        module.run_pipeline(spark, config)
        tu.assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Validating case outcomes and summary/latest consistency...")
        summary = spark.table(config["destination_table"]).filter(F.col("cons_acct_key").isin(111, 222, 333))
        latest = spark.table(config["latest_history_table"]).filter(F.col("cons_acct_key").isin(111, 222, 333))

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

        dup_latest = latest.groupBy("cons_acct_key").count().filter(F.col("count") != 1).count()
        _assert_true(dup_latest == 0, "latest_summary should have one row per tested account")

        s111 = (
            summary.filter((F.col("cons_acct_key") == 111) & (F.col("rpt_as_of_mo") == "2026-01")).collect()[0]
        )
        l111 = latest.filter(F.col("cons_acct_key") == 111).collect()[0]

        for col_name in HISTORY_COLS:
            _assert_true(len(s111[col_name]) == SUMMARY_HISTORY_LEN, f"{col_name} summary length should be 36")
            _assert_true(len(l111[col_name]) == LATEST_HISTORY_LEN, f"{col_name} latest length should be 72")
            _assert_true(
                list(s111[col_name]) == list(l111[col_name])[:SUMMARY_HISTORY_LEN],
                f"{col_name} summary should match latest prefix for same-month Case III",
            )

        _assert_true(
            s111["payment_history_grid"] == l111["payment_history_grid"],
            "payment_history_grid should match between summary and latest_summary",
        )

        _assert_true(
            s111["balance_am_history"][1] is None
            and s111["actual_payment_am_history"][1] is None
            and l111["balance_am_history"][1] is None
            and l111["actual_payment_am_history"][1] is None,
            "same-month Case III no-tail invariant violated: index 1 should remain NULL in both summary and latest prefix",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_current_max_month()
    print("[PASS] test_v4_case3_current_max_month.py")

