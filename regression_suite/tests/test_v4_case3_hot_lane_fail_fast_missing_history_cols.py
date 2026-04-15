from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_hot_lane_fail_fast_missing_history_cols():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_hot_lane_fail_fast_missing_history_cols")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_hot_lane_fail_fast_missing_history_cols")
    config = tu.load_main_test_config("v4_case3_hot_lane_fail_fast_missing_history_cols")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36

    acct = 9802
    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        existing_rows = [
            tu.build_summary_row(acct, "2026-01", existing_ts, balance=6400, actual_payment=640),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(existing_rows))
        tu.write_source_rows(
            spark,
            config["source_table"],
            [tu.build_source_row(acct, "2025-12", source_ts, balance=6300, actual_payment=630)],
        )

        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        case_iii_df = classified.filter((F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete")))
        _assert_true(case_iii_df.count() == 1, "Expected 1 hot CASE_III row")

        config["_runtime_cache"]["latest_summary_cols"] = [
            col for col in config["_runtime_cache"]["latest_summary_cols"]
            if col not in HISTORY_COLS
        ]

        error_message = None
        try:
            module.process_case_iii(spark, case_iii_df, config, expected_rows=1)
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)

        _assert_true(error_message is not None, "Expected fail-fast error for missing history cols")
        _assert_true("requires latest_summary history columns" in error_message, f"Unexpected error: {error_message}")

        print("[PASS] test_v4_case3_hot_lane_fail_fast_missing_history_cols")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_hot_lane_fail_fast_missing_history_cols()
    print("[PASS] test_v4_case3_hot_lane_fail_fast_missing_history_cols.py")
