from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def _table_key_set(spark, table_name: str):
    if not spark.catalog.tableExists(table_name):
        return set()
    return {
        int(r["cons_acct_key"])
        for r in spark.read.table(table_name).select("cons_acct_key").distinct().collect()
    }


def _history_prefix_match(summary_row, latest_row):
    for col_name in HISTORY_COLS:
        summary_hist = list(summary_row[col_name] or [])
        latest_hist = list(latest_row[col_name] or [])
        _assert_true(len(summary_hist) == 36, f"{col_name} summary length expected 36")
        _assert_true(len(latest_hist) == 72, f"{col_name} latest length expected 72")
        _assert_true(summary_hist == latest_hist[:36], f"{col_name} summary should match latest prefix")


def test_v4_case3_account_level_lane_routing_mixed_with_latest():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_lane_routing_mixed_with_latest")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_lane_routing_mixed_with_latest")
    config = tu.load_main_test_config("v4_case3_lane_routing_mixed_with_latest")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36

    cold_acct = 9301
    hot_acct = 9302
    mixed_acct = 9303

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        existing_rows = [
            tu.build_summary_row(cold_acct, "2021-10", existing_ts, balance=4100, actual_payment=410),
            tu.build_summary_row(cold_acct, "2026-01", existing_ts, balance=4150, actual_payment=415),
            tu.build_summary_row(hot_acct, "2026-01", existing_ts, balance=5150, actual_payment=515),
            tu.build_summary_row(mixed_acct, "2021-11", existing_ts, balance=6100, actual_payment=610),
            tu.build_summary_row(mixed_acct, "2026-01", existing_ts, balance=6250, actual_payment=625),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        latest_rows = [r for r in existing_rows if r["rpt_as_of_mo"] == "2026-01"]
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(latest_rows))

        source_rows = [
            # cold-only account
            tu.build_source_row(cold_acct, "2021-10", source_ts, balance=4110, actual_payment=411),
            # hot-only account (latest month)
            tu.build_source_row(hot_acct, "2026-01", source_ts, balance=5120, actual_payment=512),
            # mixed account (latest + cold month)
            tu.build_source_row(mixed_acct, "2026-01", source_ts, balance=6330, actual_payment=633),
            tu.build_source_row(mixed_acct, "2021-11", source_ts, balance=6140, actual_payment=614),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        case_iii_df = classified.filter(
            (F.col("case_type") == "CASE_III")
            & (~F.col("_is_soft_delete"))
            & (F.col("cons_acct_key").isin(cold_acct, hot_acct, mixed_acct))
        )
        _assert_true(case_iii_df.count() == 4, "Expected 4 non-delete Case III rows")

        module.process_case_iii(spark, case_iii_df, config, expected_rows=4)

        history_patch_keys = _table_key_set(
            spark,
            "execution_catalog.checkpointdb.case_3_latest_history_context_patch",
        )
        latest_month_patch_keys = _table_key_set(
            spark,
            "execution_catalog.checkpointdb.case_3_latest_month_patch",
        )

        _assert_true(
            hot_acct in history_patch_keys or hot_acct in latest_month_patch_keys,
            "Hot-only account should appear in at least one latest-context patch table",
        )
        _assert_true(
            cold_acct not in history_patch_keys and cold_acct not in latest_month_patch_keys,
            "Cold-only account should be excluded from latest-context patch tables",
        )
        _assert_true(
            mixed_acct not in history_patch_keys,
            "Mixed account should be excluded from case_3_latest_history_context_patch",
        )
        _assert_true(
            mixed_acct not in latest_month_patch_keys,
            "Mixed account should be excluded from case_3_latest_month_patch",
        )

        module.write_backfill_results(spark, config)

        for acct, month, expected_balance in [
            (cold_acct, "2021-10", 4110),
            (hot_acct, "2026-01", 5120),
            (mixed_acct, "2026-01", 6330),
            (mixed_acct, "2021-11", 6140),
        ]:
            row = tu.fetch_single_row(spark, config["destination_table"], acct, month)
            _assert_true(int(row["balance_am"]) == expected_balance, f"{acct}/{month} balance mismatch")
            _assert_true(row["base_ts"] == source_ts, f"{acct}/{month} base_ts mismatch")

        for acct in [cold_acct, hot_acct, mixed_acct]:
            summary_latest = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            latest_row = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")
            _history_prefix_match(summary_latest, latest_row)

        print(
            "[PASS] Case III lane routing (mixed with latest month) verified "
            f"(cold_only={cold_acct}, hot_only={hot_acct}, mixed={mixed_acct})"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_account_level_lane_routing_mixed_with_latest()
    print("[PASS] test_v4_case3_account_level_lane_routing_mixed_with_latest.py")
