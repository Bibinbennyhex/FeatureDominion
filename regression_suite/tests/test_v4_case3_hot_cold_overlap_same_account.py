from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def _snapshot_row_arrays(tu, spark, table, acct, month):
    row = tu.fetch_single_row(spark, table, acct, month)
    if row is None:
        return None
    return {col: tuple((row[col] or [])) for col in HISTORY_COLS}


def test_v4_case3_hot_cold_overlap_same_account():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_hot_cold_overlap_same_account")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_hot_cold_overlap_same_account")
    config = tu.load_main_test_config("v4_case3_hot_cold_overlap_same_account")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36
    config["force_cold_case3_broadcast"] = True
    config["cold_case3_broadcast_row_cap"] = 10_000_000

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        acct = 7201
        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        existing_rows = [
            tu.build_summary_row(acct, "2021-10", existing_ts, balance=1100, actual_payment=110),
            tu.build_summary_row(acct, "2022-12", existing_ts, balance=2200, actual_payment=220),
            tu.build_summary_row(acct, "2025-11", existing_ts, balance=5200, actual_payment=520),
            tu.build_summary_row(acct, "2025-12", existing_ts, balance=5300, actual_payment=530),
            tu.build_summary_row(acct, "2026-01", existing_ts, balance=5400, actual_payment=540),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        tu.write_summary_rows(
            spark,
            config["latest_history_table"],
            pad_latest_rows([existing_rows[-1]]),
        )
        pre_summary_202601 = _snapshot_row_arrays(tu, spark, config["destination_table"], acct, "2026-01")
        pre_latest_202601 = _snapshot_row_arrays(tu, spark, config["latest_history_table"], acct, "2026-01")

        # Same account with both hot and cold Case III updates.
        source_rows = [
            tu.build_source_row(acct, "2025-12", source_ts, balance=9300, actual_payment=930),
            tu.build_source_row(acct, "2025-11", source_ts, balance=9200, actual_payment=920),
            tu.build_source_row(acct, "2022-12", source_ts, balance=8200, actual_payment=820),
            tu.build_source_row(acct, "2021-10", source_ts, balance=8100, actual_payment=810),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        # Pre-check classification split.
        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        case_iii_df = classified.filter((F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete")))
        _assert_true(case_iii_df.count() == 4, "Expected 4 non-delete CASE_III rows")

        split_df = case_iii_df
        if "month_int" not in split_df.columns:
            split_df = split_df.withColumn("month_int", F.expr(module.month_to_int_expr(config["partition_column"])))
        split_df = split_df.withColumn("case3_month_diff", F.col("max_month_int") - F.col("month_int"))
        split_counts = split_df.agg(
            F.sum(F.when(F.col("case3_month_diff") <= F.lit(int(config["case3_hot_window_months"])), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
            F.sum(F.when(F.col("case3_month_diff") > F.lit(int(config["case3_hot_window_months"])), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
        ).first()
        hot_count = int(split_counts["hot_count"] or 0)
        cold_count = int(split_counts["cold_count"] or 0)
        _assert_true(hot_count == 2, f"Expected 2 hot rows, got {hot_count}")
        _assert_true(cold_count == 2, f"Expected 2 cold rows, got {cold_count}")

        module.run_pipeline(spark, config)

        # All source months should be updated in summary for this account.
        expected_balance = {
            "2025-12": 9300,
            "2025-11": 9200,
            "2022-12": 8200,
            "2021-10": 8100,
        }
        for mo, bal in expected_balance.items():
            row = tu.fetch_single_row(spark, config["destination_table"], acct, mo)
            _assert_true(int(row["balance_am"]) == bal, f"{mo} expected balance {bal}, got {row['balance_am']}")
            _assert_true(row["base_ts"] == source_ts, f"{mo} expected base_ts {source_ts}, got {row['base_ts']}")

        post_summary_202601 = _snapshot_row_arrays(tu, spark, config["destination_table"], acct, "2026-01")
        post_latest_202601 = _snapshot_row_arrays(tu, spark, config["latest_history_table"], acct, "2026-01")
        _assert_true(pre_summary_202601 != post_summary_202601, "Summary arrays should change for overlap account")
        _assert_true(pre_latest_202601 != post_latest_202601, "Latest arrays should change for overlap account")
        _assert_true(
            post_summary_202601["balance_am_history"][1] == 9300,
            "2026-01 summary idx1 should contain 2025-12 overwrite",
        )
        _assert_true(
            post_summary_202601["balance_am_history"][2] == 9200,
            "2026-01 summary idx2 should contain 2025-11 overwrite",
        )

        print(
            "[PASS] v4 Case III hot/cold overlap (same account) verified "
            f"(hot={hot_count}, cold={cold_count}, account={acct})"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_hot_cold_overlap_same_account()
    print("[PASS] test_v4_case3_hot_cold_overlap_same_account.py")

