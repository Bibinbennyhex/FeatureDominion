from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import DELETE_CODES, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_single_key_hot_cold_delete_mix():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_single_key_hot_cold_delete_mix")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_single_key_hot_cold_delete_mix")
    config = tu.load_main_test_config("v4_case3_single_key_hot_cold_delete_mix")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        for idx, delete_code in enumerate(DELETE_CODES):
            tu.reset_tables(spark, config)

            acct = 7001 + idx
            existing_ts = datetime(2026, 1, 1, 0, 0, 0)
            source_ts = datetime(2026, 2, 1, 12, 0, 0)

            # Seed existing summary rows for one account, including a low month anchor.
            existing_rows = [
                tu.build_summary_row(acct, "2021-01", existing_ts, balance=1100, actual_payment=110),
                tu.build_summary_row(acct, "2022-12", existing_ts, balance=2200, actual_payment=220),
                tu.build_summary_row(acct, "2025-10", existing_ts, balance=5100, actual_payment=510),
                tu.build_summary_row(acct, "2025-11", existing_ts, balance=5200, actual_payment=520),
                tu.build_summary_row(acct, "2025-12", existing_ts, balance=5300, actual_payment=530),
                tu.build_summary_row(
                    acct,
                    "2026-01",
                    existing_ts,
                    balance=5400,
                    actual_payment=540,
                    balance_history=tu.history({0: 5400, 1: 99991}),
                    payment_history=tu.history({0: 540, 1: 99992}),
                    credit_history=tu.history({0: 10000, 1: 99993}),
                    past_due_history=tu.history({0: 0, 1: 99994}),
                    rating_history=tu.history({0: "0", 1: "X"}),
                    dpd_history=tu.history({0: 0, 1: 99995}),
                    asset_history=tu.history({0: "A", 1: "Z"}),
                ),
            ]
            tu.write_summary_rows(spark, config["destination_table"], existing_rows)
            tu.write_summary_rows(
                spark,
                config["latest_history_table"],
                pad_latest_rows([existing_rows[-1]]),
            )

            # Same account gets mixed Case III updates:
            # - hot rows: 2025-11, 2025-10
            # - cold rows: 2022-12, 2021-10
            # - delete row: 2025-12 (soft_del_cd in {'1','4'})
            source_rows = [
                tu.build_source_row(acct, "2025-11", source_ts, balance=9200, actual_payment=920),
                tu.build_source_row(acct, "2025-10", source_ts, balance=9100, actual_payment=910),
                tu.build_source_row(acct, "2022-12", source_ts, balance=8200, actual_payment=820),
                tu.build_source_row(acct, "2021-10", source_ts, balance=8100, actual_payment=810),
                tu.build_source_row(acct, "2025-12", source_ts, balance=9300, actual_payment=930, soft_del_cd=delete_code),
            ]
            tu.write_source_rows(spark, config["source_table"], source_rows)

            module.ensure_soft_delete_columns(spark, config)
            module.preload_run_table_columns(spark, config)

            classified = module.load_and_classify_accounts(spark, config)
            case_iii_non_delete = classified.filter((F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete")))
            case_iii_delete = classified.filter((F.col("case_type") == "CASE_III") & (F.col("_is_soft_delete")))

            _assert_true(case_iii_non_delete.count() == 4, "Expected 4 non-delete CASE_III rows")
            _assert_true(case_iii_delete.count() == 1, "Expected 1 delete CASE_III row")

            split_df = case_iii_non_delete
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

            # Delete-month row should be flagged in summary.
            deleted_month_row = tu.fetch_single_row(spark, config["destination_table"], acct, "2025-12")
            _assert_true(
                str(deleted_month_row["soft_del_cd"]) == delete_code,
                f"Expected summary soft_del_cd={delete_code} for deleted month, got {deleted_month_row['soft_del_cd']}",
            )

            # Future month row should have deleted month position nullified (idx=1 for 2026-01 vs delete 2025-12).
            future_summary_row = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            future_latest_row = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")

            history_cols = [
                "actual_payment_am_history",
                "balance_am_history",
                "credit_limit_am_history",
                "past_due_am_history",
                "payment_rating_cd_history",
                "days_past_due_history",
                "asset_class_cd_4in_history",
            ]
            for col_name in history_cols:
                summary_hist = list(future_summary_row[col_name] or [])
                latest_hist = list(future_latest_row[col_name] or [])
                _assert_true(len(summary_hist) == 36, f"{col_name} summary length expected 36, got {len(summary_hist)}")
                _assert_true(len(latest_hist) == 72, f"{col_name} latest length expected 72, got {len(latest_hist)}")
                _assert_true(summary_hist[1] is None, f"{col_name} summary idx1 should be NULL after delete patch")
                _assert_true(latest_hist[1] is None, f"{col_name} latest idx1 should be NULL after delete patch")

            print(
                "[PASS] v4 single-key mixed Case III verified "
                f"(hot={hot_count}, cold={cold_count}, delete_code={delete_code}, account={acct})"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_single_key_hot_cold_delete_mix()
    print("[PASS] test_v4_case3_single_key_hot_cold_delete_mix.py")

