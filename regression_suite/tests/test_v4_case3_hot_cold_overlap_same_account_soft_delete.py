from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import DELETE_CODES, HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_hot_cold_overlap_same_account_soft_delete():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_hot_cold_overlap_same_account_soft_delete")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_hot_cold_overlap_same_account_soft_delete")
    config = tu.load_main_test_config("v4_case3_hot_cold_overlap_same_account_soft_delete")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36
    config["force_cold_case3d_broadcast"] = True
    config["cold_case3d_broadcast_row_cap"] = 10_000_000

    try:
        module.cleanup(spark)
        for idx, delete_code in enumerate(DELETE_CODES):
            tu.reset_tables(spark, config)

            acct = 7201 + idx
            existing_ts = datetime(2026, 1, 1, 0, 0, 0)
            source_ts = datetime(2026, 2, 1, 12, 0, 0)

            existing_rows = [
                tu.build_summary_row(acct, "2022-12", existing_ts, balance=2200, actual_payment=220),
                tu.build_summary_row(acct, "2023-01", existing_ts, balance=2300, actual_payment=230),
                tu.build_summary_row(acct, "2025-11", existing_ts, balance=5200, actual_payment=520),
                tu.build_summary_row(acct, "2025-12", existing_ts, balance=5300, actual_payment=530),
                tu.build_summary_row(
                    acct,
                    "2026-01",
                    existing_ts,
                    balance=5400,
                    actual_payment=540,
                    balance_history=tu.history({0: 5400, 1: 901, 2: 902, 12: 912, 13: 913}),
                    payment_history=tu.history({0: 540, 1: 801, 2: 802, 12: 812, 13: 813}),
                    credit_history=tu.history({0: 10000, 1: 701, 2: 702, 12: 712, 13: 713}),
                    past_due_history=tu.history({0: 0, 1: 601, 2: 602, 12: 612, 13: 613}),
                    rating_history=tu.history({0: "0", 1: "R1", 2: "R2", 12: "R12", 13: "R13"}),
                    dpd_history=tu.history({0: 0, 1: 501, 2: 502, 12: 512, 13: 513}),
                    asset_history=tu.history({0: "A", 1: "B1", 2: "B2", 12: "B12", 13: "B13"}),
                ),
            ]
            tu.write_summary_rows(spark, config["destination_table"], existing_rows)
            tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows([existing_rows[-1]]))

            # Same account has both hot and cold soft-delete rows.
            source_rows = [
                tu.build_source_row(acct, "2025-12", source_ts, balance=9300, actual_payment=930, soft_del_cd=delete_code),
                tu.build_source_row(acct, "2025-11", source_ts, balance=9200, actual_payment=920, soft_del_cd=delete_code),
                tu.build_source_row(acct, "2023-01", source_ts, balance=8300, actual_payment=830, soft_del_cd=delete_code),
                tu.build_source_row(acct, "2022-12", source_ts, balance=8200, actual_payment=820, soft_del_cd=delete_code),
            ]
            tu.write_source_rows(spark, config["source_table"], source_rows)

            module.ensure_soft_delete_columns(spark, config)
            module.preload_run_table_columns(spark, config)

            classified = module.load_and_classify_accounts(spark, config)
            case_iii_delete = classified.filter((F.col("case_type") == "CASE_III") & (F.col("_is_soft_delete")))
            _assert_true(case_iii_delete.count() == 4, "Expected 4 CASE_III soft-delete rows")

            split_df = case_iii_delete
            if "month_int" not in split_df.columns:
                split_df = split_df.withColumn("month_int", F.expr(module.month_to_int_expr(config["partition_column"])))
            latest_month = split_df.agg(F.max("max_existing_month").alias("m")).first()["m"]
            latest_int = int(latest_month[:4]) * 12 + int(latest_month[5:7])
            hot_cutoff_int = latest_int - (int(config["case3_hot_window_months"]) - 1)
            split_counts = split_df.agg(
                F.sum(F.when(F.col("month_int") >= F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
                F.sum(F.when(F.col("month_int") < F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
            ).first()
            hot_count = int(split_counts["hot_count"] or 0)
            cold_count = int(split_counts["cold_count"] or 0)
            _assert_true(hot_count == 2, f"Expected 2 hot rows, got {hot_count}")
            _assert_true(cold_count == 2, f"Expected 2 cold rows, got {cold_count}")

            module.run_pipeline(spark, config)

            for mo in ["2025-12", "2025-11", "2023-01", "2022-12"]:
                row = tu.fetch_single_row(spark, config["destination_table"], acct, mo)
                _assert_true(str(row["soft_del_cd"]) == delete_code, f"{mo} soft_del should be {delete_code}")

            future_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            future_latest = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")
            expected_null_indexes = [1, 2]

            for col_name in HISTORY_COLS:
                summary_hist = list(future_summary[col_name] or [])
                latest_hist = list(future_latest[col_name] or [])
                _assert_true(len(summary_hist) == 36, f"{col_name} summary length expected 36")
                _assert_true(len(latest_hist) == 72, f"{col_name} latest length expected 72")
                for idx_pos in expected_null_indexes:
                    _assert_true(summary_hist[idx_pos] is None, f"{col_name} summary idx={idx_pos} should be NULL")
                _assert_true(latest_hist[0] is None, f"{col_name} latest idx0 should be NULL after delete context patch")

            print(
                "[PASS] Case III soft-delete hot/cold overlap verified "
                f"(delete_code={delete_code}, hot={hot_count}, cold={cold_count}, account={acct})"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_hot_cold_overlap_same_account_soft_delete()
    print("[PASS] test_v4_case3_hot_cold_overlap_same_account_soft_delete.py")

