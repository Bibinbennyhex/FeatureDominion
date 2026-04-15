from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_hot_and_cold_split():
    repo_root = Path(__file__).resolve().parents[3]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_hot_and_cold_split")

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_hot_and_cold_split")
    config = tu.load_main_test_config("v4_case3_hot_and_cold_split")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        # Existing accounts with latest month at 2026-01.
        # Add one older month anchor row so source month filter keeps cold rows too.
        existing_rows = [
            tu.build_summary_row(
                cons_acct_key=9001,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=5000,
                actual_payment=500,
            ),
            tu.build_summary_row(
                cons_acct_key=9002,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=7000,
                actual_payment=700,
            ),
            tu.build_summary_row(
                cons_acct_key=9999,
                rpt_as_of_mo="2020-01",
                base_ts=existing_ts,
                balance=100,
                actual_payment=10,
            ),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(existing_rows))

        # Case III input:
        # - 9001 @ 2025-12 => HOT (within last 36 months from 2026-01)
        # - 9002 @ 2022-12 => COLD (older than 36 months from 2026-01)
        source_rows = [
            tu.build_source_row(9001, "2025-12", source_ts, balance=5100, actual_payment=510),
            tu.build_source_row(9002, "2022-12", source_ts, balance=7100, actual_payment=710),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)
        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)

        classified = module.load_and_classify_accounts(spark, config)
        case_iii_df = classified.filter((F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete")))

        _assert_true(case_iii_df.count() == 2, "Expected exactly 2 CASE_III records")

        prt = config["partition_column"]
        hot_window = int(config.get("case3_hot_window_months", 36))
        split_df = case_iii_df
        if "month_int" not in split_df.columns:
            split_df = split_df.withColumn("month_int", F.expr(module.month_to_int_expr(prt)))

        global_latest_month = (
            split_df
            .agg(F.max("max_existing_month").alias("global_latest_month"))
            .first()["global_latest_month"]
        )
        _assert_true(global_latest_month is not None, "global_latest_month should not be null")

        latest_int = int(global_latest_month[:4]) * 12 + int(global_latest_month[5:7])
        hot_cutoff_int = latest_int - (hot_window - 1)
        split_counts = (
            split_df
            .agg(
                F.sum(F.when(F.col("month_int") >= F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
                F.sum(F.when(F.col("month_int") < F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
            )
            .first()
        )
        hot_count = int(split_counts["hot_count"] or 0)
        cold_count = int(split_counts["cold_count"] or 0)

        _assert_true(hot_count > 0, "Expected at least one hot CASE_III row")
        _assert_true(cold_count > 0, "Expected at least one cold CASE_III row")

        # Execute Case III to ensure both lanes are processable end-to-end.
        module.process_case_iii(spark, case_iii_df, config, expected_rows=2)

        print(
            "[PASS] v4 hot/cold split verified "
            f"(latest_month={global_latest_month}, hot_rows={hot_count}, cold_rows={cold_count})"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_hot_and_cold_split()
    print("[PASS] test_v4_case3_hot_and_cold_split.py")
