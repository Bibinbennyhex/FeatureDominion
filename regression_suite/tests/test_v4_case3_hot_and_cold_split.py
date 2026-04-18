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


def test_v4_case3_hot_and_cold_split():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_hot_and_cold_split")

    tests_dir = Path(__file__).resolve().parent
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
                cons_acct_key=9003,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=8000,
                actual_payment=800,
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

        pre_summary_untouched = _snapshot_row_arrays(
            tu, spark, config["destination_table"], 9999, "2020-01"
        )
        pre_latest_untouched = _snapshot_row_arrays(
            tu, spark, config["latest_history_table"], 9999, "2020-01"
        )
        pre_latest_9001 = _snapshot_row_arrays(
            tu, spark, config["latest_history_table"], 9001, "2026-01"
        )
        pre_latest_9003 = _snapshot_row_arrays(
            tu, spark, config["latest_history_table"], 9003, "2026-01"
        )

        # Case III input:
        # - 9001 @ 2025-12 => HOT (diff=1)
        # - 9003 @ 2023-01 => HOT boundary (diff=36, inclusive)
        # - 9002 @ 2022-12 => COLD (diff=37)
        source_rows = [
            tu.build_source_row(9001, "2025-12", source_ts, balance=5100, actual_payment=510),
            tu.build_source_row(9002, "2022-12", source_ts, balance=7100, actual_payment=710),
            tu.build_source_row(9003, "2023-01", source_ts, balance=8100, actual_payment=810),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)
        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)

        classified = module.load_and_classify_accounts(spark, config)
        case_iii_df = classified.filter((F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete")))

        _assert_true(case_iii_df.count() == 3, "Expected exactly 3 CASE_III records")

        hot_window = int(config.get("case3_hot_window_months", 36))
        split_df = case_iii_df
        if "month_int" not in split_df.columns:
            split_df = split_df.withColumn("month_int", F.expr(module.month_to_int_expr(config["partition_column"])))
        split_df = split_df.withColumn("case3_month_diff", F.col("max_month_int") - F.col("month_int"))
        split_counts = (
            split_df
            .agg(
                F.sum(F.when(F.col("case3_month_diff") <= F.lit(hot_window), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
                F.sum(F.when(F.col("case3_month_diff") > F.lit(hot_window), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
            )
            .first()
        )
        hot_count = int(split_counts["hot_count"] or 0)
        cold_count = int(split_counts["cold_count"] or 0)

        _assert_true(hot_count == 2, f"Expected 2 hot CASE_III rows, got {hot_count}")
        _assert_true(cold_count == 1, f"Expected 1 cold CASE_III row, got {cold_count}")

        boundary_row = split_df.filter(F.col("cons_acct_key") == F.lit(9003)).select("case3_month_diff").first()
        _assert_true(int(boundary_row["case3_month_diff"]) == 36, "Boundary row should be diff=36")

        module.run_pipeline(spark, config)

        post_summary_untouched = _snapshot_row_arrays(
            tu, spark, config["destination_table"], 9999, "2020-01"
        )
        post_latest_untouched = _snapshot_row_arrays(
            tu, spark, config["latest_history_table"], 9999, "2020-01"
        )
        _assert_true(
            post_summary_untouched == pre_summary_untouched,
            "Untouched summary key changed unexpectedly for acct=9999 month=2020-01",
        )
        _assert_true(
            post_latest_untouched == pre_latest_untouched,
            "Untouched latest_summary key changed unexpectedly for acct=9999 month=2020-01",
        )

        hot_summary_9001 = tu.fetch_single_row(spark, config["destination_table"], 9001, "2025-12")
        hot_summary_9003 = tu.fetch_single_row(spark, config["destination_table"], 9003, "2023-01")
        _assert_true(int(hot_summary_9001["balance_am_history"][0]) == 5100, "acct=9001 summary idx0 mismatch")
        _assert_true(int(hot_summary_9003["balance_am_history"][0]) == 8100, "acct=9003 summary idx0 mismatch")

        post_latest_9001 = _snapshot_row_arrays(tu, spark, config["latest_history_table"], 9001, "2026-01")
        post_latest_9003 = _snapshot_row_arrays(tu, spark, config["latest_history_table"], 9003, "2026-01")
        _assert_true(pre_latest_9001 is not None and post_latest_9001 is not None, "Missing latest row for acct=9001")
        _assert_true(pre_latest_9003 is not None and post_latest_9003 is not None, "Missing latest row for acct=9003")
        _assert_true(
            post_latest_9001["balance_am_history"][1] == 5100,
            "acct=9001 latest_summary balance history idx1 should be 5100",
        )
        _assert_true(
            post_latest_9003["balance_am_history"][36] == 8100,
            "acct=9003 latest_summary balance history idx36 should be 8100",
        )
        _assert_true(
            pre_latest_9001 != post_latest_9001 and pre_latest_9003 != post_latest_9003,
            "Expected touched latest_summary rows to change",
        )

        print(
            "[PASS] v4 hot/cold split verified "
            f"(hot_rows={hot_count}, cold_rows={cold_count}, inclusive_boundary=36)"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_hot_and_cold_split()
    print("[PASS] test_v4_case3_hot_and_cold_split.py")

