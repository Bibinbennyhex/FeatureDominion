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


def test_v4_case3_soft_delete_account_level_lane_routing():
    repo_root = Path(__file__).resolve().parents[3]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_soft_delete_account_level_lane_routing")

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_soft_delete_account_level_lane_routing")
    config = tu.load_main_test_config("v4_case3_soft_delete_account_level_lane_routing")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36
    config["force_case3d_unified_on_any_overlap"] = False
    config["force_cold_case3d_broadcast"] = True
    config["cold_case3d_broadcast_row_cap"] = 10_000_000

    cold_acct = 9201
    hot_acct = 9202
    mixed_acct = 9203
    anchor_acct = 999902

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        existing_rows = [
            tu.build_summary_row(cold_acct, "2021-10", existing_ts, balance=4100, actual_payment=410),
            tu.build_summary_row(cold_acct, "2026-01", existing_ts, balance=4150, actual_payment=415),
            tu.build_summary_row(hot_acct, "2025-12", existing_ts, balance=5100, actual_payment=510),
            tu.build_summary_row(hot_acct, "2026-01", existing_ts, balance=5150, actual_payment=515),
            tu.build_summary_row(mixed_acct, "2021-11", existing_ts, balance=6100, actual_payment=610),
            tu.build_summary_row(mixed_acct, "2025-11", existing_ts, balance=6200, actual_payment=620),
            tu.build_summary_row(mixed_acct, "2026-01", existing_ts, balance=6250, actual_payment=625),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)

        latest_rows = [
            r for r in existing_rows if r["rpt_as_of_mo"] == "2026-01"
        ]
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(latest_rows))

        source_rows = [
            # cold-only delete
            tu.build_source_row(cold_acct, "2021-10", source_ts, balance=4110, actual_payment=411, soft_del_cd="4"),
            # hot-only delete
            tu.build_source_row(hot_acct, "2025-12", source_ts, balance=5120, actual_payment=512, soft_del_cd="4"),
            # mixed delete (hot+cold)
            tu.build_source_row(mixed_acct, "2025-11", source_ts, balance=6230, actual_payment=623, soft_del_cd="4"),
            tu.build_source_row(mixed_acct, "2021-11", source_ts, balance=6240, actual_payment=624, soft_del_cd="4"),
            # anchor month so source month-window filter keeps hot/cold test rows
            tu.build_source_row(anchor_acct, "2026-02", source_ts, balance=1000, actual_payment=100, soft_del_cd=""),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        case_iii_delete_df = classified.filter(
            (F.col("case_type") == "CASE_III")
            & (F.col("_is_soft_delete"))
            & (F.col("cons_acct_key").isin(cold_acct, hot_acct, mixed_acct))
        )
        _assert_true(case_iii_delete_df.count() == 4, "Expected 4 soft-delete Case III rows")

        module.process_case_iii_soft_delete(spark, case_iii_delete_df, config, expected_rows=4)

        _assert_true(
            spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_latest_history_context_patch"),
            "Expected latest-history soft-delete patch table to exist",
        )
        history_patch_keys = _table_key_set(
            spark,
            "temp_catalog.checkpointdb.case_3d_latest_history_context_patch",
        )
        _assert_true(hot_acct in history_patch_keys, "Hot-only delete account should be in context patch output")
        _assert_true(cold_acct not in history_patch_keys, "Cold-only delete account should be excluded from context patch")
        _assert_true(
            mixed_acct not in history_patch_keys,
            "Mixed delete account should be excluded from case_3d_latest_history_context_patch",
        )

        # Latest soft-delete month flags for latest_summary are derived from case_3d_month where rpt_as_of_mo matches latest month.
        latest_month = (
            spark.read.table(config["latest_history_table"])
            .agg(F.max(F.col("rpt_as_of_mo")).alias("m"))
            .first()["m"]
        )
        latest_month_delete_keys = (
            set()
            if not spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_month")
            else {
                int(r["cons_acct_key"])
                for r in (
                    spark.read.table("temp_catalog.checkpointdb.case_3d_month")
                    .filter(F.col("rpt_as_of_mo") == F.lit(latest_month))
                    .select("cons_acct_key")
                    .distinct()
                    .collect()
                )
            }
        )
        _assert_true(
            mixed_acct not in latest_month_delete_keys,
            "Mixed delete account should be excluded from latest soft-delete month patch keys",
        )

        module.write_backfill_results(spark, config)

        for acct, month in [
            (cold_acct, "2021-10"),
            (hot_acct, "2025-12"),
            (mixed_acct, "2025-11"),
            (mixed_acct, "2021-11"),
        ]:
            row = tu.fetch_single_row(spark, config["destination_table"], acct, month)
            _assert_true(str(row["soft_del_cd"]) == "4", f"{acct}/{month} soft_del_cd should be 4")

        # Hot delete should null future position in 2026-01 for hot-only account.
        hot_future_summary = tu.fetch_single_row(spark, config["destination_table"], hot_acct, "2026-01")
        hot_future_latest = tu.fetch_single_row(spark, config["latest_history_table"], hot_acct, "2026-01")
        for col_name in HISTORY_COLS:
            summary_hist = list(hot_future_summary[col_name] or [])
            latest_hist = list(hot_future_latest[col_name] or [])
            _assert_true(len(summary_hist) == 36, f"{col_name} summary length expected 36")
            _assert_true(len(latest_hist) == 72, f"{col_name} latest length expected 72")
            _assert_true(summary_hist[1] is None, f"{col_name} summary idx1 should be NULL")
            _assert_true(latest_hist[0] is None, f"{col_name} latest idx0 should be NULL")

        print(
            "[PASS] Case III soft-delete account-level lane routing verified "
            f"(cold_only={cold_acct}, hot_only={hot_acct}, mixed={mixed_acct})"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_soft_delete_account_level_lane_routing()
    print("[PASS] test_v4_case3_soft_delete_account_level_lane_routing.py")
