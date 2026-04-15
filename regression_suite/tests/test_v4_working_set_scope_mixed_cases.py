from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_working_set_scope_mixed_cases():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_workset_mixed_cases")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_workset_mixed_cases")
    config = tu.load_main_test_config("v4_workset_mixed_cases")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["use_working_set_latest_context"] = True
    config["use_working_set_case3_summary_context"] = True

    workset_latest_table = "temp_catalog.checkpointdb.workset_latest_summary"
    workset_case3_table = "temp_catalog.checkpointdb.workset_summary_case3"

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)

        acct_case1 = 9501
        acct_case2 = 9502
        acct_case3 = 9503
        acct_case4 = 9504
        acct_anchor = 9599

        existing_rows = [
            tu.build_summary_row(acct_case2, "2026-10", existing_ts, balance=5100, actual_payment=510),
            tu.build_summary_row(acct_case3, "2024-01", existing_ts, balance=6100, actual_payment=610),
            tu.build_summary_row(acct_case3, "2026-01", existing_ts, balance=6200, actual_payment=620),
            tu.build_summary_row(acct_anchor, "2026-12", existing_ts, balance=9100, actual_payment=910),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        latest_rows = [
            tu.build_summary_row(acct_case2, "2026-10", existing_ts, balance=5100, actual_payment=510),
            tu.build_summary_row(acct_case3, "2026-01", existing_ts, balance=6200, actual_payment=620),
            tu.build_summary_row(acct_anchor, "2026-12", existing_ts, balance=9100, actual_payment=910),
        ]
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(latest_rows))

        source_rows = [
            # CASE I
            tu.build_source_row(acct_case1, "2026-11", source_ts, balance=4100, actual_payment=410),
            # CASE II
            tu.build_source_row(acct_case2, "2026-11", source_ts, balance=5150, actual_payment=515),
            # CASE III
            tu.build_source_row(acct_case3, "2025-12", source_ts, balance=6250, actual_payment=625),
            # CASE IV (new account with multi-month batch)
            tu.build_source_row(acct_case4, "2026-09", source_ts, balance=7100, actual_payment=710),
            tu.build_source_row(acct_case4, "2026-10", source_ts, balance=7200, actual_payment=720),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        module.materialize_working_set_context_tables(spark, classified, config)

        _assert_true(spark.catalog.tableExists(workset_latest_table), "workset_latest_summary not created")
        _assert_true(spark.catalog.tableExists(workset_case3_table), "workset_summary_case3 not created")

        classified_keys = {
            int(r["cons_acct_key"])
            for r in classified.select("cons_acct_key").distinct().collect()
        }
        latest_keys = {
            int(r["cons_acct_key"])
            for r in spark.read.table(workset_latest_table).select("cons_acct_key").distinct().collect()
        }
        case3_keys = {
            int(r["cons_acct_key"])
            for r in classified.filter(F.col("case_type") == "CASE_III").select("cons_acct_key").distinct().collect()
        }
        case3_scope_keys = {
            int(r["cons_acct_key"])
            for r in spark.read.table(workset_case3_table).select("cons_acct_key").distinct().collect()
        }

        _assert_true(classified_keys.issubset(latest_keys), "workset_latest_summary missing classified accounts")
        _assert_true(case3_scope_keys == case3_keys, "workset_summary_case3 keys mismatch with CASE_III keys")

        print("[PASS] test_v4_working_set_scope_mixed_cases")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_working_set_scope_mixed_cases()
    print("[PASS] test_v4_working_set_scope_mixed_cases.py")

