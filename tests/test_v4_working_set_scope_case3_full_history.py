from datetime import datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_working_set_scope_case3_full_history():
    repo_root = Path(__file__).resolve().parents[3]
    module = load_v4_as_summary_inc("_summary_inc_v4_workset_case3_full_history")

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_workset_case3_full_history")
    config = tu.load_main_test_config("v4_workset_case3_full_history")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["use_working_set_latest_context"] = True
    config["use_working_set_case3_summary_context"] = True

    workset_case3_table = "temp_catalog.checkpointdb.workset_summary_case3"

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)
        acct_case3 = 9401
        acct_anchor = 9402

        existing_rows = [
            tu.build_summary_row(acct_case3, "2020-01", existing_ts, balance=3100, actual_payment=310),
            tu.build_summary_row(acct_case3, "2023-01", existing_ts, balance=3200, actual_payment=320),
            tu.build_summary_row(acct_case3, "2024-01", existing_ts, balance=3300, actual_payment=330, soft_del_cd="4"),
            tu.build_summary_row(acct_case3, "2026-01", existing_ts, balance=3400, actual_payment=340),
            tu.build_summary_row(acct_anchor, "2026-12", existing_ts, balance=8000, actual_payment=800),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)

        latest_rows = [
            row for row in existing_rows if row["rpt_as_of_mo"] in {"2026-01", "2026-12"}
        ]
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(latest_rows))

        source_rows = [
            tu.build_source_row(acct_case3, "2025-12", source_ts, balance=3410, actual_payment=341),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.ensure_soft_delete_columns(spark, config)
        module.preload_run_table_columns(spark, config)
        classified = module.load_and_classify_accounts(spark, config)
        module.materialize_working_set_context_tables(spark, classified, config)

        _assert_true(spark.catalog.tableExists(workset_case3_table), "workset_summary_case3 not created")
        case3_months = {
            r["rpt_as_of_mo"]
            for r in spark.read.table(workset_case3_table)
            .filter(F.col("cons_acct_key") == acct_case3)
            .select("rpt_as_of_mo")
            .collect()
        }
        expected_months = {"2020-01", "2023-01", "2024-01", "2026-01"}
        _assert_true(case3_months == expected_months, f"Full-history case3 scope mismatch: {case3_months}")

        print("[PASS] test_v4_working_set_scope_case3_full_history")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_working_set_scope_case3_full_history()
    print("[PASS] test_v4_working_set_scope_case3_full_history.py")
