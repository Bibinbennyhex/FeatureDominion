from datetime import datetime
from pathlib import Path
import sys

from tests.v4_contract_utils import load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_working_set_scope_case2_only():
    repo_root = Path(__file__).resolve().parents[3]
    module = load_v4_as_summary_inc("_summary_inc_v4_workset_case2_only")

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_workset_case2_only")
    config = tu.load_main_test_config("v4_workset_case2_only")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["use_working_set_latest_context"] = True
    config["use_working_set_case3_summary_context"] = True

    workset_latest_table = "execution_catalog.checkpointdb.workset_latest_summary"
    workset_case3_table = "execution_catalog.checkpointdb.workset_summary_case3"

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)

        existing_ts = datetime(2026, 1, 1, 0, 0, 0)
        source_ts = datetime(2026, 2, 1, 12, 0, 0)
        acct_case2 = 9301
        acct_anchor = 9302

        existing_rows = [
            tu.build_summary_row(acct_case2, "2026-10", existing_ts, balance=5100, actual_payment=510),
            tu.build_summary_row(acct_anchor, "2026-12", existing_ts, balance=7100, actual_payment=710),
        ]
        tu.write_summary_rows(spark, config["destination_table"], existing_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows(existing_rows))

        source_rows = [
            tu.build_source_row(acct_case2, "2026-11", source_ts, balance=5150, actual_payment=515),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.run_pipeline(spark, config)

        out_row = tu.fetch_single_row(spark, config["destination_table"], acct_case2, "2026-11")
        _assert_true(int(out_row["balance_am"]) == 5150, "Case II row not merged as expected")
        _assert_true(out_row["base_ts"] == source_ts, "Case II base_ts mismatch")

        _assert_true(spark.catalog.tableExists(workset_latest_table), "workset_latest_summary not created")
        _assert_true(spark.catalog.tableExists(workset_case3_table), "workset_summary_case3 not created")

        workset_latest_keys = {
            int(r["cons_acct_key"])
            for r in spark.read.table(workset_latest_table).select("cons_acct_key").distinct().collect()
        }
        _assert_true(acct_case2 in workset_latest_keys, "Case II account missing in workset_latest_summary")

        case3_count = spark.read.table(workset_case3_table).count()
        _assert_true(case3_count == 0, f"Expected empty case3 workset for Case-II-only run, got {case3_count}")

        print("[PASS] test_v4_working_set_scope_case2_only")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_working_set_scope_case2_only()
    print("[PASS] test_v4_working_set_scope_case2_only.py")
