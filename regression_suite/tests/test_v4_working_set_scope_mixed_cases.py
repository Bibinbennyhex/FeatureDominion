from datetime import datetime
from pathlib import Path
import sys

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

    try:
        module.cleanup(spark)
        tu.reset_tables(spark, config)
        spark.sql("DROP TABLE IF EXISTS execution_catalog.checkpointdb.workset_latest_summary")
        spark.sql("DROP TABLE IF EXISTS execution_catalog.checkpointdb.workset_summary_case3")

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
            tu.build_source_row(acct_case1, "2026-11", source_ts, balance=4100, actual_payment=410),
            tu.build_source_row(acct_case2, "2026-11", source_ts, balance=5150, actual_payment=515),
            tu.build_source_row(acct_case3, "2025-12", source_ts, balance=6250, actual_payment=625),
            tu.build_source_row(acct_case4, "2026-09", source_ts, balance=7100, actual_payment=710),
            tu.build_source_row(acct_case4, "2026-10", source_ts, balance=7200, actual_payment=720),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.run_pipeline(spark, config)

        _assert_true(int(tu.fetch_single_row(spark, config["destination_table"], acct_case1, "2026-11")["balance_am"]) == 4100, "CASE I row missing or incorrect")
        _assert_true(int(tu.fetch_single_row(spark, config["destination_table"], acct_case2, "2026-11")["balance_am"]) == 5150, "CASE II row missing or incorrect")
        _assert_true(int(tu.fetch_single_row(spark, config["destination_table"], acct_case3, "2025-12")["balance_am"]) == 6250, "CASE III backfill row missing or incorrect")
        _assert_true(int(tu.fetch_single_row(spark, config["destination_table"], acct_case4, "2026-09")["balance_am"]) == 7100, "CASE IV base row missing or incorrect")
        _assert_true(int(tu.fetch_single_row(spark, config["destination_table"], acct_case4, "2026-10")["balance_am"]) == 7200, "CASE IV historical row missing or incorrect")

        latest_case2 = spark.table(config["latest_history_table"]).where("cons_acct_key = 9502").first()
        latest_case4 = spark.table(config["latest_history_table"]).where("cons_acct_key = 9504").first()
        _assert_true(latest_case2 is not None and latest_case2["rpt_as_of_mo"] == "2026-11", "CASE II latest_summary month mismatch")
        _assert_true(latest_case4 is not None and latest_case4["rpt_as_of_mo"] == "2026-10", "CASE IV latest_summary month mismatch")

        _assert_true(
            not spark.catalog.tableExists("execution_catalog.checkpointdb.workset_latest_summary"),
            "workset_latest_summary should not be created",
        )
        _assert_true(
            not spark.catalog.tableExists("execution_catalog.checkpointdb.workset_summary_case3"),
            "workset_summary_case3 should not be created",
        )

        print("[PASS] test_v4_working_set_scope_mixed_cases")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_working_set_scope_mixed_cases()
    print("[PASS] test_v4_working_set_scope_mixed_cases.py")
