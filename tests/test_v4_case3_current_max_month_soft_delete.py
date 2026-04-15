from datetime import datetime
from pathlib import Path
import sys

from tests.v4_contract_utils import DELETE_CODES, HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_current_max_month_soft_delete():
    repo_root = Path(__file__).resolve().parents[3]
    module = load_v4_as_summary_inc("_summary_inc_v4_case3_current_max_month_soft_delete")

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case3_current_max_month_soft_delete")
    config = tu.load_main_test_config("v4_case3_current_max_month_soft_delete")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True

    try:
        module.cleanup(spark)
        for idx, delete_code in enumerate(DELETE_CODES):
            tu.reset_tables(spark, config)

            acct = 8110 + idx
            existing_ts = datetime(2026, 1, 1, 0, 0, 0)
            source_ts = datetime(2026, 2, 15, 0, 0, 0)

            existing_row = tu.build_summary_row(
                cons_acct_key=acct,
                rpt_as_of_mo="2026-01",
                base_ts=existing_ts,
                balance=5000,
                actual_payment=500,
                balance_history=tu.history({0: 5000, 1: 77701}),
                payment_history=tu.history({0: 500, 1: 77702}),
                credit_history=tu.history({0: 10000, 1: 77703}),
                past_due_history=tu.history({0: 0, 1: 77704}),
                rating_history=tu.history({0: "0", 1: "R"}),
                dpd_history=tu.history({0: 0, 1: 77705}),
                asset_history=tu.history({0: "A", 1: "Q"}),
            )
            tu.write_summary_rows(spark, config["destination_table"], [existing_row])
            tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows([existing_row]))

            pre_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            pre_latest = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")

            source_row = tu.build_source_row(
                cons_acct_key=acct,
                rpt_as_of_mo="2026-01",
                base_ts=source_ts,
                balance=5100,
                actual_payment=510,
                soft_del_cd=delete_code,
            )
            tu.write_source_rows(spark, config["source_table"], [source_row])

            module.run_pipeline(spark, config)
            tu.assert_watermark_tracker_consistent(spark, config)

            post_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            post_latest = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")

            _assert_true(str(post_summary["soft_del_cd"]) == delete_code, "summary soft_del_cd mismatch")
            _assert_true(str(post_latest["soft_del_cd"]) == delete_code, "latest_summary soft_del_cd mismatch")
            _assert_true(post_summary["base_ts"] == source_ts, "summary base_ts should be updated for delete month")
            _assert_true(post_latest["base_ts"] == source_ts, "latest_summary base_ts should be updated for delete month")

            for col_name in HISTORY_COLS:
                pre_summary_hist = list(pre_summary[col_name] or [])
                post_summary_hist = list(post_summary[col_name] or [])
                post_latest_hist = list(post_latest[col_name] or [])

                _assert_true(len(post_summary_hist) == 36, f"{col_name} summary length expected 36")
                _assert_true(len(post_latest_hist) == 72, f"{col_name} latest length expected 72")
                _assert_true(
                    post_summary_hist == pre_summary_hist,
                    f"{col_name} summary should remain unchanged on delete month",
                )

            print(f"[PASS] current-max-month Case III soft delete verified for code={delete_code}, account={acct}")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case3_current_max_month_soft_delete()
    print("[PASS] test_v4_case3_current_max_month_soft_delete.py")
