from datetime import datetime
from pathlib import Path
import sys

from tests.v4_contract_utils import DELETE_CODES, HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_null_update_case_iii_soft_delete():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_null_update_case_iii_soft_delete")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_null_update_case_iii_soft_delete")
    config = tu.load_main_test_config("v4_null_update_case_iii_soft_delete")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True

    try:
        module.cleanup(spark)
        for idx, delete_code in enumerate(DELETE_CODES):
            tu.reset_tables(spark, config)

            acct = 8301 + idx
            existing_ts = datetime(2026, 1, 1, 0, 0, 0)
            source_ts = datetime(2026, 2, 1, 12, 0, 0)

            existing_rows = [
                tu.build_summary_row(
                    acct,
                    "2025-12",
                    existing_ts,
                    balance=5300,
                    actual_payment=530,
                    balance_history=tu.history({0: 5300}),
                    payment_history=tu.history({0: 530}),
                    credit_history=tu.history({0: 10000}),
                    past_due_history=tu.history({0: 0}),
                    rating_history=tu.history({0: "0"}),
                    dpd_history=tu.history({0: 0}),
                    asset_history=tu.history({0: "A"}),
                ),
                tu.build_summary_row(
                    acct,
                    "2026-01",
                    existing_ts,
                    balance=5400,
                    actual_payment=540,
                    balance_history=tu.history({0: 5400, 1: 1111}),
                    payment_history=tu.history({0: 540, 1: 2222}),
                    credit_history=tu.history({0: 10000, 1: 3333}),
                    past_due_history=tu.history({0: 0, 1: 4444}),
                    rating_history=tu.history({0: "0", 1: "R"}),
                    dpd_history=tu.history({0: 0, 1: 5555}),
                    asset_history=tu.history({0: "A", 1: "Z"}),
                ),
            ]
            tu.write_summary_rows(spark, config["destination_table"], existing_rows)
            tu.write_summary_rows(spark, config["latest_history_table"], pad_latest_rows([existing_rows[-1]]))

            pre_deleted_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2025-12")

            # Soft-delete row with nullable payload fields (delete path should not overwrite arrays on delete month).
            source_rows = [
                tu.build_source_row(
                    cons_acct_key=acct,
                    rpt_as_of_mo="2025-12",
                    base_ts=source_ts,
                    balance=None,
                    actual_payment=None,
                    soft_del_cd=delete_code,
                )
            ]
            tu.write_source_rows(spark, config["source_table"], source_rows)

            module.run_pipeline(spark, config)

            deleted_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2025-12")
            future_summary = tu.fetch_single_row(spark, config["destination_table"], acct, "2026-01")
            future_latest = tu.fetch_single_row(spark, config["latest_history_table"], acct, "2026-01")

            _assert_true(str(deleted_summary["soft_del_cd"]) == delete_code, "deleted month soft_del mismatch")

            for col_name in HISTORY_COLS:
                pre_deleted_hist = list(pre_deleted_summary[col_name] or [])
                deleted_hist = list(deleted_summary[col_name] or [])
                future_summary_hist = list(future_summary[col_name] or [])
                future_latest_hist = list(future_latest[col_name] or [])

                _assert_true(
                    deleted_hist == pre_deleted_hist,
                    f"{col_name} deleted-month history should remain unchanged",
                )
                _assert_true(len(future_summary_hist) == 36, f"{col_name} summary length expected 36")
                _assert_true(len(future_latest_hist) == 72, f"{col_name} latest length expected 72")
                _assert_true(future_summary_hist[1] is None, f"{col_name} future summary idx1 should be NULL")
                _assert_true(future_latest_hist[0] is None, f"{col_name} future latest idx0 should be NULL")

            print(f"[PASS] null-update Case III soft-delete verified for delete_code={delete_code}, account={acct}")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_null_update_case_iii_soft_delete()
    print("[PASS] test_v4_null_update_case_iii_soft_delete.py")

