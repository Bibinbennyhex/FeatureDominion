"""
Delete matrix validation for one-time backfill v2:
1) past one month delete
2) latest month delete
3) continuous multi-delete
4) non-continuous multi-delete
"""

from datetime import datetime
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import backfill_soft_delete_from_accounts_v2 as backfill_job
from test_utils import (
    build_source_row,
    build_summary_row,
    create_spark_session,
    fetch_single_row,
    history,
    load_main_test_config,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


def _assert_eq(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def _seed_account_rows(acct: int, base_ts: datetime):
    rows = []
    for i, mo in enumerate(["2026-03", "2026-04", "2026-05", "2026-06"], 1):
        bal = acct * 10 + i
        rows.append(
            build_summary_row(
                acct,
                mo,
                base_ts,
                balance=bal,
                actual_payment=100 + i,
                balance_history=history({0: bal, 1: bal - 1, 2: bal - 2, 3: bal - 3}),
                payment_history=history({0: 100 + i, 1: 99 + i, 2: 98 + i, 3: 97 + i}),
            )
        )
    return rows


def run_test():
    spark = create_spark_session("one_time_backfill_delete_matrix_v2")
    config = load_main_test_config("main_one_time_backfill_delete_matrix_v2")
    try:
        reset_tables(spark, config)
        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)
        rows = []
        latest_rows = []
        for acct in [9201, 9202, 9203, 9204]:
            acct_rows = _seed_account_rows(acct, seed_ts)
            rows.extend(acct_rows)
            latest_rows.append(acct_rows[-1])
        write_summary_rows(spark, config["destination_table"], rows)
        write_summary_rows(spark, config["latest_history_table"], latest_rows)

        delete_ts = datetime(2026, 2, 15, 0, 0, 0)
        write_source_rows(
            spark,
            config["source_table"],
            [
                build_source_row(9201, "2026-04", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
                build_source_row(9202, "2026-06", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
                build_source_row(9203, "2026-03", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
                build_source_row(9203, "2026-04", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
                build_source_row(9204, "2026-03", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
                build_source_row(9204, "2026-05", delete_ts, balance=1, actual_payment=1, soft_del_cd="4"),
            ],
        )

        backfill_job.run_backfill_v2(spark)

        # 1) Past one month delete: 2026-04 should null index 2 in 2026-06 row
        a = fetch_single_row(spark, config["latest_history_table"], 9201, "2026-06")
        _assert_eq(a["balance_am_history"][2], None, "9201 past delete position")

        # 2) Latest month delete: latest_summary should reconstruct to 2026-05
        b = fetch_single_row(spark, config["latest_history_table"], 9202, "2026-05")
        _assert_eq(b["rpt_as_of_mo"], "2026-05", "9202 latest-month reconstruction")

        # 3) Continuous multi-delete: 2026-03,2026-04 -> null indices 3 and 2 in 2026-06
        c = fetch_single_row(spark, config["latest_history_table"], 9203, "2026-06")
        _assert_eq(c["balance_am_history"][3], None, "9203 continuous delete index 3")
        _assert_eq(c["balance_am_history"][2], None, "9203 continuous delete index 2")

        # 4) Non-continuous multi-delete: 2026-03,2026-05 -> null indices 3 and 1 in 2026-06
        d = fetch_single_row(spark, config["latest_history_table"], 9204, "2026-06")
        _assert_eq(d["balance_am_history"][3], None, "9204 non-continuous delete index 3")
        _assert_eq(d["balance_am_history"][1], None, "9204 non-continuous delete index 1")

        _assert_eq(fetch_single_row(spark, config["destination_table"], 9201, "2026-04")["soft_del_cd"], "4", "9201 delete month flag")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 9203, "2026-03")["soft_del_cd"], "4", "9203 delete month flag 1")
        _assert_eq(fetch_single_row(spark, config["destination_table"], 9203, "2026-04")["soft_del_cd"], "4", "9203 delete month flag 2")

        print("[PASS] one_time_backfill_delete_matrix_v2")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

