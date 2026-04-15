"""
Idempotency check for one-time backfill v2 rerun behavior.
"""

from datetime import date, datetime
import json
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
    history,
    load_main_test_config,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


def _snapshot_table(spark, table_name: str) -> str:
    rows = (
        spark.table(table_name)
        .orderBy("cons_acct_key", "rpt_as_of_mo", "base_ts")
        .collect()
    )
    payload = [r.asDict(recursive=True) for r in rows]
    return json.dumps(payload, default=str, sort_keys=True)


def _write_hist_rows(spark, table, rows):
    if not rows:
        return
    df = spark.createDataFrame(
        rows,
        schema=(
            "cons_acct_key BIGINT, "
            "soft_del_cd STRING, "
            "acct_dt DATE, "
            "base_ts TIMESTAMP, "
            "insert_dt TIMESTAMP, "
            "update_dt TIMESTAMP, "
            "insert_time STRING, "
            "update_time STRING"
        ),
    )
    df.writeTo(table).append()


def run_test():
    spark = create_spark_session("one_time_backfill_idempotency_v2")
    config = load_main_test_config("main_one_time_backfill_idempotency_v2")
    try:
        reset_tables(spark, config)

        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)
        summary_rows = [
            build_summary_row(1401, "2026-01", seed_ts, balance=14010, actual_payment=140),
            build_summary_row(
                1401,
                "2026-02",
                seed_ts,
                balance=14020,
                actual_payment=141,
                balance_history=history({0: 14020, 1: 14010}),
                payment_history=history({0: 141, 1: 140}),
            ),
            build_summary_row(
                1401,
                "2026-03",
                seed_ts,
                balance=14030,
                actual_payment=142,
                balance_history=history({0: 14030, 1: 14020, 2: 14010}),
                payment_history=history({0: 142, 1: 141, 2: 140}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], summary_rows)
        write_summary_rows(spark, config["latest_history_table"], [summary_rows[-1]])

        src_rows = [
            build_source_row(1401, "2026-03", datetime(2026, 2, 15, 0, 0, 0), balance=14500, actual_payment=250, soft_del_cd=""),
            build_source_row(1401, "2026-01", datetime(2026, 2, 16, 0, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        src_rows[0]["acct_dt"] = date(2026, 3, 11)
        write_source_rows(spark, config["source_table"], src_rows)

        _write_hist_rows(
            spark,
            config["hist_rpt_dt_table"],
            [
                {
                    "cons_acct_key": 1401,
                    "soft_del_cd": "",
                    "acct_dt": date(2026, 3, 15),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "12:00:00",
                    "update_time": "12:30:00",
                }
            ],
        )

        backfill_job.run_backfill_v2(spark)
        summary_first = _snapshot_table(spark, config["destination_table"])
        latest_first = _snapshot_table(spark, config["latest_history_table"])

        backfill_job.run_backfill_v2(spark)
        summary_second = _snapshot_table(spark, config["destination_table"])
        latest_second = _snapshot_table(spark, config["latest_history_table"])

        if summary_first != summary_second:
            raise AssertionError("Summary drifted on rerun")
        if latest_first != latest_second:
            raise AssertionError("Latest summary drifted on rerun")

        print("[PASS] one_time_backfill_idempotency_v2")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

