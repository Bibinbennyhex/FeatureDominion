"""
Test for standalone latest summary builder from summary.
Validates bounded + unbounded histories and payment grid length behavior.
"""

from datetime import datetime
import os
import sys

from pyspark.sql import functions as F

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

from generate_latest_summary_from_summary import generate_latest_summary_from_summary
from test_utils import (
    build_summary_row,
    create_spark_session,
    fetch_single_row,
    load_main_test_config,
    reset_tables,
    write_summary_rows,
)


def _add_months(year: int, month: int, delta: int):
    total = (year * 12 + (month - 1)) + delta
    return total // 12, (total % 12) + 1


def _ym(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def run_test():
    spark = create_spark_session("test_generate_latest_summary_from_summary")
    config = load_main_test_config("main_generate_latest_summary_from_summary")

    try:
        reset_tables(spark, config)

        # Account 7001: sparse months to validate explicit null gap in unbounded/bounded arrays.
        # Months present: 2026-01 and 2026-03 -> gap at 2026-02 should be NULL.
        rows = [
            build_summary_row(7001, "2026-01", datetime(2026, 1, 2, 0, 0, 0), balance=1000, actual_payment=100, payment_rating="1"),
            build_summary_row(7001, "2026-03", datetime(2026, 3, 2, 0, 0, 0), balance=3000, actual_payment=300, payment_rating="3"),
            # Duplicate month with newer base_ts to validate dedupe by max base_ts per account+month.
            build_summary_row(7001, "2026-03", datetime(2026, 3, 5, 0, 0, 0), balance=3333, actual_payment=333, payment_rating="3"),
        ]

        # Account 7002: 40 continuous months to validate bounded=36 and unbounded=40.
        # Range: 2023-01 .. 2026-04 (40 months).
        for i in range(40):
            y, m = _add_months(2023, 1, i)
            month = _ym(y, m)
            bal = 10000 + i
            pay = 200 + i
            rows.append(
                build_summary_row(
                    7002,
                    month,
                    datetime(y, m, 2, 0, 0, 0),
                    balance=bal,
                    actual_payment=pay,
                    payment_rating="0",
                )
            )

        write_summary_rows(spark, config["destination_table"], rows)

        output_table = f"{config['latest_history_table']}_from_summary_test"
        spark.sql(f"DROP TABLE IF EXISTS {output_table}")

        generate_latest_summary_from_summary(spark, config, output_table)

        if not spark.catalog.tableExists(output_table):
            raise AssertionError(f"Output table was not created: {output_table}")

        out_count = spark.table(output_table).count()
        if out_count != 2:
            raise AssertionError(f"Expected 2 latest rows in output, got {out_count}")

        r7001 = fetch_single_row(spark, output_table, 7001, "2026-03")
        r7002 = fetch_single_row(spark, output_table, 7002, "2026-04")

        # 7001: [2026-03, 2026-02, 2026-01] => [3333, None, 1000]
        b_ub = r7001["balance_am_history_unbounded"]
        if not (len(b_ub) >= 3 and b_ub[0] == 3333 and b_ub[1] is None and b_ub[2] == 1000):
            raise AssertionError(f"Unexpected 7001 unbounded balance history: {b_ub}")

        b_bounded = r7001["balance_am_history"]
        if not (len(b_bounded) >= 3 and b_bounded[0] == 3333 and b_bounded[1] is None and b_bounded[2] == 1000):
            raise AssertionError(f"Unexpected 7001 bounded balance history: {b_bounded}")

        # payment grid must remain 36 only.
        if len(r7001["payment_history_grid"]) != 36:
            raise AssertionError(
                f"Expected payment_history_grid length 36, got {len(r7001['payment_history_grid'])}"
            )

        # 7002: bounded should be first 36 of unbounded.
        ub2 = r7002["balance_am_history_unbounded"] or []
        b2 = r7002["balance_am_history"] or []
        if len(ub2) != 40:
            raise AssertionError(f"Expected 7002 unbounded length=40, got {len(ub2)}")
        if len(b2) != 36:
            raise AssertionError(f"Expected 7002 bounded length=36, got {len(b2)}")
        if b2 != ub2[:36]:
            raise AssertionError("7002 bounded array is not prefix subset of unbounded array")

        sample = (
            spark.table(output_table)
            .select(
                "cons_acct_key",
                "rpt_as_of_mo",
                F.size("balance_am_history").alias("bounded_len"),
                F.size("balance_am_history_unbounded").alias("unbounded_len"),
                F.length("payment_history_grid").alias("payment_grid_len"),
            )
            .orderBy("cons_acct_key")
        )
        print("[INFO] Output sample:")
        sample.show(truncate=False)

        print("[PASS] test_generate_latest_summary_from_summary")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
