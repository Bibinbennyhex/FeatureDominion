"""
Standalone test for month-by-month 72-month latest_summary builder.
"""

from datetime import datetime
import os
import sys

from pyspark.sql import functions as F

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")
if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

from generate_latest_summary_72_from_summary import generate_latest_summary_72_from_summary
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
    spark = create_spark_session("test_generate_latest_summary_72_from_summary")
    config = load_main_test_config("main_generate_latest_summary_72_from_summary")

    try:
        reset_tables(spark, config)

        rows = [
            # Account 8101 (sparse history, latest month = 2026-03).
            build_summary_row(8101, "2025-12", datetime(2025, 12, 2, 0, 0, 0), balance=1200, actual_payment=120, payment_rating="1"),
            build_summary_row(8101, "2026-03", datetime(2026, 3, 2, 0, 0, 0), balance=1300, actual_payment=130, payment_rating="3"),
        ]

        # Account 8102 (long continuous history, latest month = 2026-04).
        # 80 months from 2019-09 .. 2026-04.
        for i in range(80):
            y, m = _add_months(2019, 9, i)
            month = _ym(y, m)
            bal = 10000 + i
            pay = 200 + i
            rows.append(
                build_summary_row(
                    8102,
                    month,
                    datetime(y, m, 2, 0, 0, 0),
                    balance=bal,
                    actual_payment=pay,
                    payment_rating="0",
                )
            )

        write_summary_rows(spark, config["destination_table"], rows)

        output_table = f"{config['latest_history_table']}_72_from_summary_test"
        spark.sql(f"DROP TABLE IF EXISTS {output_table}")
        spark.sql(
            f"""
            CREATE TABLE {output_table}
            USING iceberg
            PARTITIONED BY (bucket(64, cons_acct_key))
            AS SELECT * FROM {config['latest_history_table']} WHERE 1=0
            """
        )

        generate_latest_summary_72_from_summary(
            spark=spark,
            config=config,
            output_table=output_table,
        )

        if not spark.catalog.tableExists(output_table):
            raise AssertionError(f"Output table was not created: {output_table}")

        out_count = spark.table(output_table).count()
        if out_count != 2:
            raise AssertionError(f"Expected 2 latest rows in output, got {out_count}")

        # 8101 checks (gap handling + dedupe winner).
        r8101 = fetch_single_row(spark, output_table, 8101, "2026-03")
        b1 = r8101["balance_am_history"] or []
        if len(b1) != 72:
            raise AssertionError(f"Expected 8101 history len=72, got {len(b1)}")
        if b1[0] != 1300:
            raise AssertionError(f"Expected 8101 index0=1300, got {b1[0]}")
        if b1[1] is not None:
            raise AssertionError(f"Expected 8101 index1=NULL (2026-02 gap), got {b1[1]}")
        if b1[3] != 1200:
            raise AssertionError(f"Expected 8101 index3=1200 (2025-12), got {b1[3]}")

        if len(r8101["payment_history_grid"]) != 72:
            raise AssertionError(
                f"Expected payment_history_grid length 72, got {len(r8101['payment_history_grid'])}"
            )

        # 8102 checks (long history truncated to 72).
        r8102 = fetch_single_row(spark, output_table, 8102, "2026-04")
        b2 = r8102["balance_am_history"] or []
        if len(b2) != 72:
            raise AssertionError(f"Expected 8102 history len=72, got {len(b2)}")
        if b2[0] != (10000 + 79):
            raise AssertionError(f"Expected 8102 index0={10000+79}, got {b2[0]}")
        if b2[71] != (10000 + 8):
            raise AssertionError(f"Expected 8102 index71={10000+8}, got {b2[71]}")

        sample = (
            spark.table(output_table)
            .select(
                "cons_acct_key",
                "rpt_as_of_mo",
                F.size("balance_am_history").alias("history_len"),
                F.length("payment_history_grid").alias("payment_grid_len"),
            )
            .orderBy("cons_acct_key")
        )
        print("[INFO] Output sample:")
        sample.show(truncate=False)
        print("[PASS] test_generate_latest_summary_72_from_summary")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
