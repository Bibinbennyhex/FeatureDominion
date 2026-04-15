"""
Validate partition-pruned summary merge plans for backfill_soft_delete_from_accounts_v2.
"""

from datetime import datetime
import os
import sys

from pyspark.sql import functions as F

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


def _plan_text(df):
    return "\n".join([r[0] for r in df.collect() if r and r[0] is not None]).lower()


def _assert_partition_pruning(plan: str, label: str):
    if "rpt_as_of_mo" not in plan:
        raise AssertionError(f"{label}: plan does not mention rpt_as_of_mo")
    if ("partition" not in plan) and ("dynamicpruning" not in plan):
        raise AssertionError(f"{label}: expected partition/dynamic pruning markers in explain plan")


def run_test():
    spark = create_spark_session("one_time_backfill_partition_pruning_v2")
    config = load_main_test_config("main_one_time_backfill_pruning_v2")
    try:
        reset_tables(spark, config)

        backfill_job.SOURCE_TABLE = config["source_table"]
        backfill_job.SUMMARY_TABLE = config["destination_table"]
        backfill_job.LATEST_SUMMARY_TABLE = config["latest_history_table"]
        backfill_job.HIST_RPT_TABLE = config["hist_rpt_dt_table"]

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)
        summary_rows = [
            build_summary_row(1201, "2026-01", seed_ts, balance=12010, actual_payment=121),
            build_summary_row(
                1201,
                "2026-02",
                seed_ts,
                balance=12020,
                actual_payment=122,
                balance_history=history({0: 12020, 1: 12010}),
                payment_history=history({0: 122, 1: 121}),
            ),
            build_summary_row(
                1201,
                "2026-03",
                seed_ts,
                balance=12030,
                actual_payment=123,
                balance_history=history({0: 12030, 1: 12020, 2: 12010}),
                payment_history=history({0: 123, 1: 122, 2: 121}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], summary_rows)
        write_summary_rows(spark, config["latest_history_table"], [summary_rows[-1]])

        source_rows = [
            build_source_row(1201, "2026-03", datetime(2026, 2, 15, 0, 0, 0), balance=13030, actual_payment=223),
            build_source_row(1201, "2026-01", datetime(2026, 2, 16, 0, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        backfill_job._ensure_target_schema(spark)
        src_latest = backfill_job._build_source_latest_table(spark)
        hist_latest = backfill_job._build_hist_latest_table(spark)
        resolved = backfill_job._build_resolved_updates_table(spark, src_latest, hist_latest)

        updates_df = spark.table(resolved)
        month = updates_df.select("rpt_as_of_mo").first()["rpt_as_of_mo"]
        updates_df.filter(F.col("rpt_as_of_mo") == month).createOrReplaceTempView("acct_softdel_chunk")
        explain_scalar = spark.sql(
            f"""
            EXPLAIN FORMATTED
            MERGE INTO {config['destination_table']} s
            USING acct_softdel_chunk c
            ON s.cons_acct_key = c.cons_acct_key AND s.rpt_as_of_mo = c.rpt_as_of_mo
            WHEN MATCHED THEN UPDATE SET
              s.acct_dt = c.acct_dt,
              s.soft_del_cd = c.soft_del_cd,
              s.base_ts = GREATEST(s.base_ts, c.base_ts)
            """
        )
        scalar_plan = _plan_text(explain_scalar)
        _assert_partition_pruning(scalar_plan, "Scalar merge")

        history_cols, grid_specs = backfill_job._ensure_target_schema(spark)
        backfill_job._prepare_case_tables_from_source_latest(spark, src_latest, history_cols, grid_specs)
        if spark.catalog.tableExists(backfill_job.CASE_3D_FUTURE_TABLE):
            spark.table(backfill_job.CASE_3D_FUTURE_TABLE).limit(1000).createOrReplaceTempView("case_3d_future_chunk")
            set_expr = ["s.base_ts = GREATEST(s.base_ts, c.base_ts)"] + [f"s.{c} = c.{c}" for c in history_cols]
            explain_future = spark.sql(
                f"""
                EXPLAIN FORMATTED
                MERGE INTO {config['destination_table']} s
                USING case_3d_future_chunk c
                ON s.cons_acct_key = c.cons_acct_key AND s.rpt_as_of_mo = c.rpt_as_of_mo
                WHEN MATCHED THEN UPDATE SET {', '.join(set_expr)}
                """
            )
            future_plan = _plan_text(explain_future)
            _assert_partition_pruning(future_plan, "Future merge")

        print("[PASS] one_time_backfill_partition_pruning_v2")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()

