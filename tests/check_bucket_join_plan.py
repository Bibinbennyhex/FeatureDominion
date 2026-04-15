"""
Plan check for bucket-aware join usage between summary and case tables.
"""

from datetime import datetime

from pyspark.sql import functions as F

from test_utils import (
    build_summary_row,
    create_spark_session,
    load_main_test_config,
    reset_tables,
    write_summary_rows,
)

import summary_inc as pipeline


def _focused_lines(plan_text: str):
    keys = (
        "Join",
        "Exchange",
        "BatchScan",
        "Partitioning",
        "bucket",
        "StoragePartition",
        "SortMerge",
    )
    return [line for line in plan_text.splitlines() if any(k in line for k in keys)]


def main():
    namespace = "bucket_plan_chk"
    spark = create_spark_session("bucket_plan_check")
    config = load_main_test_config(namespace)

    # Keep strategy deterministic while inspecting plans.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    try:
        reset_tables(spark, config)
        summary_table = config["destination_table"]
        pk = config["primary_column"]
        prt = config["partition_column"]

        rows = [
            build_summary_row(
                cons_acct_key=i,
                rpt_as_of_mo="2026-01",
                base_ts=datetime(2026, 1, 31, 0, 0, 0),
                balance=1000 + i,
                actual_payment=100 + (i % 50),
                soft_del_cd="",
            )
            for i in range(1, 801)
        ]
        write_summary_rows(spark, summary_table, rows)

        base_df = spark.table(summary_table)
        case_match_df = (
            base_df.where(F.col(pk) <= 400)
            .withColumn("base_ts", F.expr("base_ts + INTERVAL 1 DAY"))
            .withColumn("balance_am", F.col("balance_am") + F.lit(5))
        )
        case_new_df = (
            base_df.where(F.col(pk) <= 120)
            .withColumn(pk, F.col(pk) + F.lit(1000000))
            .withColumn("base_ts", F.expr("base_ts + INTERVAL 1 DAY"))
        )
        case_df = case_match_df.unionByName(case_new_df)

        pipeline.write_case_table_bucketed(
            spark=spark,
            df=case_df,
            table_name="temp_catalog.checkpointdb.case_1",
            config=config,
            stage="bucket_plan_case_1",
            expected_rows=520,
        )

        join_plan = "\n".join(
            r[0]
            for r in spark.sql(
                f"""
                EXPLAIN FORMATTED
                SELECT s.{pk}, s.{prt}
                FROM {summary_table} s
                JOIN temp_catalog.checkpointdb.case_1 c
                  ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                """
            ).collect()
        )

        merge_plan = "\n".join(
            r[0]
            for r in spark.sql(
                f"""
                EXPLAIN FORMATTED
                MERGE INTO {summary_table} s
                USING temp_catalog.checkpointdb.case_1 c
                ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                WHEN MATCHED AND c.base_ts >= s.base_ts THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            ).collect()
        )

        print("\n===== JOIN PLAN (focused) =====")
        for line in _focused_lines(join_plan):
            print(line)
        print(f"\nJOIN_HAS_EXCHANGE={('Exchange' in join_plan)}")

        print("\n===== MERGE PLAN (focused) =====")
        for line in _focused_lines(merge_plan):
            print(line)
        print(f"\nMERGE_HAS_EXCHANGE={('Exchange' in merge_plan)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
