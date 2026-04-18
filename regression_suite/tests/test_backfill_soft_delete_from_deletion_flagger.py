from datetime import datetime

from pyspark.sql import functions as F

import test_utils as tu


def _assert_eq(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def _print_snapshot(spark, summary_table: str, latest_table: str, title: str):
    print(f"\n===== {title} :: SUMMARY =====")
    (
        spark.table(summary_table)
        .filter(F.col("cons_acct_key").isin(3001, 3002))
        .orderBy("cons_acct_key", "rpt_as_of_mo")
        .select("cons_acct_key", "rpt_as_of_mo", "soft_del_cd", "base_ts", "balance_am_history", "payment_history_grid")
        .show(truncate=False)
    )
    print(f"\n===== {title} :: LATEST_SUMMARY =====")
    (
        spark.table(latest_table)
        .filter(F.col("cons_acct_key").isin(3001, 3002))
        .orderBy("cons_acct_key", "rpt_as_of_mo")
        .select("cons_acct_key", "rpt_as_of_mo", "soft_del_cd", "base_ts", "balance_am_history", "payment_history_grid")
        .show(truncate=False)
    )


def run_test():
    spark = tu.create_spark_session("reg_backfill_soft_delete_flagger")
    config = tu.load_main_test_config("reg_backfill_soft_delete_flagger")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        ts_1 = datetime(2026, 1, 2, 0, 0, 0)
        ts_2 = datetime(2026, 2, 2, 0, 0, 0)
        ts_3 = datetime(2026, 3, 2, 0, 0, 0)

        summary_rows = [
            tu.build_summary_row(3001, "2026-01", ts_1, balance=1100, actual_payment=100, payment_rating="1"),
            tu.build_summary_row(3001, "2026-02", ts_2, balance=1200, actual_payment=200, payment_rating="2",
                                 balance_history=tu.history({0: 1200, 1: 1100}),
                                 payment_history=tu.history({0: 200, 1: 100}),
                                 rating_history=tu.history({0: "2", 1: "1"})),
            tu.build_summary_row(3001, "2026-03", ts_3, balance=1300, actual_payment=300, payment_rating="3",
                                 balance_history=tu.history({0: 1300, 1: 1200, 2: 1100}),
                                 payment_history=tu.history({0: 300, 1: 200, 2: 100}),
                                 rating_history=tu.history({0: "3", 1: "2", 2: "1"})),
            tu.build_summary_row(3002, "2026-02", ts_2, balance=2200, actual_payment=220, payment_rating="2"),
            tu.build_summary_row(3002, "2026-03", ts_3, balance=2300, actual_payment=230, payment_rating="3",
                                 balance_history=tu.history({0: 2300, 1: 2200}),
                                 payment_history=tu.history({0: 230, 1: 220}),
                                 rating_history=tu.history({0: "3", 1: "2"})),
        ]

        latest_rows = [
            tu.build_summary_row(3001, "2026-03", ts_3, balance=1300, actual_payment=300, payment_rating="3",
                                 balance_history=tu.history({0: 1300, 1: 1200, 2: 1100}),
                                 payment_history=tu.history({0: 300, 1: 200, 2: 100}),
                                 rating_history=tu.history({0: "3", 1: "2", 2: "1"}),
                                 history_len=72),
            tu.build_summary_row(3002, "2026-03", ts_3, balance=2300, actual_payment=230, payment_rating="3",
                                 balance_history=tu.history({0: 2300, 1: 2200}, length=72),
                                 payment_history=tu.history({0: 230, 1: 220}, length=72),
                                 rating_history=tu.history({0: "3", 1: "2"}, length=72),
                                 history_len=72),
        ]

        tu.write_summary_rows(spark, config["destination_table"], summary_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], latest_rows)

        source_rows = [
            tu.build_source_row(3001, "2026-01", datetime(2026, 3, 10, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(3002, "2026-03", datetime(2026, 3, 11, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        _print_snapshot(spark, config["destination_table"], config["latest_history_table"], "BEFORE")
        tu.main_pipeline.run_pipeline(spark, config)
        _print_snapshot(spark, config["destination_table"], config["latest_history_table"], "AFTER")

        s_3001_202601 = tu.fetch_single_row(spark, config["destination_table"], 3001, "2026-01")
        s_3001_202603 = tu.fetch_single_row(spark, config["destination_table"], 3001, "2026-03")
        _assert_eq(s_3001_202601["soft_del_cd"], "4", "Deleted month soft_del flag")
        _assert_eq(s_3001_202603["balance_am_history"][2], None, "Future index-2 null patch in summary")

        latest_3002 = spark.table(config["latest_history_table"]).filter("cons_acct_key = 3002").collect()
        if len(latest_3002) != 1:
            raise AssertionError(f"Expected one latest row for 3002, got {len(latest_3002)}")
        _assert_eq(latest_3002[0]["rpt_as_of_mo"], "2026-02", "Latest reconstruction month for 3002")

        print("\n[PASS] test_backfill_soft_delete_from_deletion_flagger")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
