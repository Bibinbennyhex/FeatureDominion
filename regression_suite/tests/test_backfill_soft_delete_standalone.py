from datetime import datetime

import test_utils as tu


HISTORY_COLS = [
    "actual_payment_am_history",
    "balance_am_history",
    "credit_limit_am_history",
    "past_due_am_history",
    "payment_rating_cd_history",
    "days_past_due_history",
    "asset_class_cd_4in_history",
]


def _assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected={expected}, actual={actual}")


def run_test():
    spark = tu.create_spark_session("reg_backfill_soft_delete_standalone")
    config = tu.load_main_test_config("reg_backfill_soft_delete_standalone")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)

        seed_8101 = [
            tu.build_summary_row(8101, "2025-12", seed_ts, balance=5000, actual_payment=500),
            tu.build_summary_row(8101, "2026-01", seed_ts, balance=5100, actual_payment=510,
                                 balance_history=tu.history({0: 5100, 1: 5000})),
            tu.build_summary_row(8101, "2026-02", seed_ts, balance=5200, actual_payment=520,
                                 balance_history=tu.history({0: 5200, 1: 5100, 2: 5000})),
        ]
        seed_8102 = [
            tu.build_summary_row(8102, "2026-01", seed_ts, balance=1000, actual_payment=100),
            tu.build_summary_row(8102, "2026-02", seed_ts, balance=1100, actual_payment=110,
                                 balance_history=tu.history({0: 1100, 1: 1000})),
        ]

        all_summary_rows = seed_8101 + seed_8102
        tu.write_summary_rows(spark, config["destination_table"], all_summary_rows)
        latest_rows = [
            [r for r in seed_8101 if r["rpt_as_of_mo"] == "2026-02"][0],
            [r for r in seed_8102 if r["rpt_as_of_mo"] == "2026-02"][0],
        ]
        tu.write_summary_rows(spark, config["latest_history_table"], latest_rows)

        source_rows = [
            tu.build_source_row(8101, "2025-12", datetime(2026, 2, 15, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            tu.build_source_row(8101, "2026-01", datetime(2026, 2, 15, 11, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            tu.build_source_row(8101, "2026-01", datetime(2026, 2, 15, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(8102, "2026-02", datetime(2026, 2, 16, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        tu.main_pipeline.run_pipeline(spark, config)

        s_8101_2025_12 = tu.fetch_single_row(spark, config["destination_table"], 8101, "2025-12")
        s_8101_2026_01 = tu.fetch_single_row(spark, config["destination_table"], 8101, "2026-01")
        s_8101_2026_02 = tu.fetch_single_row(spark, config["destination_table"], 8101, "2026-02")
        l_8101_2026_02 = tu.fetch_single_row(spark, config["latest_history_table"], 8101, "2026-02")

        _assert_eq(s_8101_2025_12["soft_del_cd"], "1", "Deleted month 2025-12 should be flagged")
        _assert_eq(s_8101_2026_01["soft_del_cd"], "1", "Deleted month 2026-01 should use latest delete winner")
        _assert_eq(s_8101_2026_02["balance_am_history"][1], None, "Future index 1 should be nullified")
        _assert_eq(s_8101_2026_02["balance_am_history"][2], None, "Future index 2 should be nullified")
        _assert_eq(l_8101_2026_02["balance_am_history"][1], None, "latest_summary index 1 should be nullified")
        _assert_eq(l_8101_2026_02["balance_am_history"][2], None, "latest_summary index 2 should be nullified")

        latest_8102 = spark.table(config["latest_history_table"]).filter("cons_acct_key = 8102").collect()
        if len(latest_8102) != 1:
            raise AssertionError(f"Expected one latest row for 8102, got {len(latest_8102)}")
        _assert_eq(latest_8102[0]["rpt_as_of_mo"], "2026-01", "latest_summary should reconstruct to prior non-deleted month")

        tu.assert_deletion_aware_invariants(spark, config)
        print("[PASS] test_backfill_soft_delete_standalone")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
