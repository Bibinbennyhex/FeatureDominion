from datetime import datetime

import test_utils as tu


def _assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def run_test():
    spark = tu.create_spark_session("reg_generate_latest_from_summary")
    config = tu.load_main_test_config("reg_generate_latest_from_summary")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        rows = [
            tu.build_source_row(7001, "2026-01", datetime(2026, 1, 2, 0, 0, 0), balance=1000, actual_payment=100),
            tu.build_source_row(7001, "2026-03", datetime(2026, 3, 5, 0, 0, 0), balance=3333, actual_payment=333),
        ]
        for i in range(40):
            month = datetime(2023 + ((i) // 12), ((i % 12) + 1), 2, 0, 0, 0)
            rpt = month.strftime("%Y-%m")
            rows.append(tu.build_source_row(7002, rpt, month, balance=10000 + i, actual_payment=200 + i))

        tu.write_source_rows(spark, config["source_table"], rows)
        tu.main_pipeline.run_pipeline(spark, config)

        r7001 = tu.fetch_single_row(spark, config["latest_history_table"], 7001, "2026-03")
        r7002 = tu.fetch_single_row(spark, config["latest_history_table"], 7002, "2026-04")

        b1 = r7001["balance_am_history"] or []
        _assert_equal(len(b1), 72, "7001 latest history length")
        _assert_equal(b1[0], 3333, "7001 latest index0")
        _assert_equal(b1[1], None, "7001 gap month should be NULL")
        _assert_equal(b1[2], 1000, "7001 prior month value")

        b2 = r7002["balance_am_history"] or []
        _assert_equal(len(b2), 72, "7002 latest history length")

        # summary latest row must be 36-prefix of latest_summary 72 arrays
        s7002 = tu.fetch_single_row(spark, config["destination_table"], 7002, "2026-04")
        _assert_equal(list(s7002["balance_am_history"] or []), list(b2[:36]), "summary prefix matches latest")

        print("[PASS] test_generate_latest_summary_from_summary")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
