from datetime import datetime

import test_utils as tu


def _assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def run_test():
    spark = tu.create_spark_session("reg_generate_latest_72_from_summary")
    config = tu.load_main_test_config("reg_generate_latest_72_from_summary")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        rows = [
            tu.build_source_row(8101, "2025-12", datetime(2025, 12, 2, 0, 0, 0), balance=1200, actual_payment=120),
            tu.build_source_row(8101, "2026-03", datetime(2026, 3, 2, 0, 0, 0), balance=1300, actual_payment=130),
        ]
        for i in range(80):
            dt = datetime(2019 + (8 + i) // 12, ((8 + i) % 12) + 1, 2, 0, 0, 0)
            rows.append(tu.build_source_row(8102, dt.strftime("%Y-%m"), dt, balance=10000 + i, actual_payment=200 + i))

        tu.write_source_rows(spark, config["source_table"], rows)
        tu.main_pipeline.run_pipeline(spark, config)

        r8101 = tu.fetch_single_row(spark, config["latest_history_table"], 8101, "2026-03")
        b1 = r8101["balance_am_history"] or []
        _assert_equal(len(b1), 72, "8101 latest history len")
        _assert_equal(b1[0], 1300, "8101 index0")
        _assert_equal(b1[1], None, "8101 index1 gap")
        _assert_equal(b1[3], 1200, "8101 index3")

        r8102 = tu.fetch_single_row(spark, config["latest_history_table"], 8102, "2026-04")
        b2 = r8102["balance_am_history"] or []
        _assert_equal(len(b2), 72, "8102 latest history len")
        _assert_equal(b2[0], 10079, "8102 index0")
        _assert_equal(b2[35], 10044, "8102 recent 36-month window value")

        # Pipeline contract check: summary latest row must remain the 36-prefix of latest_summary.
        s8102 = tu.fetch_single_row(spark, config["destination_table"], 8102, "2026-04")
        _assert_equal(
            list(s8102["balance_am_history"] or []),
            list(b2[:36]),
            "8102 summary prefix matches latest",
        )

        print("[PASS] test_generate_latest_summary_72_from_summary")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
