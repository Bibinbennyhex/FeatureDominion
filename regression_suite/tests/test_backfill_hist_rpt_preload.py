from datetime import date, datetime

from pyspark.sql import functions as F

import test_utils as tu


def _assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected={expected}, actual={actual}")


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


def _row(table_df, key: int, month: str):
    out = (
        table_df
        .filter((F.col("cons_acct_key") == key) & (F.col("rpt_as_of_mo") == month))
        .select("cons_acct_key", "rpt_as_of_mo", "acct_dt", "soft_del_cd")
        .collect()
    )
    if len(out) != 1:
        raise AssertionError(f"Expected 1 row for key={key}, month={month}; got {len(out)}")
    return out[0]


def run_test():
    spark = tu.create_spark_session("reg_backfill_hist_rpt_preload")
    config = tu.load_main_test_config("reg_backfill_hist_rpt_preload")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        seed_rows = [
            tu.build_summary_row(7001, "2026-02", datetime(2026, 2, 1, 0, 0, 0), balance=100, actual_payment=10, soft_del_cd=""),
            tu.build_summary_row(7002, "2026-02", datetime(2026, 2, 1, 0, 0, 0), balance=110, actual_payment=11, soft_del_cd=""),
        ]
        tu.write_summary_rows(spark, config["destination_table"], seed_rows)
        tu.write_summary_rows(spark, config["latest_history_table"], seed_rows)

        src_7001 = tu.build_source_row(7001, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=200, actual_payment=20, soft_del_cd="")
        src_7002 = tu.build_source_row(7002, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=210, actual_payment=21, soft_del_cd="")
        src_7001["acct_dt"] = date(2026, 2, 10)
        src_7002["acct_dt"] = date(2026, 2, 12)
        tu.write_source_rows(spark, config["source_table"], [src_7001, src_7002])

        _write_hist_rows(
            spark,
            config["hist_rpt_dt_table"],
            [
                {
                    "cons_acct_key": 7001,
                    "soft_del_cd": "",
                    "acct_dt": date(2026, 2, 18),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "09:00:00",
                    "update_time": "11:00:00",
                },
                {
                    "cons_acct_key": 7002,
                    "soft_del_cd": "4",
                    "acct_dt": date(2026, 2, 25),
                    "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                    "insert_time": "10:00:00",
                    "update_time": "10:30:00",
                },
            ],
        )

        tu.main_pipeline.run_pipeline(spark, config)

        s_df = spark.table(config["destination_table"])
        l_df = spark.table(config["latest_history_table"])

        s7001 = _row(s_df, 7001, "2026-02")
        s7002 = _row(s_df, 7002, "2026-02")
        l7001 = _row(l_df, 7001, "2026-02")
        l7002 = _row(l_df, 7002, "2026-02")

        _assert_eq(s7001["acct_dt"], date(2026, 2, 18), "summary 7001 acct_dt from hist")
        _assert_eq(s7001["soft_del_cd"], "", "summary 7001 soft_del_cd")
        _assert_eq(s7002["acct_dt"], date(2026, 2, 12), "summary 7002 acct_dt from source when hist is delete")
        _assert_eq(s7002["soft_del_cd"], "", "summary 7002 soft_del_cd")

        _assert_eq(l7001["acct_dt"], date(2026, 2, 18), "latest 7001 acct_dt")
        _assert_eq(l7001["soft_del_cd"], "", "latest 7001 soft_del_cd")
        _assert_eq(l7002["acct_dt"], date(2026, 2, 12), "latest 7002 acct_dt")
        _assert_eq(l7002["soft_del_cd"], "", "latest 7002 soft_del_cd")

        print("[PASS] test_backfill_hist_rpt_preload")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
