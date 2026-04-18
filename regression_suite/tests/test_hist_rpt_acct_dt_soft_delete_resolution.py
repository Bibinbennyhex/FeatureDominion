"""
Dedicated regression for acct_dt + soft_del_cd resolution using
consumer_account_hist_rpt (maintainer table).

Validates (pipeline-reachable paths):
1) Hist dedup ordering: base_ts, insert_dt, update_dt, insert_time, update_time (DESC)
2) Resolution rules between accounts_all (r) and hist table (d):
   - r non-delete vs d delete(4) => keep r
   - r non-delete vs d non-delete with newer d.acct_dt => take d
"""

from datetime import date, datetime

from pyspark.sql import functions as F

from test_utils import (
    assert_watermark_tracker_consistent,
    build_source_row,
    build_summary_row,
    create_spark_session,
    load_main_test_config,
    main_pipeline,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


def _assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected={expected}, actual={actual}")


def _write_hist_rows(spark, table: str, rows):
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
    spark = create_spark_session("main_hist_rpt_acct_dt_soft_del_test")
    config = load_main_test_config("main_hist_rpt_resolution")

    try:
        print("[SETUP] Resetting tables...")
        reset_tables(spark, config)

        summary_table = config["destination_table"]
        latest_table = config["latest_history_table"]
        source_table = config["source_table"]
        hist_table = config["hist_rpt_dt_table"]

        print("[SETUP] Seeding summary/latest baseline...")
        # Global max month must be 2026-02 so source month 2026-02 is eligible (prt < next_month => < 2026-03).
        baseline_rows = [
            build_summary_row(2001, "2026-01", datetime(2026, 1, 1, 0, 0, 0), balance=100, actual_payment=10, soft_del_cd=""),
            build_summary_row(2002, "2026-01", datetime(2026, 1, 1, 0, 0, 0), balance=110, actual_payment=11, soft_del_cd=""),
            build_summary_row(2003, "2026-01", datetime(2026, 1, 1, 0, 0, 0), balance=120, actual_payment=12, soft_del_cd=""),
            build_summary_row(9998, "2026-02", datetime(2026, 2, 1, 0, 0, 0), balance=500, actual_payment=50, soft_del_cd=""),
        ]
        write_summary_rows(spark, summary_table, baseline_rows)
        write_summary_rows(spark, latest_table, baseline_rows)

        print("[SETUP] Loading source accounts_all rows...")
        src_2001 = build_source_row(2001, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=210, actual_payment=21, soft_del_cd="")
        src_2002 = build_source_row(2002, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=220, actual_payment=22, soft_del_cd="")
        src_2003 = build_source_row(2003, "2026-02", datetime(2026, 2, 15, 0, 0, 0), balance=230, actual_payment=23, soft_del_cd="")

        # Override acct_dt to assert resolution behavior clearly.
        src_2001["acct_dt"] = date(2026, 2, 10)
        src_2002["acct_dt"] = date(2026, 2, 8)
        src_2003["acct_dt"] = date(2026, 2, 7)
        write_source_rows(spark, source_table, [src_2001, src_2002, src_2003])

        print("[SETUP] Loading consumer_account_hist_rpt rows...")
        hist_rows = [
            # 2001: duplicate keys, winner should be update_time='11:00:00' (same base/insert/update dt).
            {
                "cons_acct_key": 2001,
                "soft_del_cd": "",
                "acct_dt": date(2026, 2, 12),
                "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                "insert_time": "09:00:00",
                "update_time": "10:00:00",
            },
            {
                "cons_acct_key": 2001,
                "soft_del_cd": "",
                "acct_dt": date(2026, 2, 18),
                "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                "insert_time": "09:00:00",
                "update_time": "11:00:00",
            },
            # 2002: r is active, d is active newer => should take d values.
            {
                "cons_acct_key": 2002,
                "soft_del_cd": "",
                "acct_dt": date(2026, 2, 20),
                "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                "insert_time": "08:00:00",
                "update_time": "08:30:00",
            },
            # 2003: r is active, d is delete(4) => should keep r values.
            {
                "cons_acct_key": 2003,
                "soft_del_cd": "4",
                "acct_dt": date(2026, 2, 21),
                "base_ts": datetime(2026, 2, 15, 0, 0, 0),
                "insert_dt": datetime(2026, 2, 15, 0, 0, 0),
                "update_dt": datetime(2026, 2, 15, 0, 0, 0),
                "insert_time": "07:00:00",
                "update_time": "07:30:00",
            },
        ]
        _write_hist_rows(spark, hist_table, hist_rows)

        print("[RUN] Executing pipeline...")
        main_pipeline.cleanup(spark)
        main_pipeline.run_pipeline(spark, config)
        assert_watermark_tracker_consistent(spark, config)

        print("[ASSERT] Validating summary outputs...")
        summary_df = spark.table(summary_table)
        latest_df = spark.table(latest_table)

        s2001 = _row(summary_df, 2001, "2026-02")
        s2002 = _row(summary_df, 2002, "2026-02")
        s2003 = _row(summary_df, 2003, "2026-02")

        _assert_eq(s2001["acct_dt"], date(2026, 2, 18), "summary 2001 acct_dt from dedup winner")
        _assert_eq(s2001["soft_del_cd"], "", "summary 2001 soft_del_cd")

        _assert_eq(s2002["acct_dt"], date(2026, 2, 20), "summary 2002 acct_dt from hist active row")
        _assert_eq(s2002["soft_del_cd"], "", "summary 2002 soft_del_cd from hist active row")

        _assert_eq(s2003["acct_dt"], date(2026, 2, 7), "summary 2003 acct_dt when hist is delete")
        _assert_eq(s2003["soft_del_cd"], "", "summary 2003 soft_del_cd when hist is delete")

        print("[ASSERT] Validating latest_summary outputs...")
        l2001 = _row(latest_df, 2001, "2026-02")
        l2002 = _row(latest_df, 2002, "2026-02")
        l2003 = _row(latest_df, 2003, "2026-02")

        _assert_eq(l2001["acct_dt"], date(2026, 2, 18), "latest 2001 acct_dt")
        _assert_eq(l2001["soft_del_cd"], "", "latest 2001 soft_del_cd")

        _assert_eq(l2002["acct_dt"], date(2026, 2, 20), "latest 2002 acct_dt")
        _assert_eq(l2002["soft_del_cd"], "", "latest 2002 soft_del_cd")

        _assert_eq(l2003["acct_dt"], date(2026, 2, 7), "latest 2003 acct_dt when hist is delete")
        _assert_eq(l2003["soft_del_cd"], "", "latest 2003 soft_del_cd when hist is delete")

        print("[PASS] test_hist_rpt_acct_dt_soft_delete_resolution")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
