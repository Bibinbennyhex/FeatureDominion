from datetime import datetime, timezone
from pathlib import Path
import csv

from pyspark.sql import functions as F

import test_utils as tu


LOG_ROOT = Path(__file__).resolve().parent / "artifacts" / "audits" / "backfill_soft_delete_audit_logs"


def _write_df_csv(df, output_path: Path):
    rows = [r.asDict(recursive=True) for r in df.collect()]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def run_test():
    spark = tu.create_spark_session("reg_backfill_soft_delete_audit_export")
    config = tu.load_main_test_config("reg_backfill_soft_delete_audit_export")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_dir = LOG_ROOT / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        print(f"[AUDIT] Output folder: {run_dir}")
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        seed_ts = datetime(2026, 2, 1, 0, 0, 0)
        summary_rows = [
            tu.build_summary_row(9101, "2025-12", seed_ts, balance=5000, actual_payment=500,
                                 balance_history=tu.history({0: 5000})),
            tu.build_summary_row(9101, "2026-01", seed_ts, balance=5100, actual_payment=510,
                                 balance_history=tu.history({0: 5100, 1: 5000})),
            tu.build_summary_row(9101, "2026-02", seed_ts, balance=5200, actual_payment=520,
                                 balance_history=tu.history({0: 5200, 1: 5100, 2: 5000})),
            tu.build_summary_row(9102, "2026-01", seed_ts, balance=1000, actual_payment=100,
                                 balance_history=tu.history({0: 1000})),
            tu.build_summary_row(9102, "2026-02", seed_ts, balance=1100, actual_payment=110,
                                 balance_history=tu.history({0: 1100, 1: 1000})),
        ]
        tu.write_summary_rows(spark, config["destination_table"], summary_rows)
        tu.write_summary_rows(
            spark,
            config["latest_history_table"],
            [
                tu.build_summary_row(9101, "2026-02", seed_ts, balance=5200, actual_payment=520,
                                     balance_history=tu.history({0: 5200, 1: 5100, 2: 5000}, length=72), history_len=72),
                tu.build_summary_row(9102, "2026-02", seed_ts, balance=1100, actual_payment=110,
                                     balance_history=tu.history({0: 1100, 1: 1000}, length=72), history_len=72),
            ],
        )

        source_rows = [
            tu.build_source_row(9101, "2025-12", datetime(2026, 2, 15, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            tu.build_source_row(9101, "2026-01", datetime(2026, 2, 15, 11, 0, 0), balance=1, actual_payment=1, soft_del_cd="1"),
            tu.build_source_row(9102, "2026-02", datetime(2026, 2, 16, 9, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        summary_before_df = spark.table(config["destination_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")
        latest_before_df = spark.table(config["latest_history_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")

        _write_df_csv(summary_before_df, run_dir / "summary_before.csv")
        _write_df_csv(latest_before_df, run_dir / "latest_summary_before.csv")

        print("[AUDIT] summary (before):")
        summary_before_df.show(200, truncate=False)
        print("[AUDIT] latest_summary (before):")
        latest_before_df.show(200, truncate=False)

        tu.main_pipeline.run_pipeline(spark, config)

        summary_after_df = spark.table(config["destination_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")
        latest_after_df = spark.table(config["latest_history_table"]).orderBy("cons_acct_key", "rpt_as_of_mo")

        _write_df_csv(summary_after_df, run_dir / "summary_after.csv")
        _write_df_csv(latest_after_df, run_dir / "latest_summary_after.csv")

        print("[AUDIT] summary (after):")
        summary_after_df.show(200, truncate=False)
        print("[AUDIT] latest_summary (after):")
        latest_after_df.show(200, truncate=False)

        s_9101_2026_02 = tu.fetch_single_row(spark, config["destination_table"], 9101, "2026-02")
        l_9101_2026_02 = tu.fetch_single_row(spark, config["latest_history_table"], 9101, "2026-02")
        _assert_equal(s_9101_2026_02["balance_am_history"][1], None, "summary future index 1 nullified")
        _assert_equal(l_9101_2026_02["balance_am_history"][1], None, "latest future index 1 nullified")

        l_9102 = spark.table(config["latest_history_table"]).filter("cons_acct_key = 9102").collect()
        _assert_equal(len(l_9102), 1, "one latest row for 9102")
        _assert_equal(l_9102[0]["rpt_as_of_mo"], "2026-01", "latest_summary reconstructed for 9102")

        print(f"[PASS] test_backfill_soft_delete_audit_export | audit_dir={run_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
