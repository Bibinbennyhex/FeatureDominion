from datetime import datetime

import test_utils as tu


def _assert_eq(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


def _seed_account_rows(cons_acct_key: int, balance_base: int, pay_base: int, history_len: int = 36):
    rows = []
    months = [f"2026-{m:02d}" for m in range(1, 7)]
    for i, month in enumerate(months):
        ts = datetime(2026, i + 1, 2, 0, 0, 0)
        bal_positions = {pos: balance_base + ((i - pos) * 100) for pos in range(i + 1)}
        pay_positions = {pos: pay_base + (i - pos) for pos in range(i + 1)}
        rating_positions = {pos: str(((i - pos) + 1) % 7) for pos in range(i + 1)}
        if history_len > 36:
            bal_positions[36] = balance_base - 999
            pay_positions[36] = pay_base - 99
            rating_positions[36] = "Z"

        rows.append(
            tu.build_summary_row(
                cons_acct_key,
                month,
                ts,
                balance=balance_base + (i * 100),
                actual_payment=pay_base + i,
                payment_rating=str((i + 1) % 7),
                balance_history=tu.history(bal_positions, length=history_len),
                payment_history=tu.history(pay_positions, length=history_len),
                rating_history=tu.history(rating_positions, length=history_len),
                history_len=history_len,
            )
        )
    return rows


def run_test():
    spark = tu.create_spark_session("reg_backfill_soft_delete_flagger_multidelete")
    config = tu.load_main_test_config("reg_backfill_soft_delete_flagger_multidelete")

    try:
        tu.main_pipeline.cleanup(spark)
        tu.reset_tables(spark, config)

        rows_4001 = _seed_account_rows(4001, 1000, 100, history_len=36)
        rows_4002 = _seed_account_rows(4002, 2000, 200, history_len=36)
        rows_4003 = _seed_account_rows(4003, 3000, 300, history_len=36)
        latest_rows_4001 = _seed_account_rows(4001, 1000, 100, history_len=72)
        latest_rows_4002 = _seed_account_rows(4002, 2000, 200, history_len=72)
        latest_rows_4003 = _seed_account_rows(4003, 3000, 300, history_len=72)

        tu.write_summary_rows(spark, config["destination_table"], rows_4001 + rows_4002 + rows_4003)
        latest_rows = [
            next(r for r in latest_rows_4001 if r["rpt_as_of_mo"] == "2026-06"),
            next(r for r in latest_rows_4002 if r["rpt_as_of_mo"] == "2026-06"),
            next(r for r in latest_rows_4003 if r["rpt_as_of_mo"] == "2026-06"),
        ]
        tu.write_summary_rows(spark, config["latest_history_table"], latest_rows)

        deletion_source_rows = [
            tu.build_source_row(4001, "2026-02", datetime(2026, 7, 1, 10, 0, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(4001, "2026-03", datetime(2026, 7, 1, 10, 1, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(4002, "2026-01", datetime(2026, 7, 1, 10, 2, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(4002, "2026-04", datetime(2026, 7, 1, 10, 3, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(4003, "2026-05", datetime(2026, 7, 1, 10, 4, 0), balance=1, actual_payment=1, soft_del_cd="4"),
            tu.build_source_row(4003, "2026-06", datetime(2026, 7, 1, 10, 5, 0), balance=1, actual_payment=1, soft_del_cd="4"),
        ]
        tu.write_source_rows(spark, config["source_table"], deletion_source_rows)

        tu.main_pipeline.run_pipeline(spark, config)

        s_4001_202606 = tu.fetch_single_row(spark, config["destination_table"], 4001, "2026-06")
        _assert_eq(s_4001_202606["balance_am_history"][3], None, "4001 continuous delete index-3 null")
        _assert_eq(s_4001_202606["balance_am_history"][4], None, "4001 continuous delete index-4 null")

        s_4002_202606 = tu.fetch_single_row(spark, config["destination_table"], 4002, "2026-06")
        _assert_eq(s_4002_202606["balance_am_history"][2], None, "4002 random delete index-2 null")
        _assert_eq(s_4002_202606["balance_am_history"][5], None, "4002 random delete index-5 null")

        latest_4003 = spark.table(config["latest_history_table"]).filter("cons_acct_key = 4003").collect()
        if len(latest_4003) != 1:
            raise AssertionError(f"Expected exactly 1 latest row for 4003, got {len(latest_4003)}")
        _assert_eq(latest_4003[0]["rpt_as_of_mo"], "2026-04", "4003 reconstructed latest month")

        latest_4001_post = tu.fetch_single_row(spark, config["latest_history_table"], 4001, "2026-06")
        _assert_eq(len(latest_4001_post["balance_am_history"]), 72, "latest 4001 balance history length")
        _assert_eq(latest_4001_post["balance_am_history"][36], 1, "latest 4001 balance tail preserved")

        print("\n[PASS] test_backfill_soft_delete_from_deletion_flagger_multidelete")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_test()
