"""
Compact scenario suite for main/docker_test/tests.
Provides shared test routines and lightweight aliases for full filename parity.
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import datetime
from typing import List

from pyspark.sql import functions as F

from test_utils import (
    assert_watermark_tracker_consistent,
    build_source_row,
    build_summary_row,
    create_spark_session,
    fetch_single_row,
    history,
    load_main_test_config,
    main_pipeline,
    reset_tables,
    write_source_rows,
    write_summary_rows,
)


ANCHOR_ACCOUNT = 999999
ANCHOR_MIN_MONTH = "2020-01"
ANCHOR_MAX_MONTH = "2026-12"


def _assert_equal(actual, expected, message: str):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def _assert_true(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def _month_iter(start_year: int, start_month: int, count: int) -> List[str]:
    year = start_year
    month = start_month
    out = []
    for _ in range(count):
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return out


def _initialize(namespace: str, app_name: str):
    spark = create_spark_session(app_name)
    config = load_main_test_config(namespace)
    reset_tables(spark, config)

    anchor_old = build_summary_row(
        cons_acct_key=ANCHOR_ACCOUNT,
        rpt_as_of_mo=ANCHOR_MIN_MONTH,
        base_ts=datetime(2020, 1, 1, 0, 0, 0),
        balance=100,
        actual_payment=10,
        balance_history=history({0: 100}),
        payment_history=history({0: 10}),
        credit_history=history({0: 1000}),
        past_due_history=history({0: 0}),
        rating_history=history({0: "0"}),
        dpd_history=history({0: 0}),
        asset_history=history({0: "A"}),
    )
    anchor_new = build_summary_row(
        cons_acct_key=ANCHOR_ACCOUNT,
        rpt_as_of_mo=ANCHOR_MAX_MONTH,
        base_ts=datetime(2020, 1, 2, 0, 0, 0),
        balance=120,
        actual_payment=12,
        balance_history=history({0: 120, 1: 100}),
        payment_history=history({0: 12, 1: 10}),
        credit_history=history({0: 1000, 1: 1000}),
        past_due_history=history({0: 0, 1: 0}),
        rating_history=history({0: "0", 1: "0"}),
        dpd_history=history({0: 0, 1: 0}),
        asset_history=history({0: "A", 1: "A"}),
    )
    write_summary_rows(spark, config["destination_table"], [anchor_old, anchor_new])
    write_summary_rows(spark, config["latest_history_table"], [anchor_new])

    return spark, config


def _run_pipeline(spark, config):
    main_pipeline.cleanup(spark)
    main_pipeline.run_pipeline(spark, config)
    assert_watermark_tracker_consistent(spark, config)


def run_simple_test():
    spark = create_spark_session("main_simple_test")
    try:
        _assert_equal(spark.range(5).count(), 5, "Spark smoke count")
        print("[PASS] simple_test")
    finally:
        spark.stop()


def run_all_scenarios_test(namespace: str = "main_all_scenarios"):
    spark, config = _initialize(namespace, "main_all_scenarios")
    try:
        existing_ts = datetime(2025, 12, 1, 0, 0, 0)
        existing_summary = [
            build_summary_row(
                2001,
                "2025-12",
                existing_ts,
                balance=5000,
                actual_payment=500,
                balance_history=history({0: 5000, 1: 4500}),
                payment_history=history({0: 500, 1: 450}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
            build_summary_row(
                3001,
                "2026-01",
                existing_ts,
                balance=4700,
                actual_payment=470,
                balance_history=history({0: 4700, 1: 4600}),
                payment_history=history({0: 470, 1: 460}),
                credit_history=history({0: 12000, 1: 12000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], existing_summary)
        write_summary_rows(spark, config["latest_history_table"], existing_summary)

        batch_ts = datetime(2026, 2, 1, 12, 0, 0)
        source_rows = [
            build_source_row(1001, "2026-01", batch_ts, balance=3000, actual_payment=300),
            build_source_row(2001, "2026-01", batch_ts, balance=5200, actual_payment=520),
            build_source_row(3001, "2025-12", batch_ts, balance=4800, actual_payment=480),
            build_source_row(4001, "2025-12", batch_ts, balance=7000, actual_payment=700),
            build_source_row(4001, "2026-01", batch_ts, balance=6800, actual_payment=680),
        ]
        write_source_rows(spark, config["source_table"], source_rows)

        _run_pipeline(spark, config)

        row_2001 = fetch_single_row(spark, config["destination_table"], 2001, "2026-01")
        _assert_equal(row_2001["balance_am_history"][0], 5200, "Case II current value")
        _assert_equal(row_2001["balance_am_history"][1], 5000, "Case II shifted history")

        row_3001 = fetch_single_row(spark, config["destination_table"], 3001, "2026-01")
        _assert_equal(row_3001["balance_am_history"][1], 4800, "Case III propagated history")

        row_4001 = fetch_single_row(spark, config["destination_table"], 4001, "2026-01")
        _assert_equal(row_4001["balance_am_history"][1], 7000, "Case IV chained value")

        print("[PASS] all_scenarios")
    finally:
        spark.stop()


def run_backfill_test(namespace: str = "main_backfill"):
    spark, config = _initialize(namespace, "main_backfill")
    try:
        old_ts = datetime(2025, 12, 1, 0, 0, 0)
        new_ts = datetime(2026, 2, 1, 0, 0, 0)
        rows = [
            build_summary_row(
                5001,
                "2025-12",
                old_ts,
                balance=5000,
                actual_payment=500,
                balance_history=history({0: 5000}),
                payment_history=history({0: 500}),
                credit_history=history({0: 10000}),
                past_due_history=history({0: 0}),
                rating_history=history({0: "0"}),
                dpd_history=history({0: 0}),
                asset_history=history({0: "A"}),
            ),
            build_summary_row(
                5001,
                "2026-01",
                old_ts,
                balance=5100,
                actual_payment=510,
                balance_history=history({0: 5100, 1: 5000}),
                payment_history=history({0: 510, 1: 500}),
                credit_history=history({0: 10000, 1: 10000}),
                past_due_history=history({0: 0, 1: 0}),
                rating_history=history({0: "0", 1: "0"}),
                dpd_history=history({0: 0, 1: 0}),
                asset_history=history({0: "A", 1: "A"}),
            ),
        ]
        write_summary_rows(spark, config["destination_table"], rows)
        write_summary_rows(spark, config["latest_history_table"], [rows[-1]])
        write_source_rows(
            spark,
            config["source_table"],
            [build_source_row(5001, "2025-12", new_ts, balance=4900, actual_payment=490)],
        )

        _run_pipeline(spark, config)
        jan = fetch_single_row(spark, config["destination_table"], 5001, "2026-01")
        _assert_equal(jan["balance_am_history"][1], 4900, "Backfill propagated")
        _assert_equal(jan["base_ts"], new_ts, "Backfill base_ts propagated")
        print("[PASS] backfill")
    finally:
        spark.stop()


def run_duplicate_records_test(namespace: str = "main_duplicates"):
    spark, config = _initialize(namespace, "main_duplicates")
    try:
        ts1 = datetime(2026, 2, 1, 10, 0, 0)
        ts2 = datetime(2026, 2, 1, 11, 0, 0)
        ts3 = datetime(2026, 2, 1, 12, 0, 0)
        write_source_rows(
            spark,
            config["source_table"],
            [
                build_source_row(12001, "2026-01", ts1, balance=5000, actual_payment=100),
                build_source_row(12001, "2026-01", ts2, balance=5500, actual_payment=150),
                build_source_row(12001, "2026-01", ts3, balance=5600, actual_payment=160),
            ],
        )
        _run_pipeline(spark, config)
        row = fetch_single_row(spark, config["destination_table"], 12001, "2026-01")
        _assert_equal(row["balance_am"], 5600, "Latest duplicate wins")
        print("[PASS] duplicate_records")
    finally:
        spark.stop()


def run_null_update_test(namespace: str = "main_null"):
    spark, config = _initialize(namespace, "main_null")
    try:
        old_ts = datetime(2025, 12, 1, 0, 0, 0)
        ts = datetime(2026, 2, 1, 0, 0, 0)
        existing = build_summary_row(
            14001,
            "2025-12",
            old_ts,
            balance=5000,
            actual_payment=500,
            balance_history=history({0: 5000}),
            payment_history=history({0: 500}),
            credit_history=history({0: 10000}),
            past_due_history=history({0: 0}),
            rating_history=history({0: "0"}),
            dpd_history=history({0: 0}),
            asset_history=history({0: "A"}),
        )
        write_summary_rows(spark, config["destination_table"], [existing])
        write_summary_rows(spark, config["latest_history_table"], [existing])
        write_source_rows(
            spark,
            config["source_table"],
            [
                build_source_row(14001, "2026-01", ts, balance=-1, actual_payment=-1),
                build_source_row(14002, "2026-01", ts, balance=-1, actual_payment=-1),
            ],
        )
        _run_pipeline(spark, config)
        row1 = fetch_single_row(spark, config["destination_table"], 14001, "2026-01")
        row2 = fetch_single_row(spark, config["destination_table"], 14002, "2026-01")
        _assert_equal(row1["balance_am_history"][0], None, "Case II sentinel->NULL")
        _assert_equal(row2["balance_am_history"][0], None, "Case I sentinel->NULL")
        print("[PASS] null_update")
    finally:
        spark.stop()


def run_bulk_historical_load_test(namespace: str = "main_bulk"):
    spark, config = _initialize(namespace, "main_bulk")
    try:
        ts = datetime(2026, 2, 1, 0, 0, 0)
        months = _month_iter(2023, 1, 48)
        bal = 10000
        rows = []
        for mo in months:
            rows.append(build_source_row(9001, mo, ts, balance=bal, actual_payment=max(100, bal // 20)))
            bal -= 50
        write_source_rows(spark, config["source_table"], rows)
        _run_pipeline(spark, config)
        latest = fetch_single_row(spark, config["latest_history_table"], 9001, months[-1])
        _assert_equal(len(latest["balance_am_history"]), 36, "Bulk history length")
        print("[PASS] bulk_historical_load")
    finally:
        spark.stop()


def run_full_46_columns_test(namespace: str = "main_full46"):
    spark, config = _initialize(namespace, "main_full46")
    try:
        ts = datetime(2026, 2, 1, 0, 0, 0)
        row = build_source_row(16001, "2026-01", ts, balance=6000, actual_payment=600)
        row["high_credit_am_4in"] = 6500
        row["acct_type_dtl_cd"] = "5"
        write_source_rows(spark, config["source_table"], [row])
        _run_pipeline(spark, config)
        out = fetch_single_row(spark, config["destination_table"], 16001, "2026-01")
        _assert_true(len(out.asDict()) >= 46, "Full output shape present")
        _assert_equal(out["orig_loan_am"], 6500, "orig_loan_am inferred")
        print("[PASS] full_46_columns")
    finally:
        spark.stop()


def run_recovery_test(namespace: str = "main_recovery"):
    spark, config = _initialize(namespace, "main_recovery")
    try:
        ts1 = datetime(2026, 2, 1, 0, 0, 0)
        ts2 = datetime(2026, 2, 2, 0, 0, 0)
        write_source_rows(spark, config["source_table"], [build_source_row(17001, "2026-01", ts1, 5000, 500)])
        _run_pipeline(spark, config)
        before = spark.table(config["destination_table"]).filter(F.col("cons_acct_key") == 17001).count()
        _run_pipeline(spark, config)
        after = spark.table(config["destination_table"]).filter(F.col("cons_acct_key") == 17001).count()
        _assert_equal(before, after, "Idempotent rerun")
        write_source_rows(spark, config["source_table"], [build_source_row(17001, "2026-02", ts2, 5200, 520)])
        _run_pipeline(spark, config)
        latest = fetch_single_row(spark, config["latest_history_table"], 17001, "2026-02")
        _assert_equal(latest["rpt_as_of_mo"], "2026-02", "Recovery forward update")
        print("[PASS] recovery")
    finally:
        spark.stop()


def run_comprehensive_edge_cases_test():
    run_null_update_test("main_edge_null")
    run_duplicate_records_test("main_edge_dup")
    run_backfill_test("main_edge_backfill")
    print("[PASS] comprehensive_edge_cases")


def run_comprehensive_50_cases_test(namespace: str = "main_50_cases"):
    spark, config = _initialize(namespace, "main_50_cases")
    try:
        random.seed(42)
        old_ts = datetime(2025, 12, 1, 0, 0, 0)
        batch_ts = datetime(2026, 2, 1, 0, 0, 0)

        existing = []
        for i in range(10):
            acct = 19000 + i
            row = build_summary_row(acct, "2025-12", old_ts, 4000 + i, 400 + i)
            existing.append(row)
        write_summary_rows(spark, config["destination_table"], existing)
        write_summary_rows(spark, config["latest_history_table"], existing)

        source_rows = []
        for i in range(20):
            source_rows.append(build_source_row(19200 + i, "2026-01", batch_ts, 3000 + i, 300 + i))
        for i in range(10):
            source_rows.append(build_source_row(19000 + i, "2026-01", batch_ts, 4500 + i, 450 + i))
        for i in range(10):
            acct = 19300 + i
            source_rows.append(build_source_row(acct, "2025-12", batch_ts, 6000 + i, 600 + i))
            source_rows.append(build_source_row(acct, "2026-01", batch_ts, 5800 + i, 580 + i))
        write_source_rows(spark, config["source_table"], source_rows)
        _run_pipeline(spark, config)
        produced = spark.table(config["destination_table"]).filter(F.col("cons_acct_key") >= 19000).count()
        _assert_true(produced >= 40, "Large scenario batch processed")
        print("[PASS] comprehensive_50_cases")
    finally:
        spark.stop()


PERF_SCALES = {
    "TINY": {"new": 20, "forward": 10, "backfill": 8, "bulk": 5, "bulk_months": 8},
    "SMALL": {"new": 200, "forward": 100, "backfill": 80, "bulk": 40, "bulk_months": 12},
    "MEDIUM": {"new": 1000, "forward": 500, "backfill": 350, "bulk": 150, "bulk_months": 18},
    "LARGE": {"new": 4000, "forward": 2000, "backfill": 1200, "bulk": 600, "bulk_months": 24},
}


def run_performance_benchmark(scale: str = "TINY", namespace: str = "main_perf"):
    scale = scale.upper()
    if scale not in PERF_SCALES:
        raise ValueError(f"Unknown scale {scale}")
    spark, config = _initialize(namespace, f"main_perf_{scale}")
    try:
        cfg = PERF_SCALES[scale]
        old_ts = datetime(2025, 12, 1, 0, 0, 0)
        batch_ts = datetime(2026, 2, 1, 0, 0, 0)

        existing = []
        for i in range(cfg["forward"]):
            existing.append(build_summary_row(20000 + i, "2025-12", old_ts, 5000, 500))
        write_summary_rows(spark, config["destination_table"], existing)
        write_summary_rows(spark, config["latest_history_table"], existing)

        rows = []
        for i in range(cfg["new"]):
            rows.append(build_source_row(22000 + i, "2026-01", batch_ts, 3000 + (i % 50), 300))
        for i in range(cfg["forward"]):
            rows.append(build_source_row(20000 + i, "2026-01", batch_ts, 5300 + (i % 50), 530))
        months = _month_iter(2025, 1, cfg["bulk_months"])
        for i in range(cfg["bulk"]):
            acct = 23000 + i
            bal = 8000
            for mo in months:
                rows.append(build_source_row(acct, mo, batch_ts, bal, max(100, bal // 20)))
                bal -= 20

        write_source_rows(spark, config["source_table"], rows)
        t0 = time.time()
        _run_pipeline(spark, config)
        elapsed = time.time() - t0
        throughput = len(rows) / elapsed if elapsed > 0 else 0
        print(f"[PASS] performance_benchmark scale={scale} rows={len(rows):,} elapsed={elapsed:.2f}s throughput={throughput:,.1f}/s")
        return {"scale": scale, "rows": len(rows), "elapsed": elapsed, "throughput": throughput}
    finally:
        spark.stop()


def cli_performance():
    parser = argparse.ArgumentParser(description="Main pipeline performance benchmark")
    parser.add_argument("--scale", default="TINY", choices=list(PERF_SCALES.keys()))
    args = parser.parse_args()
    run_performance_benchmark(args.scale)
