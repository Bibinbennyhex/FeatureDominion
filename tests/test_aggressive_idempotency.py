"""
Aggressive idempotency stress test for main/summary_inc.py.

What this test does:
1. Seeds a realistic baseline with existing + anchor accounts.
2. Repeats multiple mixed batches (Case I/II/III/IV + duplicates + null sentinels).
3. For each batch:
   - Runs pipeline once (state-changing run).
   - Runs pipeline again with no new data (idempotency rerun).
   - Validates:
     - summary unchanged on rerun
     - latest_summary unchanged on rerun
     - no duplicate keys in summary/latest
     - latest_summary equals latest(summary) by account
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from pyspark.sql import Window
from pyspark.sql import functions as F

from test_utils import (
    assert_watermark_tracker_consistent,
    build_source_row,
    build_summary_row,
    create_spark_session,
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

HORIZON_MIN = "2026-09"
HORIZON_MAX = "2026-12"

SCALES = {
    "TINY": {"seed_existing": 40, "case_1": 12, "case_2": 12, "case_3": 10, "case_4": 8, "cycles": 3},
    "SMALL": {"seed_existing": 80, "case_1": 24, "case_2": 24, "case_3": 20, "case_4": 16, "cycles": 4},
    "MEDIUM": {"seed_existing": 160, "case_1": 48, "case_2": 48, "case_3": 40, "case_4": 32, "cycles": 5},
}


def _assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected={expected}, actual={actual}")


def _month_to_int(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 12 + int(mon)


def _int_to_month(value: int) -> str:
    year = value // 12
    mon = value % 12
    if mon == 0:
        year -= 1
        mon = 12
    return f"{year:04d}-{mon:02d}"


def _add_month(month: str, delta: int) -> str:
    return _int_to_month(_month_to_int(month) + delta)


def _snapshot_table(spark, table_name: str) -> str:
    # Exact, deterministic snapshot for rerun equality checks.
    rows = (
        spark.table(table_name)
        .orderBy(F.col("cons_acct_key").asc(), F.col("rpt_as_of_mo").asc(), F.col("base_ts").asc())
        .collect()
    )
    payload = [r.asDict(recursive=True) for r in rows]
    return json.dumps(payload, default=str, sort_keys=True)


def _assert_no_duplicates(spark, table_name: str, keys: List[str], label: str):
    dup_count = (
        spark.table(table_name)
        .groupBy(*[F.col(k) for k in keys])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    _assert_equal(dup_count, 0, f"{label} should not have duplicate keys {keys}")


def _assert_latest_matches_summary(spark, summary_table: str, latest_table: str):
    summary_df = spark.table(summary_table)
    latest_df = spark.table(latest_table)

    w = Window.partitionBy("cons_acct_key").orderBy(
        F.col("rpt_as_of_mo").desc(),
        F.col("base_ts").desc(),
    )

    expected_latest_df = (
        summary_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(*latest_df.columns)
    )
    latest_df = latest_df.select(*latest_df.columns)

    missing = expected_latest_df.exceptAll(latest_df).count()
    extra = latest_df.exceptAll(expected_latest_df).count()
    if missing > 0 or extra > 0:
        raise AssertionError(f"latest_summary mismatch vs summary latest rows (missing={missing}, extra={extra})")


def _seed_baseline(spark, config: Dict, seed_existing: int):
    seed_ts = datetime(2026, 1, 1, 0, 0, 0)

    summary_rows = [
        build_summary_row(
            cons_acct_key=ANCHOR_ACCOUNT,
            rpt_as_of_mo=ANCHOR_MIN_MONTH,
            base_ts=seed_ts,
            balance=100,
            actual_payment=10,
            balance_history=history({0: 100}),
            payment_history=history({0: 10}),
            credit_history=history({0: 1000}),
            past_due_history=history({0: 0}),
            rating_history=history({0: "0"}),
            dpd_history=history({0: 0}),
            asset_history=history({0: "A"}),
        ),
        build_summary_row(
            cons_acct_key=ANCHOR_ACCOUNT,
            rpt_as_of_mo=ANCHOR_MAX_MONTH,
            base_ts=seed_ts + timedelta(seconds=1),
            balance=120,
            actual_payment=12,
            balance_history=history({0: 120, 1: 100}),
            payment_history=history({0: 12, 1: 10}),
            credit_history=history({0: 1000, 1: 1000}),
            past_due_history=history({0: 0, 1: 0}),
            rating_history=history({0: "0", 1: "0"}),
            dpd_history=history({0: 0, 1: 0}),
            asset_history=history({0: "A", 1: "A"}),
        ),
    ]
    latest_rows = [summary_rows[-1]]

    start_acct = 30000
    for i in range(seed_existing):
        acct = start_acct + i
        bal = 4000 + i
        row = build_summary_row(
            cons_acct_key=acct,
            rpt_as_of_mo="2026-10",
            base_ts=seed_ts + timedelta(seconds=2 + i),
            balance=bal,
            actual_payment=350 + (i % 50),
            balance_history=history({0: bal, 1: bal - 50}),
            payment_history=history({0: 350 + (i % 50), 1: 340 + (i % 50)}),
            credit_history=history({0: 10000, 1: 10000}),
            past_due_history=history({0: 0, 1: 0}),
            rating_history=history({0: "0", 1: "0"}),
            dpd_history=history({0: 0, 1: 0}),
            asset_history=history({0: "A", 1: "A"}),
        )
        summary_rows.append(row)
        latest_rows.append(row)

    write_summary_rows(spark, config["destination_table"], summary_rows)
    write_summary_rows(spark, config["latest_history_table"], latest_rows)

    return list(range(start_acct, start_acct + seed_existing))


def _latest_month_by_account(spark, latest_table: str) -> Dict[int, str]:
    rows = spark.table(latest_table).select("cons_acct_key", "rpt_as_of_mo").collect()
    return {int(r["cons_acct_key"]): r["rpt_as_of_mo"] for r in rows}


def _pick(rng: random.Random, items: List[int], n: int) -> List[int]:
    if not items or n <= 0:
        return []
    if n >= len(items):
        return items[:]
    return rng.sample(items, n)


def _generate_cycle_rows(
    rng: random.Random,
    cycle_idx: int,
    latest_month_map: Dict[int, str],
    next_new_acct: int,
    cfg: Dict[str, int],
) -> Tuple[List[Dict], int]:
    rows: List[Dict] = []
    cycle_ts = datetime(2026, 2, 1, 0, 0, 0) + timedelta(days=cycle_idx)

    existing_accounts = [a for a in latest_month_map.keys() if a != ANCHOR_ACCOUNT]
    case2_candidates = [a for a in existing_accounts if latest_month_map[a] < HORIZON_MAX]
    case3_candidates = [a for a in existing_accounts if latest_month_map[a] > HORIZON_MIN]

    # Case II: forward for existing accounts that are behind horizon max.
    for idx, acct in enumerate(_pick(rng, case2_candidates, cfg["case_2"])):
        latest_m = latest_month_map[acct]
        fwd_month = _add_month(latest_m, 1)
        if fwd_month > HORIZON_MAX:
            continue
        base_val = 4500 + ((acct + cycle_idx) % 300)
        bal = -1 if (idx % 13 == 0) else base_val
        pay = -1 if (idx % 13 == 0) else (250 + ((acct + cycle_idx) % 120))
        row_ts = cycle_ts + timedelta(seconds=idx * 3)
        rows.append(build_source_row(acct, fwd_month, row_ts, balance=bal, actual_payment=pay))

        # Inject duplicates with different timestamps for dedupe stress.
        if idx % 5 == 0:
            rows.append(build_source_row(acct, fwd_month, row_ts - timedelta(minutes=1), balance=base_val - 30, actual_payment=max(1, pay - 10)))
            rows.append(build_source_row(acct, fwd_month, row_ts + timedelta(minutes=1), balance=base_val + 30, actual_payment=(pay if pay < 0 else pay + 10)))

    # Case III: backfill one month behind current latest for selected accounts.
    for idx, acct in enumerate(_pick(rng, case3_candidates, cfg["case_3"])):
        latest_m = latest_month_map[acct]
        backfill_month = _add_month(latest_m, -1)
        if backfill_month < HORIZON_MIN:
            continue
        bal = 3800 + ((acct + cycle_idx + idx) % 250)
        pay = 220 + ((acct + idx) % 80)
        row_ts = cycle_ts + timedelta(hours=1, seconds=idx * 2)
        rows.append(build_source_row(acct, backfill_month, row_ts, balance=bal, actual_payment=pay))

    # Case I: brand new accounts, single month.
    for idx in range(cfg["case_1"]):
        acct = next_new_acct
        next_new_acct += 1
        month = "2026-11" if idx % 2 == 0 else "2026-12"
        row_ts = cycle_ts + timedelta(hours=2, seconds=idx)
        bal = 3000 + ((cycle_idx * 100 + idx) % 500)
        pay = 180 + (idx % 60)
        rows.append(build_source_row(acct, month, row_ts, balance=bal, actual_payment=pay))

    # Case IV: brand new accounts with two months in same batch.
    for idx in range(cfg["case_4"]):
        acct = next_new_acct
        next_new_acct += 1
        m1 = "2026-11"
        m2 = "2026-12"
        row_ts = cycle_ts + timedelta(hours=3, seconds=idx * 2)
        base_bal = 7000 + ((cycle_idx * 50 + idx) % 400)
        rows.append(build_source_row(acct, m1, row_ts, balance=base_bal, actual_payment=320 + (idx % 70)))
        rows.append(build_source_row(acct, m2, row_ts + timedelta(seconds=1), balance=base_bal - 80, actual_payment=315 + (idx % 70)))

    return rows, next_new_acct


def run_test(scale: str = "SMALL", cycles: int = 0, reruns: int = 1, seed: int = 42):
    scale = scale.upper()
    if scale not in SCALES:
        raise ValueError(f"Unsupported scale: {scale}")

    cfg = dict(SCALES[scale])
    if cycles > 0:
        cfg["cycles"] = cycles

    rng = random.Random(seed)
    spark = create_spark_session(f"main_aggressive_idempotency_{scale}")
    config = load_main_test_config("main_aggressive_idempotency")

    try:
        print(f"[SETUP] Resetting tables (scale={scale}, cycles={cfg['cycles']}, reruns={reruns})...")
        reset_tables(spark, config)
        stable_accounts = _seed_baseline(spark, config, cfg["seed_existing"])
        _assert_equal(len(stable_accounts), cfg["seed_existing"], "Baseline seed account count")

        next_new_acct = 50000

        for cycle in range(1, cfg["cycles"] + 1):
            latest_map = _latest_month_by_account(spark, config["latest_history_table"])
            rows, next_new_acct = _generate_cycle_rows(rng, cycle, latest_map, next_new_acct, cfg)

            print(f"[CYCLE {cycle}] Loading source rows: {len(rows)}")
            write_source_rows(spark, config["source_table"], rows)

            print(f"[CYCLE {cycle}] Run #1 (state-changing run)")
            main_pipeline.cleanup(spark)
            main_pipeline.run_pipeline(spark, config)
            assert_watermark_tracker_consistent(spark, config)

            _assert_no_duplicates(spark, config["destination_table"], ["cons_acct_key", "rpt_as_of_mo"], "summary")
            _assert_no_duplicates(spark, config["latest_history_table"], ["cons_acct_key"], "latest_summary")
            _assert_latest_matches_summary(spark, config["destination_table"], config["latest_history_table"])

            summary_before = _snapshot_table(spark, config["destination_table"])
            latest_before = _snapshot_table(spark, config["latest_history_table"])

            for rerun_idx in range(1, reruns + 1):
                print(f"[CYCLE {cycle}] Rerun #{rerun_idx} (idempotency check)")
                main_pipeline.cleanup(spark)
                main_pipeline.run_pipeline(spark, config)
                assert_watermark_tracker_consistent(spark, config)

                _assert_no_duplicates(spark, config["destination_table"], ["cons_acct_key", "rpt_as_of_mo"], "summary")
                _assert_no_duplicates(spark, config["latest_history_table"], ["cons_acct_key"], "latest_summary")
                _assert_latest_matches_summary(spark, config["destination_table"], config["latest_history_table"])

                summary_after = _snapshot_table(spark, config["destination_table"])
                latest_after = _snapshot_table(spark, config["latest_history_table"])

                _assert_equal(summary_after, summary_before, f"summary drifted on cycle={cycle}, rerun={rerun_idx}")
                _assert_equal(latest_after, latest_before, f"latest_summary drifted on cycle={cycle}, rerun={rerun_idx}")

            print(f"[CYCLE {cycle}] PASS")

        print("[PASS] test_aggressive_idempotency")

    finally:
        spark.stop()


def _cli():
    parser = argparse.ArgumentParser(description="Aggressive idempotency stress test")
    parser.add_argument("--scale", default="SMALL", choices=list(SCALES.keys()))
    parser.add_argument("--cycles", type=int, default=0, help="Override default cycle count for scale")
    parser.add_argument("--reruns", type=int, default=1, help="Idempotency reruns per cycle")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    run_test(scale=args.scale, cycles=args.cycles, reruns=args.reruns, seed=args.seed)


if __name__ == "__main__":
    _cli()
