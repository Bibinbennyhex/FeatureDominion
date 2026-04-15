"""
Scaled simulation for 50B summary vs 100M updates (ratio 500:1).

Compares:
- baseline: main/summary_inc_baseline.py (committed version)
- previous: main/summary_inc_optimized.py
- current:  main/summary_inc.py (working version)

This is not a literal 50B/100M run; it is a scale-preserving simulation.
"""

from __future__ import annotations

import argparse
import importlib
import time
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from test_utils import (
    SOURCE_SCHEMA_SPEC,
    SUMMARY_SCHEMA_SPEC,
    create_spark_session,
    load_main_test_config,
    reset_tables,
)


def _build_int_history(curr_col):
    return F.concat(
        F.array(curr_col.cast("int")),
        F.array_repeat(F.lit(None).cast("int"), 35),
    )


def _build_str_history(curr_col):
    return F.concat(
        F.array(curr_col.cast("string")),
        F.array_repeat(F.lit(None).cast("string"), 35),
    )


def _summary_seed_df(spark, count: int, month: str, base_ts: str) -> DataFrame:
    date_expr = F.to_date(F.lit(f"{month}-01"))
    ts_expr = F.to_timestamp(F.lit(base_ts))

    base = spark.range(1, count + 1).select(F.col("id").cast("long").alias("cons_acct_key"))
    bal = (F.lit(5000) + (F.col("cons_acct_key") % 100).cast("int")).cast("int")
    pay = (F.lit(500) + (F.col("cons_acct_key") % 20).cast("int")).cast("int")
    credit = F.lit(10000).cast("int")
    past_due = F.lit(0).cast("int")
    dpd = F.lit(0).cast("int")
    rating = F.lit("0")
    asset = F.lit("A")

    cols = {
        "cons_acct_key": F.col("cons_acct_key"),
        "bureau_member_id": F.concat(F.lit("MBR"), F.col("cons_acct_key").cast("string")),
        "portfolio_rating_type_cd": F.lit("PT"),
        "acct_type_dtl_cd": F.lit("5"),
        "open_dt": date_expr,
        "closed_dt": F.lit(None).cast("date"),
        "pymt_terms_cd": F.lit("01"),
        "pymt_terms_dtl_cd": F.lit("M"),
        "acct_dt": date_expr,
        "acct_stat_cd": F.lit("1"),
        "acct_pymt_stat_cd": F.lit("0"),
        "acct_pymt_stat_dtl_cd": F.lit("0"),
        "credit_limit_am": credit,
        "balance_am": bal,
        "past_due_am": past_due,
        "actual_payment_am": pay,
        "last_payment_dt": F.lit(None).cast("date"),
        "schd_pymt_dt": date_expr,
        "emi_amt": pay,
        "collateral_cd": F.lit("N"),
        "orig_pymt_due_dt": date_expr,
        "dflt_status_dt": F.lit(None).cast("date"),
        "write_off_am": F.lit(0).cast("int"),
        "asset_class_cd": asset,
        "days_past_due": dpd,
        "hi_credit_am": credit,
        "cash_limit_am": F.lit(0).cast("int"),
        "collateral_am": F.lit(0).cast("int"),
        "charge_off_am": F.lit(0).cast("int"),
        "principal_write_off_am": F.lit(0).cast("int"),
        "settled_am": F.lit(0).cast("int"),
        "interest_rate": F.lit(0).cast("int"),
        "suit_filed_willful_dflt": F.lit("N"),
        "written_off_and_settled_status": F.lit("N"),
        "soft_del_cd": F.lit(""),
        "base_ts": ts_expr,
        "rpt_as_of_mo": F.lit(month),
        "orig_loan_am": credit,
        "payment_rating_cd": rating,
        "actual_payment_am_history": _build_int_history(pay),
        "balance_am_history": _build_int_history(bal),
        "credit_limit_am_history": _build_int_history(credit),
        "past_due_am_history": _build_int_history(past_due),
        "payment_rating_cd_history": _build_str_history(rating),
        "days_past_due_history": _build_int_history(dpd),
        "asset_class_cd_4in_history": _build_str_history(asset),
        "payment_history_grid": F.concat(F.lit("0"), F.expr("repeat('?', 35)")),
    }

    return base.select(*[cols[name].alias(name) for name, _ in SUMMARY_SCHEMA_SPEC])


def _source_updates_df(spark, update_rows: int, month: str, base_ts: str) -> DataFrame:
    date_expr = F.to_date(F.lit(f"{month}-01"))
    ts_expr = F.to_timestamp(F.lit(base_ts))

    base = spark.range(1, update_rows + 1).select(F.col("id").cast("long").alias("cons_acct_key"))
    bal = (F.lit(5100) + (F.col("cons_acct_key") % 100).cast("int")).cast("int")
    pay = (F.lit(520) + (F.col("cons_acct_key") % 20).cast("int")).cast("int")
    credit = F.lit(10000).cast("int")

    cols = {
        "cons_acct_key": F.col("cons_acct_key"),
        "bureau_mbr_id": F.concat(F.lit("MBR"), F.col("cons_acct_key").cast("string")),
        "port_type_cd": F.lit("PT"),
        "acct_type_dtl_cd": F.lit("5"),
        "acct_open_dt": date_expr,
        "acct_closed_dt": F.lit(None).cast("date"),
        "pymt_terms_cd": F.lit("01"),
        "pymt_terms_dtl_cd": F.lit("M"),
        "acct_dt": date_expr,
        "acct_stat_cd": F.lit("1"),
        "acct_pymt_stat_cd": F.lit("0"),
        "acct_pymt_stat_dtl_cd": F.lit("0"),
        "acct_credit_ext_am": credit,
        "acct_bal_am": bal,
        "past_due_am": F.lit(0).cast("int"),
        "actual_pymt_am": pay,
        "last_pymt_dt": F.lit(None).cast("date"),
        "schd_pymt_dt": date_expr,
        "next_schd_pymt_am": pay,
        "collateral_cd": F.lit("N"),
        "orig_pymt_due_dt": date_expr,
        "write_off_dt": F.lit(None).cast("date"),
        "write_off_am": F.lit(0).cast("int"),
        "asset_class_cd_4in": F.lit("A"),
        "days_past_due_ct_4in": F.lit(0).cast("int"),
        "high_credit_am_4in": credit,
        "cash_limit_am_4in": F.lit(0).cast("int"),
        "collateral_am_4in": F.lit(0).cast("int"),
        "total_write_off_am_4in": F.lit(0).cast("int"),
        "principal_write_off_am_4in": F.lit(0).cast("int"),
        "settled_am_4in": F.lit(0).cast("int"),
        "interest_rate_4in": F.lit(0).cast("int"),
        "suit_filed_wilful_def_stat_cd_4in": F.lit("N"),
        "wo_settled_stat_cd_4in": F.lit("N"),
        "soft_del_cd": F.lit(""),
        "base_ts": ts_expr,
        "rpt_as_of_mo": F.lit(month),
        "insert_ts": ts_expr,
        "update_ts": ts_expr,
    }
    return base.select(*[cols[name].alias(name) for name, _ in SOURCE_SCHEMA_SPEC])


def _append_aligned(spark, df: DataFrame, table_name: str) -> None:
    target_cols = spark.table(table_name).columns
    df.select(*[c for c in target_cols if c in df.columns]).writeTo(table_name).append()


def _run_variant(
    module_name: str,
    label: str,
    namespace: str,
    summary_rows: int,
    update_rows: int,
) -> Dict[str, float]:
    spark = create_spark_session(f"sim_{module_name}_{summary_rows}_{update_rows}")
    config = load_main_test_config(namespace)
    pipeline = importlib.import_module(module_name)

    # Keep strategy consistent for comparison.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")

    try:
        reset_tables(spark, config)

        # Seed existing summary/latest at max existing month.
        t0 = time.time()
        summary_seed = _summary_seed_df(spark, summary_rows, "2025-12", "2026-01-01 00:00:00")
        _append_aligned(spark, summary_seed, config["destination_table"])
        _append_aligned(spark, summary_seed, config["latest_history_table"])
        seed_summary_sec = time.time() - t0

        # Seed updates in an older month -> Case III backfill updates for existing accounts.
        t1 = time.time()
        updates = _source_updates_df(spark, update_rows, "2025-11", "2026-02-01 00:00:00")
        _append_aligned(spark, updates, config["source_table"])
        seed_source_sec = time.time() - t1

        t2 = time.time()
        pipeline.cleanup(spark)
        t3 = time.time()
        pipeline.run_pipeline(spark, config)
        t4 = time.time()

        source_count = spark.table(config["source_table"]).count()

        return {
            "variant": label,
            "module": module_name,
            "summary_rows": float(summary_rows),
            "update_rows": float(update_rows),
            "seed_summary_sec": seed_summary_sec,
            "seed_source_sec": seed_source_sec,
            "cleanup_sec": t3 - t2,
            "pipeline_sec": t4 - t3,
            "total_sec": t4 - t2,
            "throughput_rows_per_sec": (source_count / (t4 - t3)) if (t4 - t3) > 0 else 0.0,
        }
    finally:
        spark.stop()


def _print_table(rows: List[Dict[str, float]]) -> None:
    headers = [
        "variant",
        "module",
        "summary_rows",
        "update_rows",
        "seed_summary_sec",
        "seed_source_sec",
        "cleanup_sec",
        "pipeline_sec",
        "total_sec",
        "throughput_rows_per_sec",
    ]
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        vals = []
        for h in headers:
            v = r[h]
            if h in {"variant", "module"}:
                vals.append(str(v))
            elif h.endswith("_rows"):
                vals.append(f"{int(v):,}")
            else:
                vals.append(f"{v:,.2f}")
        print("| " + " | ".join(vals) + " |")


def main():
    parser = argparse.ArgumentParser(description="Scaled simulation compare for 50B summary / 100M updates")
    parser.add_argument("--summary-rows", type=int, default=1_000_000, help="Scaled summary row count")
    parser.add_argument("--update-rows", type=int, default=2_000, help="Scaled update row count")
    args = parser.parse_args()

    ratio = args.summary_rows / max(1, args.update_rows)
    print(f"[INFO] Simulation ratio summary:update = {ratio:.2f}:1 (target 500:1)")

    module_specs = [
        ("baseline", "summary_inc_baseline", "sim_baseline_50b_100m"),
        ("previous", "summary_inc_optimized", "sim_previous_50b_100m"),
        ("current", "summary_inc", "sim_current_50b_100m"),
    ]

    results: List[Dict[str, float]] = []
    for label, module_name, namespace in module_specs:
        print(f"[RUN] {label} module = {module_name}")
        results.append(
            _run_variant(
                module_name=module_name,
                label=label,
                namespace=namespace,
                summary_rows=args.summary_rows,
                update_rows=args.update_rows,
            )
        )

    print("\nSimulation Metrics")
    _print_table(results)

    baseline = next((r for r in results if r["variant"] == "baseline"), None)
    if baseline and baseline["pipeline_sec"] > 0:
        print("\nSpeedup vs baseline (pipeline_sec)")
        for r in results:
            if r["variant"] == "baseline":
                continue
            speedup = baseline["pipeline_sec"] / r["pipeline_sec"] if r["pipeline_sec"] > 0 else 0.0
            delta_pct = ((baseline["pipeline_sec"] - r["pipeline_sec"]) / baseline["pipeline_sec"]) * 100.0
            print(
                f"[RESULT] {r['variant']}_vs_baseline_speedup={speedup:.3f}x, "
                f"pipeline_time_reduction={delta_pct:.2f}%"
            )


if __name__ == "__main__":
    main()
