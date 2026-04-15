"""
Large-scale benchmark runner for summary pipeline variants.

Default workload:
- 1,000,000 Case I rows
- 1,000,000 Case II rows
- 1,000,000 Case III (normal backfill) rows
- 1,000,000 Case III (soft delete) rows
"""

from __future__ import annotations

import argparse
import importlib
import time
from datetime import datetime
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from test_utils import (
    SOURCE_SCHEMA_SPEC,
    SUMMARY_SCHEMA_SPEC,
    create_spark_session,
    load_main_test_config,
)


CASE1_START = 1
CASE2_START = 2_000_001
CASE3_START = 4_000_001
CASE3D_START = 6_000_001


def _schema_sql(spec) -> str:
    return ",\n            ".join([f"{name} {sql_type}" for name, sql_type in spec])


def _reset_tables_bucketed(spark, config: Dict) -> None:
    source_table = config["source_table"]
    summary_table = config["destination_table"]
    latest_table = config["latest_history_table"]
    namespace = source_table.rsplit(".", 1)[0]
    tracker_table = config.get("watermark_tracker_table") or f"{namespace}.summary_watermark_tracker"

    latest_schema_spec = list(SUMMARY_SCHEMA_SPEC)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS execution_catalog.checkpointdb")

    for table in [source_table, summary_table, latest_table, tracker_table]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    for temp_case in ["case_1", "case_2", "case_3a", "case_3b", "case_3d_month", "case_3d_future", "case_4"]:
        spark.sql(f"DROP TABLE IF EXISTS execution_catalog.checkpointdb.{temp_case}")

    spark.sql(
        f"""
        CREATE TABLE {source_table} (
            {_schema_sql(SOURCE_SCHEMA_SPEC)}
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
        """
    )

    # summary: partition by month and clustered by account key bucket(64)
    spark.sql(
        f"""
        CREATE TABLE {summary_table} (
            {_schema_sql(SUMMARY_SCHEMA_SPEC)}
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo, bucket(64, cons_acct_key))
        """
    )

    # latest_summary: clustered by account key bucket(64)
    spark.sql(
        f"""
        CREATE TABLE {latest_table} (
            {_schema_sql(latest_schema_spec)}
        )
        USING iceberg
        PARTITIONED BY (bucket(64, cons_acct_key))
        """
    )


def _build_int_history(curr_col, prev_col=None):
    null_tail = 34 if prev_col is not None else 35
    if prev_col is not None:
        return F.concat(
            F.array(curr_col.cast("int"), prev_col.cast("int")),
            F.array_repeat(F.lit(None).cast("int"), null_tail),
        )
    return F.concat(
        F.array(curr_col.cast("int")),
        F.array_repeat(F.lit(None).cast("int"), null_tail),
    )


def _build_str_history(curr_col, prev_col=None):
    null_tail = 34 if prev_col is not None else 35
    if prev_col is not None:
        return F.concat(
            F.array(curr_col.cast("string"), prev_col.cast("string")),
            F.array_repeat(F.lit(None).cast("string"), null_tail),
        )
    return F.concat(
        F.array(curr_col.cast("string")),
        F.array_repeat(F.lit(None).cast("string"), null_tail),
    )


def _summary_df(
    spark,
    start_id: int,
    count: int,
    month: str,
    base_ts: str,
    bal_base: int,
    pay_base: int,
    include_prev: bool,
) -> DataFrame:
    date_expr = F.to_date(F.lit(f"{month}-01"))
    ts_expr = F.to_timestamp(F.lit(base_ts))

    base = spark.range(start_id, start_id + count).select(F.col("id").cast("long").alias("cons_acct_key"))
    bal = (F.lit(bal_base) + (F.col("cons_acct_key") % 100).cast("int")).cast("int")
    pay = (F.lit(pay_base) + (F.col("cons_acct_key") % 20).cast("int")).cast("int")
    credit = F.lit(10000).cast("int")
    past_due = F.lit(0).cast("int")
    dpd = F.lit(0).cast("int")
    rating = F.lit("0")
    asset = F.lit("A")

    prev_bal = (bal - F.lit(50)).cast("int") if include_prev else None
    prev_pay = (pay - F.lit(5)).cast("int") if include_prev else None
    prev_credit = credit if include_prev else None
    prev_past_due = past_due if include_prev else None
    prev_dpd = dpd if include_prev else None
    prev_rating = rating if include_prev else None
    prev_asset = asset if include_prev else None

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
        "actual_payment_am_history": _build_int_history(pay, prev_pay),
        "balance_am_history": _build_int_history(bal, prev_bal),
        "credit_limit_am_history": _build_int_history(credit, prev_credit),
        "past_due_am_history": _build_int_history(past_due, prev_past_due),
        "payment_rating_cd_history": _build_str_history(rating, prev_rating),
        "days_past_due_history": _build_int_history(dpd, prev_dpd),
        "asset_class_cd_4in_history": _build_str_history(asset, prev_asset),
        "payment_history_grid": F.concat(F.lit("0"), F.expr("repeat('?', 35)")),
    }

    select_exprs = [cols[name].alias(name) for name, _ in SUMMARY_SCHEMA_SPEC]
    return base.select(*select_exprs)


def _source_df(
    spark,
    start_id: int,
    count: int,
    month: str,
    base_ts: str,
    bal_base: int,
    pay_base: int,
    soft_del_cd: str,
) -> DataFrame:
    date_expr = F.to_date(F.lit(f"{month}-01"))
    ts_expr = F.to_timestamp(F.lit(base_ts))

    base = spark.range(start_id, start_id + count).select(F.col("id").cast("long").alias("cons_acct_key"))
    bal = (F.lit(bal_base) + (F.col("cons_acct_key") % 100).cast("int")).cast("int")
    pay = (F.lit(pay_base) + (F.col("cons_acct_key") % 20).cast("int")).cast("int")
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
        "soft_del_cd": F.lit(soft_del_cd),
        "base_ts": ts_expr,
        "rpt_as_of_mo": F.lit(month),
        "insert_ts": ts_expr,
        "update_ts": ts_expr,
    }
    select_exprs = [cols[name].alias(name) for name, _ in SOURCE_SCHEMA_SPEC]
    return base.select(*select_exprs)


def _seed_tables(spark, config: Dict, records_per_case: int) -> Dict[str, float]:
    timings: Dict[str, float] = {}

    t0 = time.time()
    summary_table = config["destination_table"]
    latest_table = config["latest_history_table"]
    source_table = config["source_table"]

    def append_aligned(df: DataFrame, table_name: str) -> None:
        target_cols = spark.table(table_name).columns
        df.select(*[c for c in target_cols if c in df.columns]).writeTo(table_name).append()

    # Seed summary + latest for Case II (forward).
    case2_latest = _summary_df(
        spark, CASE2_START, records_per_case, "2025-12", "2026-01-01 00:00:00", 5000, 500, include_prev=False
    )
    append_aligned(case2_latest, summary_table)
    append_aligned(case2_latest, latest_table)

    # Seed summary + latest for Case III (normal backfill).
    case3_prior = _summary_df(
        spark, CASE3_START, records_per_case, "2025-12", "2026-01-01 00:00:00", 4800, 480, include_prev=False
    )
    case3_curr = _summary_df(
        spark, CASE3_START, records_per_case, "2026-01", "2026-01-01 00:00:00", 5000, 500, include_prev=True
    )
    append_aligned(case3_prior, summary_table)
    append_aligned(case3_curr, summary_table)
    append_aligned(case3_curr, latest_table)

    # Seed summary + latest for Case III soft-delete.
    case3d_prior = _summary_df(
        spark, CASE3D_START, records_per_case, "2025-12", "2026-01-01 00:00:00", 5200, 520, include_prev=False
    )
    case3d_curr = _summary_df(
        spark, CASE3D_START, records_per_case, "2026-01", "2026-01-01 00:00:00", 5400, 540, include_prev=True
    )
    append_aligned(case3d_prior, summary_table)
    append_aligned(case3d_curr, summary_table)
    append_aligned(case3d_curr, latest_table)
    timings["seed_summary_latest_sec"] = time.time() - t0

    t1 = time.time()
    # Source rows: 1M each for Case I / II / III / III-soft-delete
    case1_src = _source_df(
        spark, CASE1_START, records_per_case, "2026-01", "2026-02-01 00:00:00", 3000, 300, "0"
    )
    case2_src = _source_df(
        spark, CASE2_START, records_per_case, "2026-01", "2026-02-01 00:00:00", 5300, 530, "0"
    )
    case3_src = _source_df(
        spark, CASE3_START, records_per_case, "2025-12", "2026-02-01 00:00:00", 4700, 470, "0"
    )
    case3d_src = _source_df(
        spark, CASE3D_START, records_per_case, "2025-12", "2026-02-01 00:00:00", 5100, 510, "1"
    )
    source_all = case1_src.unionByName(case2_src).unionByName(case3_src).unionByName(case3d_src)
    source_all.writeTo(source_table).append()
    timings["seed_source_sec"] = time.time() - t1

    return timings


def _run_one_variant(
    variant: str,
    records_per_case: int,
    bucketed_layout: bool,
    apply_bucketed_sql_conf: bool,
) -> Dict[str, float]:
    module_name = "summary_inc" if variant == "baseline" else "summary_inc_optimized"
    namespace = f"main_perf_{variant}_{records_per_case}"

    spark = create_spark_session(f"main_perf_{variant}_{records_per_case}")
    config = load_main_test_config(namespace)

    # Use slightly larger default shuffle for this benchmark.
    config["performance"]["target_records_per_partition"] = 2_000_000
    config["performance"]["min_partitions"] = 64
    config["performance"]["max_partitions"] = 512
    config["spark"]["spark.sql.shuffle.partitions"] = "256"
    config["spark"]["spark.default.parallelism"] = "256"

    if apply_bucketed_sql_conf:
        spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
        spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
        spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
        spark.conf.set("spark.sql.requireAllClusterKeysForDistribution", "false")

    pipeline = importlib.import_module(module_name)

    try:
        if bucketed_layout:
            _reset_tables_bucketed(spark, config)
        else:
            # Fallback: keep default layout from test harness if needed.
            from test_utils import reset_tables
            reset_tables(spark, config)
        seed_metrics = _seed_tables(spark, config, records_per_case)

        t0 = time.time()
        pipeline.cleanup(spark)
        t1 = time.time()
        pipeline.run_pipeline(spark, config)
        t2 = time.time()

        source_count = spark.table(config["source_table"]).count()
        summary_count = spark.table(config["destination_table"]).count()
        latest_count = spark.table(config["latest_history_table"]).count()

        total_elapsed = t2 - t0
        throughput = source_count / total_elapsed if total_elapsed > 0 else 0.0

        return {
            "variant": variant,
            "records_per_case": float(records_per_case),
            "input_rows": float(source_count),
            "summary_rows": float(summary_count),
            "latest_rows": float(latest_count),
            "seed_summary_latest_sec": seed_metrics["seed_summary_latest_sec"],
            "seed_source_sec": seed_metrics["seed_source_sec"],
            "cleanup_sec": t1 - t0,
            "pipeline_sec": t2 - t1,
            "total_sec": total_elapsed,
            "throughput_rows_per_sec": throughput,
        }
    finally:
        spark.stop()


def _print_metrics(rows: List[Dict[str, float]]) -> None:
    headers = [
        "variant",
        "records_per_case",
        "input_rows",
        "summary_rows",
        "latest_rows",
        "seed_summary_latest_sec",
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
            if isinstance(v, float) and h not in {"variant"}:
                if h.endswith("_rows") or h in {"records_per_case", "input_rows", "summary_rows", "latest_rows"}:
                    vals.append(f"{int(v):,}")
                else:
                    vals.append(f"{v:,.2f}")
            else:
                vals.append(str(v))
        print("| " + " | ".join(vals) + " |")


def main():
    parser = argparse.ArgumentParser(description="Run large benchmark for baseline/optimized summary pipeline")
    parser.add_argument("--records-per-case", type=int, default=1_000_000)
    parser.add_argument("--variant", choices=["baseline", "optimized", "both"], default="optimized")
    parser.add_argument("--bucketed-layout", action="store_true", default=True)
    parser.add_argument("--no-bucketed-layout", action="store_false", dest="bucketed_layout")
    parser.add_argument("--apply-bucketed-sql-conf", action="store_true", default=True)
    parser.add_argument("--no-bucketed-sql-conf", action="store_false", dest="apply_bucketed_sql_conf")
    args = parser.parse_args()

    variants = ["baseline", "optimized"] if args.variant == "both" else [args.variant]
    metrics = []
    for variant in variants:
        print(f"[RUN] variant={variant}, records_per_case={args.records_per_case:,}")
        row = _run_one_variant(
            variant,
            args.records_per_case,
            bucketed_layout=args.bucketed_layout,
            apply_bucketed_sql_conf=args.apply_bucketed_sql_conf,
        )
        metrics.append(row)
        print(f"[DONE] variant={variant} pipeline_sec={row['pipeline_sec']:.2f}s throughput={row['throughput_rows_per_sec']:.1f}/s")

    print("\nBenchmark Metrics")
    _print_metrics(metrics)


if __name__ == "__main__":
    main()
