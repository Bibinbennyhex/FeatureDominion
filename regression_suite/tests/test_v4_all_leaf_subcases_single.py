"""
Single mega-test covering all requested CASE I/II/III/IV leaf subcases.

This test is intentionally verbose and nightly-oriented. For every subcase:
1) Seed summary/latest_summary/source fixtures
2) Log summary/latest_summary BEFORE
3) Run pipeline
4) Log summary/latest_summary AFTER
5) Assert row/array/scalar/soft-delete invariants
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
import sys

from pyspark.sql import functions as F

from tests.v4_contract_utils import DELETE_CODES, HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


CONTROL_ACCT = 909090
CONTROL_OLD_MONTH = "2020-01"
CONTROL_NEW_MONTH = "2026-12"


class TeeStream:
    """Write output to multiple streams (stdout + file)."""

    def __init__(self, *streams) -> None:
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


@dataclass
class SourceSpec:
    month: str
    balance: int
    payment: int
    soft_del_cd: str = ""


@dataclass
class Scenario:
    subcase_id: str
    acct: int
    seed_months: List[str]
    source_specs: List[SourceSpec]
    expected_latest_month: Optional[str] = None
    expected_deleted_months: Optional[List[str]] = None


def _month_to_int(month: str) -> int:
    y = int(month[:4])
    m = int(month[5:7])
    return y * 12 + m


def _int_to_month(value: int) -> str:
    y, m = divmod(value, 12)
    if m == 0:
        y -= 1
        m = 12
    return f"{y}-{m:02d}"


def _month_range(start: str, end: str) -> List[str]:
    start_i = _month_to_int(start)
    end_i = _month_to_int(end)
    if start_i > end_i:
        return []
    return [_int_to_month(v) for v in range(start_i, end_i + 1)]


def _assert_true(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _mk_hist(values_by_month: Dict[str, int], current_month: str, length: int) -> List[Optional[int]]:
    arr: List[Optional[int]] = []
    cur = _month_to_int(current_month)
    for i in range(length):
        m = _int_to_month(cur - i)
        arr.append(values_by_month.get(m))
    return arr


def _make_seed_rows(tu, acct: int, months: Sequence[str], base_ts: datetime) -> List[Dict]:
    # newest -> oldest
    months_sorted = sorted(set(months), key=_month_to_int, reverse=True)
    values = {m: 1000 + (_month_to_int(m) % 700) for m in months_sorted}
    rows: List[Dict] = []
    for m in months_sorted:
        bal = values[m]
        pay = max(10, bal // 10)
        rows.append(
            tu.build_summary_row(
                cons_acct_key=acct,
                rpt_as_of_mo=m,
                base_ts=base_ts,
                balance=bal,
                actual_payment=pay,
                balance_history=_mk_hist(values, m, 36),
                payment_history=_mk_hist({k: max(10, v // 10) for k, v in values.items()}, m, 36),
                credit_history=[10000] * 36,
                past_due_history=[0] * 36,
                rating_history=["0"] * 36,
                dpd_history=[0] * 36,
                asset_history=["A"] * 36,
            )
        )
    return rows


def _make_latest_rows(summary_rows: List[Dict]) -> List[Dict]:
    best: Dict[int, Dict] = {}
    for row in summary_rows:
        acct = int(row["cons_acct_key"])
        if acct not in best or row["rpt_as_of_mo"] > best[acct]["rpt_as_of_mo"]:
            best[acct] = row
    return pad_latest_rows(list(best.values()))


def _row_to_log_dict(row) -> Dict:
    data = row.asDict(recursive=True)
    for c in ("balance_am_history", "actual_payment_am_history", "payment_rating_cd_history"):
        if c in data and data[c] is not None:
            # Keep complete arrays in logs (nightly verbose contract).
            data[c] = list(data[c])
    keep = [
        "cons_acct_key",
        "rpt_as_of_mo",
        "base_ts",
        "balance_am",
        "actual_payment_am",
        "soft_del_cd",
        "balance_am_history",
        "actual_payment_am_history",
        "payment_rating_cd_history",
    ]
    return {k: data.get(k) for k in keep if k in data}


def _source_row_to_log_dict(row) -> Dict:
    data = row.asDict(recursive=True)
    keep = [
        "cons_acct_key",
        "rpt_as_of_mo",
        "base_ts",
        "balance_am",
        "actual_payment_am",
        "soft_del_cd",
    ]
    return {k: data.get(k) for k in keep if k in data}


def log_table_snapshot(label: str, spark, summary_table: str, latest_table: str, account_keys: List[int], months: Optional[List[str]] = None) -> None:
    print(f"--- {label} ---")
    for table_name, table_alias in ((summary_table, "summary"), (latest_table, "latest_summary")):
        df = spark.table(table_name)
        total_rows = df.count()
        acct_count = df.select("cons_acct_key").distinct().count()
        max_month = df.agg(F.max("rpt_as_of_mo").alias("m")).first()["m"]
        print(f"[{table_alias}] total_rows={total_rows}, distinct_accounts={acct_count}, max_month={max_month}")

        scoped = df.filter(F.col("cons_acct_key").isin(account_keys))
        if months:
            scoped = scoped.filter(F.col("rpt_as_of_mo").isin(months))
        rows = scoped.orderBy("cons_acct_key", "rpt_as_of_mo").collect()
        if not rows:
            print(f"[{table_alias}] scoped_rows=0")
        else:
            print(f"[{table_alias}] scoped_rows={len(rows)}")
            for r in rows:
                print(f"[{table_alias}] {_row_to_log_dict(r)}")


def log_accounts_all_snapshot(label: str, spark, source_table: str, account_keys: List[int], months: Optional[List[str]] = None) -> None:
    print(f"--- {label} ---")
    df = spark.table(source_table)
    total_rows = df.count()
    acct_count = df.select("cons_acct_key").distinct().count()
    max_month = df.agg(F.max("rpt_as_of_mo").alias("m")).first()["m"]
    print(f"[accounts_all] total_rows={total_rows}, distinct_accounts={acct_count}, max_month={max_month}")

    scoped = df.filter(F.col("cons_acct_key").isin(account_keys))
    if months:
        scoped = scoped.filter(F.col("rpt_as_of_mo").isin(months))
    rows = scoped.orderBy("cons_acct_key", "rpt_as_of_mo").collect()
    if not rows:
        print("[accounts_all] scoped_rows=0")
    else:
        print(f"[accounts_all] scoped_rows={len(rows)}")
        for r in rows:
            print(f"[accounts_all] {_source_row_to_log_dict(r)}")


def _snapshot_control(spark, summary_table: str, latest_table: str):
    s = spark.table(summary_table).filter(F.col("cons_acct_key") == CONTROL_ACCT).orderBy("rpt_as_of_mo").collect()
    l = spark.table(latest_table).filter(F.col("cons_acct_key") == CONTROL_ACCT).orderBy("rpt_as_of_mo").collect()
    return [x.asDict(recursive=True) for x in s], [x.asDict(recursive=True) for x in l]


def _assert_hist_lengths(row, expected: int, subcase_id: str, table_name: str) -> None:
    for col in HISTORY_COLS:
        hist = list(row[col] or [])
        _assert_true(
            len(hist) == expected,
            f"[{subcase_id}] {table_name}.{col} expected len={expected}, got {len(hist)}",
        )


def _assert_scenario(spark, config: Dict, scenario: Scenario, pre_control_summary, pre_control_latest) -> None:
    subcase_id = scenario.subcase_id
    acct = scenario.acct
    summary_df = spark.table(config["destination_table"])
    latest_df = spark.table(config["latest_history_table"])

    latest_rows = latest_df.filter(F.col("cons_acct_key") == acct).collect()
    _assert_true(len(latest_rows) == 1, f"[{subcase_id}] latest_summary must have exactly 1 row for acct={acct}")
    latest_row = latest_rows[0]
    _assert_hist_lengths(latest_row, 72, subcase_id, "latest_summary")

    if scenario.expected_latest_month:
        _assert_true(
            latest_row["rpt_as_of_mo"] == scenario.expected_latest_month,
            f"[{subcase_id}] expected latest month {scenario.expected_latest_month}, got {latest_row['rpt_as_of_mo']}",
        )

    expected_delete = set(scenario.expected_deleted_months or [])
    for spec in scenario.source_specs:
        rows = summary_df.filter(
            (F.col("cons_acct_key") == acct) & (F.col("rpt_as_of_mo") == spec.month)
        ).collect()
        _assert_true(rows, f"[{subcase_id}] missing summary row for acct={acct}, month={spec.month}")
        row = max(rows, key=lambda r: r["base_ts"])
        _assert_hist_lengths(row, 36, subcase_id, "summary")

        is_delete = spec.soft_del_cd in DELETE_CODES
        if is_delete:
            _assert_true(
                str(row["soft_del_cd"]) in DELETE_CODES,
                f"[{subcase_id}] expected delete code for acct={acct}, month={spec.month}, got {row['soft_del_cd']!r}",
            )
        else:
            _assert_true(
                int(row["balance_am_history"][0]) == int(spec.balance),
                f"[{subcase_id}] summary balance_am_history[0] mismatch for acct={acct}, month={spec.month}",
            )
            _assert_true(
                int(row["actual_payment_am_history"][0]) == int(spec.payment),
                f"[{subcase_id}] summary actual_payment_am_history[0] mismatch for acct={acct}, month={spec.month}",
            )

    for m in expected_delete:
        rows = summary_df.filter((F.col("cons_acct_key") == acct) & (F.col("rpt_as_of_mo") == m)).collect()
        _assert_true(rows, f"[{subcase_id}] expected deleted month row missing: {m}")
        row = max(rows, key=lambda r: r["base_ts"])
        _assert_true(str(row["soft_del_cd"]) in DELETE_CODES, f"[{subcase_id}] expected soft-delete marker for month={m}")

    post_control_summary, post_control_latest = _snapshot_control(
        spark, config["destination_table"], config["latest_history_table"]
    )
    control_unchanged = (post_control_summary == pre_control_summary) and (post_control_latest == pre_control_latest)
    print(f"[{subcase_id}] control account unchanged: {control_unchanged}")
    _assert_true(control_unchanged, f"[{subcase_id}] control account changed unexpectedly")


def _build_scenarios() -> List[Scenario]:
    scenarios: List[Scenario] = []
    acct = 700001
    def add(subcase_id: str, seed_months: List[str], source_specs: List[SourceSpec], expected_latest_month: Optional[str], expected_deleted_months: Optional[List[str]] = None):
        nonlocal acct
        scenarios.append(
            Scenario(
                subcase_id=subcase_id,
                acct=acct,
                seed_months=seed_months,
                source_specs=source_specs,
                expected_latest_month=expected_latest_month,
                expected_deleted_months=expected_deleted_months or [],
            )
        )
        acct += 1

    # CASE I
    add("CASE_I_01", [], [SourceSpec("2026-01", 3100, 310)], "2026-01")

    # CASE II
    add("CASE_II_A_01", ["2025-12"], [SourceSpec("2026-01", 3201, 321)], "2026-01")
    add("CASE_II_A_02", ["2025-10"], [SourceSpec("2026-01", 3202, 322)], "2026-01")
    add("CASE_II_B_01", ["2025-11"], [SourceSpec("2025-12", 3203, 323), SourceSpec("2026-01", 3204, 324)], "2026-01")
    add("CASE_II_B_02", ["2025-09"], [SourceSpec("2025-11", 3205, 325), SourceSpec("2026-02", 3206, 326)], "2026-02")

    # CASE III A
    add("CASE_III_A_01", ["2026-01", "2025-12"], [SourceSpec("2026-01", 3301, 331)], "2026-01")
    add("CASE_III_A_02", ["2026-01", "2025-12", "2025-11"], [SourceSpec("2025-12", 3302, 332)], "2026-01")
    add("CASE_III_A_03", ["2026-01", "2025-10"], [SourceSpec("2025-11", 3303, 333)], "2026-01")
    add("CASE_III_A_04", ["2023-06", "2022-11"], [SourceSpec("2023-04", 3304, 334)], "2023-06")

    # CASE III B
    add("CASE_III_B_01", ["2026-01", "2025-12", "2025-11"], [SourceSpec("2026-01", 3401, 341), SourceSpec("2025-12", 3402, 342)], "2026-01")
    add("CASE_III_B_02", ["2026-01", "2025-12", "2025-11", "2025-10"], [SourceSpec("2025-11", 3403, 343), SourceSpec("2025-10", 3404, 344)], "2026-01")
    add("CASE_III_B_03", ["2026-01", "2025-12", "2025-11"], [SourceSpec("2026-01", 3405, 345), SourceSpec("2025-12", 3406, 346), SourceSpec("2025-11", 3407, 347)], "2026-01")
    add("CASE_III_B_04", ["2023-01", "2021-10"], [SourceSpec(m, 3500 + i, 350 + i) for i, m in enumerate(_month_range("2021-11", "2022-12"))], "2023-01")
    add("CASE_III_B_05", ["2023-01", "2021-10"], [SourceSpec(m, 3600 + i, 360 + i) for i, m in enumerate(_month_range("2022-05", "2022-08"))], "2023-01")
    add("CASE_III_B_06", ["2023-01", "2021-10"], [SourceSpec("2022-01", 3701, 371), SourceSpec("2022-05", 3705, 375)], "2023-01")

    # CASE III C
    add("CASE_III_C_01", ["2026-01", "2025-12"], [SourceSpec("2026-01", 3801, 381, soft_del_cd="4")], "2025-12", ["2026-01"])
    add("CASE_III_C_02", ["2026-01", "2025-12", "2025-11"], [SourceSpec("2025-12", 3802, 382, soft_del_cd="4")], "2026-01", ["2025-12"])

    # CASE III D
    add("CASE_III_D_01", ["2026-01", "2025-12", "2025-11"], [SourceSpec("2026-01", 3901, 391, soft_del_cd="4"), SourceSpec("2025-12", 3902, 392, soft_del_cd="4")], "2025-11", ["2026-01", "2025-12"])
    add("CASE_III_D_02", ["2026-01", "2025-12", "2025-11", "2025-10"], [SourceSpec("2025-11", 3903, 393, soft_del_cd="4"), SourceSpec("2025-10", 3904, 394, soft_del_cd="4")], "2026-01", ["2025-11", "2025-10"])
    add("CASE_III_D_03", ["2026-01", "2025-12"], [SourceSpec("2026-01", 3905, 395, soft_del_cd="4"), SourceSpec("2025-12", 3906, 396, soft_del_cd="4")], "2026-01", ["2026-01", "2025-12"])
    add("CASE_III_D_04", _month_range("2021-10", "2023-01"), [SourceSpec(m, 3907 + i, 397 + i, soft_del_cd="4") for i, m in enumerate(_month_range("2022-05", "2022-08"))], "2023-01", _month_range("2022-05", "2022-08"))
    add("CASE_III_D_05", _month_range("2021-10", "2023-01"), [SourceSpec("2022-01", 3915, 405, soft_del_cd="4"), SourceSpec("2022-05", 3916, 406, soft_del_cd="4")], "2023-01", ["2022-01", "2022-05"])

    # CASE IV
    add("CASE_IV_01", [], [SourceSpec("2025-11", 4101, 411), SourceSpec("2025-12", 4102, 412), SourceSpec("2026-01", 4103, 413)], "2026-01")
    add("CASE_IV_02", [], [SourceSpec("2025-09", 4201, 421), SourceSpec("2025-12", 4202, 422), SourceSpec("2026-02", 4203, 423)], "2026-02")

    _assert_true(len(scenarios) == 24, f"Expected 24 subcases, got {len(scenarios)}")
    return scenarios


def test_v4_all_leaf_subcases_single() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_all_leaf_subcases_single")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))
    import tests.test_utils as tu  # noqa: WPS433

    scenarios = _build_scenarios()
    failures: List[str] = []

    log_file = repo_root / "regression_suite" / "tests" / "artifacts" / "logs_v4" / "leaf_logs.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with log_file.open("a", encoding="utf-8") as file_stream:
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        tee_stream = TeeStream(original_stdout, file_stream)
        sys.stdout = tee_stream
        sys.stderr = tee_stream
        try:
            print("\n" + "=" * 120)
            print(f"MEGA TEST RUN START: {datetime.utcnow().isoformat()}Z")
            print("=" * 120)
            for idx, scenario in enumerate(scenarios, start=1):
                spark = tu.create_spark_session(f"v4_all_leaf_single_{scenario.subcase_id}")
                config = tu.load_main_test_config(f"v4_all_leaf_single_{scenario.subcase_id}")
                config["history_length"] = 36
                config["latest_history_window_months"] = 72
                config["validate_latest_history_window"] = True
                config["enable_case3_hot_cold_split"] = True
                config["case3_hot_window_months"] = 36

                print(f"\n===== [{idx:02d}/24] {scenario.subcase_id} =====")
                try:
                    module.cleanup(spark)
                    tu.reset_tables(spark, config)

                    seed_ts = datetime(2026, 1, 1, 0, 0, 0)
                    src_ts = datetime(2026, 2, 1, 12, 0, 0)

                    summary_rows: List[Dict] = []
                    # control anchor rows (range guard + control invariant)
                    summary_rows.extend(_make_seed_rows(tu, CONTROL_ACCT, [CONTROL_NEW_MONTH, CONTROL_OLD_MONTH], seed_ts))
                    # scenario account seed rows
                    summary_rows.extend(_make_seed_rows(tu, scenario.acct, scenario.seed_months, seed_ts))

                    latest_rows = _make_latest_rows(summary_rows)
                    source_rows = [
                        tu.build_source_row(
                            cons_acct_key=scenario.acct,
                            rpt_as_of_mo=s.month,
                            base_ts=src_ts,
                            balance=s.balance,
                            actual_payment=s.payment,
                            soft_del_cd=s.soft_del_cd,
                        )
                        for s in scenario.source_specs
                    ]

                    tu.write_summary_rows(spark, config["destination_table"], summary_rows)
                    tu.write_summary_rows(spark, config["latest_history_table"], latest_rows)
                    tu.write_source_rows(spark, config["source_table"], source_rows)

                    account_keys = [scenario.acct, CONTROL_ACCT]
                    touched_months = sorted({s.month for s in scenario.source_specs})

                    log_accounts_all_snapshot(
                        label=f"[{scenario.subcase_id}] BEFORE accounts_all",
                        spark=spark,
                        source_table=config["source_table"],
                        account_keys=account_keys,
                        months=touched_months + [CONTROL_NEW_MONTH, CONTROL_OLD_MONTH],
                    )
                    log_table_snapshot(
                        label=f"[{scenario.subcase_id}] BEFORE",
                        spark=spark,
                        summary_table=config["destination_table"],
                        latest_table=config["latest_history_table"],
                        account_keys=account_keys,
                        months=touched_months + [CONTROL_NEW_MONTH, CONTROL_OLD_MONTH],
                    )

                    pre_control_summary, pre_control_latest = _snapshot_control(
                        spark, config["destination_table"], config["latest_history_table"]
                    )

                    module.run_pipeline(spark, config)

                    log_accounts_all_snapshot(
                        label=f"[{scenario.subcase_id}] AFTER accounts_all",
                        spark=spark,
                        source_table=config["source_table"],
                        account_keys=account_keys,
                        months=touched_months + [CONTROL_NEW_MONTH, CONTROL_OLD_MONTH],
                    )
                    log_table_snapshot(
                        label=f"[{scenario.subcase_id}] AFTER",
                        spark=spark,
                        summary_table=config["destination_table"],
                        latest_table=config["latest_history_table"],
                        account_keys=account_keys,
                        months=touched_months + [CONTROL_NEW_MONTH, CONTROL_OLD_MONTH],
                    )

                    _assert_scenario(spark, config, scenario, pre_control_summary, pre_control_latest)
                    print(f"[PASS] {scenario.subcase_id}")
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{scenario.subcase_id}: {exc}")
                    print(f"[FAIL] {scenario.subcase_id}: {exc}")
                finally:
                    spark.stop()
            print(f"MEGA TEST RUN END: {datetime.utcnow().isoformat()}Z")
            print("=" * 120)
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    if failures:
        raise AssertionError(
            "Subcase failures:\n- " + "\n- ".join(failures)
        )

    print("[PASS] test_v4_all_leaf_subcases_single")


if __name__ == "__main__":
    test_v4_all_leaf_subcases_single()
    print("[PASS] test_v4_all_leaf_subcases_single.py")
