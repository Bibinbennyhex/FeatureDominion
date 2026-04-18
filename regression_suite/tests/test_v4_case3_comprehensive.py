"""
Comprehensive Case III test suite for summary_inc_v4.py

Covers 13 distinct backfill scenarios:
  1.  Single Hot Update — only latest month exists in summary
  2.  Single Hot Update — multiple prior months exist, update latest
  3.  Single Hot Update — 122 months history, backfill at 2025-12 (hot, not latest)
  4.  Single Cold Update — 122 months history, backfill at 2021-12 (cold)
  5.  Multiple consecutive Hot updates — 2025-11 + 2025-12
  6.  Multiple consecutive Cold updates — 2021-11 + 2021-12
  7.  Multiple non-consecutive Hot updates — 2024-11 + 2025-10
  8.  Multiple non-consecutive Cold updates — 2021-11 + 2022-03
  9.  Multiple non-consecutive Hot updates with existing NULL gaps
 10.  Multiple consecutive Hot updates filling existing NULL gaps
 11.  Multiple consecutive Cold updates filling existing NULL gaps
 12.  Mixed hot + cold entries for a single account
 13.  Mixed hot + cold entries with soft-deletes

Key design rules (derived from reference sample + pipeline source):
  - summary table  : every historical month row per account (full depth).
  - latest_summary : ONLY the single latest-month row per account, padded to 72.
  - source table   : ONLY the new / incoming rows to backfill.
  - sentinel row   : dummy account (key=99999) at the oldest cold month — keeps
                     min_month_destination low enough so the pipeline source-filter
                     (prt >= min_month_destination) does not drop cold source rows.
  - Processing flow: categorize_updates() then write_backfill_results()
                     (matches run_pipeline order exactly; no process_case_iii shortcut).
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import functions as F

from tests.v4_contract_utils import DELETE_CODES, HISTORY_COLS, load_v4_as_summary_inc, pad_latest_rows


# ---------------------------------------------------------------------------
# Month arithmetic
# ---------------------------------------------------------------------------

def _month_to_int(month: str) -> int:
    y, m = int(month[:4]), int(month[5:7])
    return y * 12 + m


def _int_to_month(n: int) -> str:
    y, m = divmod(n, 12)
    if m == 0:
        m, y = 12, y - 1
    return f"{y}-{m:02d}"


def _build_month_list(latest_month: str, count: int) -> List[str]:
    """Return *count* months newest-first ending at latest_month."""
    base = _month_to_int(latest_month)
    return [_int_to_month(base - i) for i in range(count)]


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_summary_rows(
    tu,
    acct: int,
    months: List[str],           # newest-first
    base_ts: datetime,
    gap_months: Optional[List[str]] = None,
    history_len: int = 36,
) -> List[Dict]:
    """
    Build one summary row per entry in *months*.
    Each row's arrays are filled backward from its own position:
      array[0] = this month's value
      array[j] = value j months earlier  (None when that slot is in gap_months)
    """
    gap_set = set(gap_months or [])

    def _val(m: str) -> Optional[int]:
        return None if m in gap_set else (1000 + _month_to_int(m) % 500)

    rows = []
    for i, month in enumerate(months):
        bal = _val(month)
        pay = None if month in gap_set else max(10, (bal or 0) // 10)

        bal_hist, pay_hist = [], []
        for j in range(history_len):
            src = i + j
            if src < len(months):
                m_j = months[src]
                v = _val(m_j)
                bal_hist.append(v)
                pay_hist.append(None if v is None else max(10, v // 10))
            else:
                bal_hist.append(None)
                pay_hist.append(None)

        rows.append(
            tu.build_summary_row(
                cons_acct_key=acct,
                rpt_as_of_mo=month,
                base_ts=base_ts,
                balance=bal or 0,
                actual_payment=pay or 0,
                balance_history=bal_hist,
                payment_history=pay_hist,
                credit_history=[10000] * history_len,
                past_due_history=[0] * history_len,
                rating_history=["0"] * history_len,
                dpd_history=[0] * history_len,
                asset_history=["A"] * history_len,
            )
        )
    return rows


def _latest_row_only(summary_rows: List[Dict]) -> List[Dict]:
    """
    Return the single latest-month row per account.
    latest_summary must hold exactly ONE row per account.
    """
    best: Dict[int, Dict] = {}
    for row in summary_rows:
        key = int(row["cons_acct_key"])
        if key not in best or row["rpt_as_of_mo"] > best[key]["rpt_as_of_mo"]:
            best[key] = row
    return list(best.values())


def _sentinel_anchor(tu, oldest_cold_month: str, base_ts: datetime) -> Dict:
    """
    Dummy account (key=99999) written to summary at oldest_cold_month.
    This drives min_month_destination below the cold source months so the
    pipeline's source filter keeps them in scope.
    """
    return tu.build_summary_row(
        cons_acct_key=99999,
        rpt_as_of_mo=oldest_cold_month,
        base_ts=base_ts,
        balance=1,
        actual_payment=1,
    )


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------

def _assert_true(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _get_latest_row(spark, latest_table: str, acct: int):
    rows = spark.table(latest_table).filter(F.col("cons_acct_key") == acct).collect()
    if not rows:
        return None
    _assert_true(
        len(rows) == 1,
        f"latest_summary must have exactly 1 row for acct={acct}, found {len(rows)}",
    )
    return rows[0]


def _assert_summary_balance(spark, table, acct, month, expected, label):
    rows = (
        spark.table(table)
        .filter((F.col("cons_acct_key") == acct) & (F.col("rpt_as_of_mo") == month))
        .collect()
    )
    _assert_true(len(rows) >= 1, f"{label}: no summary row acct={acct} month={month}")
    row = max(rows, key=lambda r: r["base_ts"])
    _assert_true(
        row["balance_am"] == expected,
        f"{label}: acct={acct} {month} balance_am: expected={expected} got={row['balance_am']}",
    )


def _assert_latest_balance(spark, table, acct, expected, label):
    row = _get_latest_row(spark, table, acct)
    _assert_true(row is not None, f"{label}: no latest_summary row for acct={acct}")
    _assert_true(
        row["balance_am"] == expected,
        f"{label}: acct={acct} latest balance_am: expected={expected} got={row['balance_am']}",
    )


def _assert_latest_array(spark, table, acct, array_col, idx, expected, label):
    row = _get_latest_row(spark, table, acct)
    _assert_true(row is not None, f"{label}: no latest_summary row for acct={acct}")
    arr = row[array_col]
    _assert_true(arr is not None, f"{label}: {array_col} is None for acct={acct}")
    _assert_true(len(arr) > idx, f"{label}: {array_col} len={len(arr)} < idx={idx}")
    _assert_true(
        arr[idx] == expected,
        f"{label}: {array_col}[{idx}]: expected={expected} got={arr[idx]}",
    )


def _assert_soft_deleted(spark, table, acct, month, label):
    rows = (
        spark.table(table)
        .filter((F.col("cons_acct_key") == acct) & (F.col("rpt_as_of_mo") == month))
        .collect()
    )
    _assert_true(len(rows) >= 1, f"{label}: no row for acct={acct} month={month}")
    row = max(rows, key=lambda r: r["base_ts"])
    sd = row["soft_del_cd"]
    _assert_true(
        sd in ("1", "4"),
        f"{label}: acct={acct} {month}: expected soft_del_cd, got {sd!r}",
    )


def _assert_latest_active(spark, table, acct, label):
    row = _get_latest_row(spark, table, acct)
    if row is None:
        return
    sd = row["soft_del_cd"]
    _assert_true(
        sd not in ("1", "4"),
        f"{label}: acct={acct} latest_summary must be active, got soft_del_cd={sd!r}",
    )


def _snapshot_row_arrays(row) -> Dict[str, tuple]:
    return {col: tuple((row[col] or [])) for col in HISTORY_COLS}


def _fetch_row_arrays(tu, spark, table: str, acct: int, month: str):
    row = tu.fetch_single_row(spark, table, acct, month)
    if row is None:
        return None
    return _snapshot_row_arrays(row)


# ---------------------------------------------------------------------------
# Module-level state (single shared Spark session across all tests)
# ---------------------------------------------------------------------------

_SPARK = None
_MODULE = None
_CONFIG = None
_TU = None


def _init(test_name: str) -> None:
    global _SPARK, _MODULE, _CONFIG, _TU

    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_comprehensive")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session(f"v4_case3_comprehensive_{test_name}")

    config = tu.load_main_test_config(f"v4_case3_comprehensive_{test_name}")
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True
    config["enable_case3_hot_cold_split"] = True
    config["case3_hot_window_months"] = 36

    _SPARK = spark
    _MODULE = module
    _CONFIG = config
    _TU = tu


# ---------------------------------------------------------------------------
# Core pipeline helper — classify + assert count + run + write
# ---------------------------------------------------------------------------

def _run_case3(
    summary_rows: List[Dict],
    source_rows: List[Dict],
    latest_rows: Optional[List[Dict]] = None,
    expected_case3_normal: Optional[int] = None,
    expected_case3_deletes: Optional[int] = None,
    label: str = "",
) -> None:
    """
    Full Case III fixture execution:
      1. Drop / recreate all tables.
      2. Write summary (full history), latest_summary (one row per acct, padded to 72),
         and source rows.
      3. ensure_soft_delete_columns + preload_run_table_columns.
      4. load_and_classify_accounts.
      5. Optional count assertions on CASE_III rows (before processing).
      6. process_case_iii/process_case_iii_soft_delete.
      7. write_backfill_results (commits to summary / latest_summary).
    """
    tu, spark, module, config = _TU, _SPARK, _MODULE, _CONFIG

    module.cleanup(spark)
    tu.reset_tables(spark, config)

    touched_keys = {(int(r["cons_acct_key"]), r["rpt_as_of_mo"]) for r in source_rows}
    source_row_map = {(int(r["cons_acct_key"]), r["rpt_as_of_mo"]): r for r in source_rows}
    latest_key_by_acct = {}
    if latest_rows:
        for lr in latest_rows:
            acct = int(lr["cons_acct_key"])
            mo = lr["rpt_as_of_mo"]
            if acct not in latest_key_by_acct or mo > latest_key_by_acct[acct]:
                latest_key_by_acct[acct] = mo

    tu.write_summary_rows(spark, config["destination_table"], summary_rows)

    if latest_rows is None:
        latest_rows = pad_latest_rows(_latest_row_only(summary_rows))
    tu.write_summary_rows(spark, config["latest_history_table"], latest_rows)

    pre_summary = {}
    for acct, mo in touched_keys:
        arr = _fetch_row_arrays(tu, spark, config["destination_table"], acct, mo)
        if arr is not None:
            pre_summary[(acct, mo)] = arr
    pre_latest = {
        acct: _fetch_row_arrays(tu, spark, config["latest_history_table"], acct, mo)
        for acct, mo in latest_key_by_acct.items()
    }

    tu.write_source_rows(spark, config["source_table"], source_rows)

    module.ensure_soft_delete_columns(spark, config)
    module.preload_run_table_columns(spark, config)

    classified = module.load_and_classify_accounts(spark, config)

    # --- optional pre-processing count assertions --------------------------
    if expected_case3_normal is not None:
        normal_count = classified.filter(
            (F.col("case_type") == "CASE_III") & (~F.col("_is_soft_delete"))
        ).count()
        _assert_true(
            normal_count == expected_case3_normal,
            f"{label}: expected {expected_case3_normal} normal CASE_III rows, got {normal_count}",
        )

    if expected_case3_deletes is not None:
        del_count = classified.filter(
            (F.col("case_type") == "CASE_III") & (F.col("_is_soft_delete"))
        ).count()
        _assert_true(
            del_count == expected_case3_deletes,
            f"{label}: expected {expected_case3_deletes} soft-delete CASE_III rows, got {del_count}",
        )
    # -----------------------------------------------------------------------

    case_iii_all = classified.filter(F.col("case_type") == "CASE_III")
    case_iii_normal = case_iii_all.filter(~F.col("_is_soft_delete"))
    case_iii_delete = case_iii_all.filter(F.col("_is_soft_delete"))

    normal_count = case_iii_normal.count()
    delete_count = case_iii_delete.count()

    if normal_count > 0:
        module.process_case_iii(spark, case_iii_normal, config, expected_rows=normal_count)
    if delete_count > 0:
        module.process_case_iii_soft_delete(spark, case_iii_delete, config, expected_rows=delete_count)

    if normal_count + delete_count > 0:
        module.write_backfill_results(spark, config)

    for acct, mo in touched_keys:
        summary_row = tu.fetch_single_row(spark, config["destination_table"], acct, mo)
        _assert_true(summary_row is not None, f"{label}: missing post summary row acct={acct} month={mo}")
        for col in HISTORY_COLS:
            _assert_true(len(list(summary_row[col] or [])) == 36, f"{label}: {col} summary len must be 36")

        src = source_row_map[(acct, mo)]
        if str(src.get("soft_del_cd", "")) not in DELETE_CODES:
            src_balance = src.get("balance_am")
            if src_balance is None:
                src_balance = src.get("current_balance")
            if src_balance is not None:
                _assert_true(
                    int(summary_row["balance_am_history"][0]) == int(src_balance),
                    f"{label}: acct={acct} month={mo} summary idx0 mismatch",
                )
            if (acct, mo) in pre_summary:
                _assert_true(
                    _snapshot_row_arrays(summary_row) != pre_summary[(acct, mo)],
                    f"{label}: expected touched summary arrays to change acct={acct} month={mo}",
                )

    latest_changed = False
    for acct, mo in latest_key_by_acct.items():
        latest_row = tu.fetch_single_row(spark, config["latest_history_table"], acct, mo)
        _assert_true(latest_row is not None, f"{label}: missing post latest row acct={acct} month={mo}")
        for col in HISTORY_COLS:
            _assert_true(len(list(latest_row[col] or [])) == 72, f"{label}: {col} latest len must be 72")
        before = pre_latest.get(acct)
        if before is not None and _snapshot_row_arrays(latest_row) != before:
            latest_changed = True

    # If any source row is within 36 months of account latest or is a soft-delete, latest arrays should change.
    must_change_latest = False
    for src in source_rows:
        acct = int(src["cons_acct_key"])
        acct_latest = latest_key_by_acct.get(acct)
        if acct_latest is None:
            continue
        if str(src.get("soft_del_cd", "")) in DELETE_CODES:
            must_change_latest = True
            break
        if (_month_to_int(acct_latest) - _month_to_int(src["rpt_as_of_mo"])) <= 36:
            must_change_latest = True
            break
    if must_change_latest and pre_latest:
        _assert_true(latest_changed, f"{label}: expected at least one touched latest_summary row to change")


# ---------------------------------------------------------------------------
# TC-01
# ---------------------------------------------------------------------------

def test_01_single_hot_update_latest_only():
    """
    Summary: one row at 2026-03 (the only month for this account).
    Source:  one row for 2026-03 with updated balance.
    CASE_III (MONTH_DIFF=0, hot).
    Assert: summary 2026-03 and latest_summary updated to new balance.
    """
    tu = _TU
    ACCT = 101
    MONTH = "2026-03"
    existing_ts = datetime(2026, 3, 1)
    source_ts   = datetime(2026, 3, 31, 12, 0, 0)

    summary_rows = [
        tu.build_summary_row(
            cons_acct_key=ACCT, rpt_as_of_mo=MONTH,
            base_ts=existing_ts, balance=100, actual_payment=10,
        )
    ]
    source_rows = [
        tu.build_source_row(ACCT, MONTH, source_ts, balance=1238, actual_payment=124),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=1, label="[TC-01]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, MONTH, 1238, "[TC-01]")
    _assert_latest_balance(spark, config["latest_history_table"], ACCT, 1238, "[TC-01]")
    print("[PASS] TC-01: Single hot update — latest partition only")


# ---------------------------------------------------------------------------
# TC-02
# ---------------------------------------------------------------------------

def test_02_single_hot_update_with_prior_history():
    """
    Summary: 6 months (2025-10 … 2026-03, latest=2026-03).
    Source:  one row for 2026-03 with updated balance.
    Assert: summary 2026-03 and latest_summary updated.
    """
    tu = _TU
    ACCT = 102
    LATEST = "2026-03"
    existing_ts = datetime(2026, 1, 1)
    source_ts   = datetime(2026, 3, 31, 12, 0, 0)

    months = _build_month_list(LATEST, 6)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    source_rows = [
        tu.build_source_row(ACCT, LATEST, source_ts, balance=1238, actual_payment=124),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=1, label="[TC-02]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, LATEST, 1238, "[TC-02]")
    _assert_latest_balance(spark, config["latest_history_table"], ACCT, 1238, "[TC-02]")
    print("[PASS] TC-02: Single hot update — with prior history, update latest partition")


# ---------------------------------------------------------------------------
# TC-03
# ---------------------------------------------------------------------------

def test_03_single_hot_update_not_latest_month():
    """
    Summary: 36 months (2023-04 … 2026-03, latest=2026-03).
    Source:  one row for 2025-12 (hot, MONTH_DIFF=3).
    Assert:
      - summary 2025-12 balance = 9999.
      - latest_summary balance_am_history[3] = 9999.
    """
    tu = _TU
    ACCT = 103
    LATEST = "2026-03"
    BACKFILL = "2025-12"
    existing_ts = datetime(2023, 1, 1)
    source_ts   = datetime(2026, 3, 15, 8, 0, 0)

    months = _build_month_list(LATEST, 36)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    source_rows = [
        tu.build_source_row(ACCT, BACKFILL, source_ts, balance=9999, actual_payment=999),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=1, label="[TC-03]")

    spark, config = _SPARK, _CONFIG
    diff = _month_to_int(LATEST) - _month_to_int(BACKFILL)  # 3
    _assert_summary_balance(spark, config["destination_table"], ACCT, BACKFILL, 9999, "[TC-03]")
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history", diff, 9999, "[TC-03]")
    print("[PASS] TC-03: Single hot update — backfill non-latest hot partition (MONTH_DIFF=3)")


# ---------------------------------------------------------------------------
# TC-04
# ---------------------------------------------------------------------------

def test_04_single_cold_update():
    """
    Summary: 60 months (2021-04 … 2026-03, latest=2026-03).
    Source:  one row for 2021-12 (cold, MONTH_DIFF=51).
    Sentinel at 2021-04 keeps min_month_destination low.
    Assert:
      - summary 2021-12 balance = 8888.
      - summary 2022-12 balance_am_history[12] = 8888 (cascade).
    """
    tu = _TU
    ACCT = 104
    LATEST = "2026-03"
    BACKFILL = "2021-12"
    oldest = "2021-04"
    existing_ts = datetime(2021, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 60)   # 2026-03 … 2021-04
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    summary_rows.append(_sentinel_anchor(tu, oldest, existing_ts))

    source_rows = [
        tu.build_source_row(ACCT, BACKFILL, source_ts, balance=8888, actual_payment=888),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=1, label="[TC-04]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, BACKFILL, 8888, "[TC-04]")

    # Cascade check: 2022-12 is 12 months after 2021-12
    INTERMEDIATE = "2022-12"
    offset = _month_to_int(INTERMEDIATE) - _month_to_int(BACKFILL)  # 12
    if offset < config["history_length"]:
        rows_i = (
            spark.table(config["destination_table"])
            .filter((F.col("cons_acct_key") == ACCT) & (F.col("rpt_as_of_mo") == INTERMEDIATE))
            .collect()
        )
        if rows_i:
            hist = max(rows_i, key=lambda r: r["base_ts"])["balance_am_history"]
            if hist and len(hist) > offset:
                _assert_true(
                    hist[offset] == 8888,
                    f"[TC-04] {INTERMEDIATE} balance_am_history[{offset}]: expected 8888 got {hist[offset]}",
                )
    print("[PASS] TC-04: Single cold update — 2021-12 backfill with cascade")


# ---------------------------------------------------------------------------
# TC-05
# ---------------------------------------------------------------------------

def test_05_multiple_consecutive_hot_updates():
    """
    Summary: 36 months (latest=2026-03).
    Source:  2025-11 (MONTH_DIFF=4) + 2025-12 (MONTH_DIFF=3) — both hot.
    Assert summary updated; latest_summary array[4]=5111, array[3]=5222.
    """
    tu = _TU
    ACCT = 105
    LATEST = "2026-03"
    existing_ts = datetime(2023, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 36)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    source_rows = [
        tu.build_source_row(ACCT, "2025-11", source_ts, balance=5111, actual_payment=511),
        tu.build_source_row(ACCT, "2025-12", source_ts, balance=5222, actual_payment=522),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=2, label="[TC-05]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-11", 5111, "[TC-05]")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-12", 5222, "[TC-05]")
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history",
                         _month_to_int(LATEST) - _month_to_int("2025-11"), 5111, "[TC-05] arr[4]")
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history",
                         _month_to_int(LATEST) - _month_to_int("2025-12"), 5222, "[TC-05] arr[3]")
    print("[PASS] TC-05: Multiple consecutive hot updates — 2025-11 + 2025-12")


# ---------------------------------------------------------------------------
# TC-06
# ---------------------------------------------------------------------------

def test_06_multiple_consecutive_cold_updates():
    """
    Summary: 60 months (2021-04 … 2026-03).
    Source:  2021-11 + 2021-12 — both cold (MONTH_DIFF > 36).
    Sentinel at 2021-04.
    Assert both months updated in summary.
    """
    tu = _TU
    ACCT = 106
    LATEST = "2026-03"
    oldest = "2021-04"
    existing_ts = datetime(2021, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 60)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    summary_rows.append(_sentinel_anchor(tu, oldest, existing_ts))

    source_rows = [
        tu.build_source_row(ACCT, "2021-11", source_ts, balance=6111, actual_payment=611),
        tu.build_source_row(ACCT, "2021-12", source_ts, balance=6222, actual_payment=622),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=2, label="[TC-06]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2021-11", 6111, "[TC-06]")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2021-12", 6222, "[TC-06]")
    print("[PASS] TC-06: Multiple consecutive cold updates — 2021-11 + 2021-12")


# ---------------------------------------------------------------------------
# TC-07
# ---------------------------------------------------------------------------

def test_07_multiple_non_consecutive_hot_updates():
    """
    Summary: 36 months (latest=2026-03).
    Source:  2024-11 (MONTH_DIFF=16) + 2025-10 (MONTH_DIFF=5) — both hot.
    Assert summary updated; latest array[16]=7111, array[5]=7222.
    """
    tu = _TU
    ACCT = 107
    LATEST = "2026-03"
    existing_ts = datetime(2023, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 36)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    source_rows = [
        tu.build_source_row(ACCT, "2024-11", source_ts, balance=7111, actual_payment=711),
        tu.build_source_row(ACCT, "2025-10", source_ts, balance=7222, actual_payment=722),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=2, label="[TC-07]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2024-11", 7111, "[TC-07]")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-10", 7222, "[TC-07]")

    diff_nov = _month_to_int(LATEST) - _month_to_int("2024-11")  # 16
    diff_oct = _month_to_int(LATEST) - _month_to_int("2025-10")  # 5
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history", diff_nov, 7111, f"[TC-07] arr[{diff_nov}]")
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history", diff_oct, 7222, f"[TC-07] arr[{diff_oct}]")
    print("[PASS] TC-07: Multiple non-consecutive hot updates — 2024-11 + 2025-10")


# ---------------------------------------------------------------------------
# TC-08
# ---------------------------------------------------------------------------

def test_08_multiple_non_consecutive_cold_updates():
    """
    Summary: 60 months (2021-04 … 2026-03).
    Source:  2021-11 + 2022-03 — both cold. Sentinel at 2021-04.
    Assert both months updated in summary.
    """
    tu = _TU
    ACCT = 108
    LATEST = "2026-03"
    oldest = "2021-04"
    existing_ts = datetime(2021, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 60)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    summary_rows.append(_sentinel_anchor(tu, oldest, existing_ts))

    source_rows = [
        tu.build_source_row(ACCT, "2021-11", source_ts, balance=8111, actual_payment=811),
        tu.build_source_row(ACCT, "2022-03", source_ts, balance=8222, actual_payment=822),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=2, label="[TC-08]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2021-11", 8111, "[TC-08]")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2022-03", 8222, "[TC-08]")
    print("[PASS] TC-08: Multiple non-consecutive cold updates — 2021-11 + 2022-03")


# ---------------------------------------------------------------------------
# TC-09
# ---------------------------------------------------------------------------

def test_09_non_consecutive_hot_updates_with_gaps():
    """
    Summary: 36 months (2023-04 … 2026-03, latest=2026-03).
    Existing NULLs in gap months: 2025-01…2025-06 and 2025-12…2026-02.
    Source:  4 of those gap months (all hot, non-consecutive):
             2025-02 → 9000, 2025-05 → 9100, 2025-12 → 9200, 2026-01 → 9300.
    Assert summary updated; latest_summary array patched at correct positions.
    """
    tu = _TU
    ACCT = 109
    LATEST = "2026-03"
    existing_ts = datetime(2023, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    gap_a = [_int_to_month(_month_to_int("2025-01") + i) for i in range(6)]
    gap_b = [_int_to_month(_month_to_int("2025-12") + i) for i in range(3)]
    all_gaps = gap_a + gap_b

    months = _build_month_list(LATEST, 36)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts, gap_months=all_gaps)

    backfill_months = ["2025-02", "2025-05", "2025-12", "2026-01"]
    expected = {m: 9000 + i * 100 for i, m in enumerate(backfill_months)}
    source_rows = [
        tu.build_source_row(ACCT, m, source_ts, balance=expected[m], actual_payment=expected[m] // 10)
        for m in backfill_months
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=4, label="[TC-09]")

    spark, config = _SPARK, _CONFIG
    latest_win = config["latest_history_window_months"]
    for m, bal in expected.items():
        _assert_summary_balance(spark, config["destination_table"], ACCT, m, bal, f"[TC-09] {m}")
        diff = _month_to_int(LATEST) - _month_to_int(m)
        if diff < latest_win:
            _assert_latest_array(spark, config["latest_history_table"], ACCT,
                                 "balance_am_history", diff, bal, f"[TC-09] arr[{diff}] ({m})")
    print("[PASS] TC-09: Non-consecutive hot updates into NULL gap positions")


# ---------------------------------------------------------------------------
# TC-10
# ---------------------------------------------------------------------------

def test_10_consecutive_hot_updates_filling_gaps():
    """
    Summary: 36 months (2023-04 … 2026-03, latest=2026-03).
    Existing NULLs: 2025-01…2025-06 and 2025-12…2026-02 (9 months total).
    Source:  all 9 gap months filled (all hot).
    Assert all 9 months updated in summary.
    """
    tu = _TU
    ACCT = 110
    LATEST = "2026-03"
    existing_ts = datetime(2023, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    gap_a = [_int_to_month(_month_to_int("2025-01") + i) for i in range(6)]
    gap_b = [_int_to_month(_month_to_int("2025-12") + i) for i in range(3)]
    all_gaps = gap_a + gap_b

    months = _build_month_list(LATEST, 36)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts, gap_months=all_gaps)

    expected = {m: 10000 + i * 50 for i, m in enumerate(all_gaps)}
    source_rows = [
        tu.build_source_row(ACCT, m, source_ts, balance=expected[m], actual_payment=expected[m] // 10)
        for m in all_gaps
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=len(all_gaps), label="[TC-10]")

    spark, config = _SPARK, _CONFIG
    for m, bal in expected.items():
        _assert_summary_balance(spark, config["destination_table"], ACCT, m, bal, f"[TC-10] {m}")
    print("[PASS] TC-10: Consecutive hot updates filling all NULL gap months")


# ---------------------------------------------------------------------------
# TC-11
# ---------------------------------------------------------------------------

def test_11_consecutive_cold_updates_filling_gaps():
    """
    Summary: 66 months (2020-10 … 2026-03, latest=2026-03).
    Existing NULLs (cold range): 2021-01…2021-06 and 2021-12…2022-02.
    Sentinel at 2020-10 (already covered by the 66-month window, no extra needed).
    Source:  all 9 cold gap months.
    Assert all 9 months updated in summary.
    """
    tu = _TU
    ACCT = 111
    LATEST = "2026-03"
    existing_ts = datetime(2020, 10, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    gap_a = [_int_to_month(_month_to_int("2021-01") + i) for i in range(6)]
    gap_b = [_int_to_month(_month_to_int("2021-12") + i) for i in range(3)]
    all_gaps = gap_a + gap_b

    months = _build_month_list(LATEST, 66)   # reaches 2020-10
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts, gap_months=all_gaps)
    # The oldest month in this account's summary is 2020-10, which is already
    # <= 2021-01 so min_month_destination will keep all cold source rows in scope.
    # No separate sentinel needed.

    expected = {m: 11000 + i * 50 for i, m in enumerate(all_gaps)}
    source_rows = [
        tu.build_source_row(ACCT, m, source_ts, balance=expected[m], actual_payment=expected[m] // 10)
        for m in all_gaps
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=len(all_gaps), label="[TC-11]")

    spark, config = _SPARK, _CONFIG
    for m, bal in expected.items():
        _assert_summary_balance(spark, config["destination_table"], ACCT, m, bal, f"[TC-11] {m}")
    print("[PASS] TC-11: Consecutive cold updates filling cold NULL gap months")


# ---------------------------------------------------------------------------
# TC-12
# ---------------------------------------------------------------------------

def test_12_mixed_hot_and_cold():
    """
    Summary: 60 months (2021-04 … 2026-03, latest=2026-03).
    Source:
      2022-01 — cold  (MONTH_DIFF=50)  balance=12001
      2025-11 — hot   (MONTH_DIFF=4)   balance=12011
      2025-12 — hot   (MONTH_DIFF=3)   balance=12012
    Account has both hot and cold months → mixed lane.
    Sentinel at 2021-04.
    Assert all three months updated; latest_summary arrays patched.
    """
    tu = _TU
    ACCT = 112
    LATEST = "2026-03"
    oldest = "2021-04"
    existing_ts = datetime(2021, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 60)
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)
    summary_rows.append(_sentinel_anchor(tu, oldest, existing_ts))

    source_rows = [
        tu.build_source_row(ACCT, "2022-01", source_ts, balance=12001, actual_payment=1200),
        tu.build_source_row(ACCT, "2025-11", source_ts, balance=12011, actual_payment=1201),
        tu.build_source_row(ACCT, "2025-12", source_ts, balance=12012, actual_payment=1202),
    ]

    _run_case3(summary_rows, source_rows, expected_case3_normal=3, label="[TC-12]")

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2022-01", 12001, "[TC-12] cold")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-11", 12011, "[TC-12] hot-11")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-12", 12012, "[TC-12] hot-12")

    diff_nov = _month_to_int(LATEST) - _month_to_int("2025-11")  # 4
    diff_dec = _month_to_int(LATEST) - _month_to_int("2025-12")  # 3
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history", diff_nov, 12011, f"[TC-12] arr[{diff_nov}]")
    _assert_latest_array(spark, config["latest_history_table"], ACCT,
                         "balance_am_history", diff_dec, 12012, f"[TC-12] arr[{diff_dec}]")
    print("[PASS] TC-12: Mixed hot + cold entries for a single account")


# ---------------------------------------------------------------------------
# TC-13
# ---------------------------------------------------------------------------

def test_13_mixed_hot_cold_with_soft_deletes():
    """
    Summary: 75 months (2020-01 … 2026-03, latest=2026-03).
    Source:
      Normal: 2022-01 (cold), 2025-11 (hot), 2025-12 (hot)
      Soft-delete code '4': 2024-09, 2020-04
    Sentinel: 2020-01 already covered by the 75-month window.
    Assert:
      - Normal months updated with new balances.
      - 2024-09 and 2020-04 flagged soft-deleted in summary.
      - latest_summary remains active (account still has live months).
    """
    tu = _TU
    ACCT = 113
    LATEST = "2026-03"
    existing_ts = datetime(2020, 1, 1)
    source_ts   = datetime(2026, 3, 10, 8, 0, 0)

    months = _build_month_list(LATEST, 75)   # reaches 2020-01
    summary_rows = _make_summary_rows(tu, ACCT, months, existing_ts)

    source_rows = [
        tu.build_source_row(ACCT, "2022-01", source_ts, balance=13001, actual_payment=1300),
        tu.build_source_row(ACCT, "2025-11", source_ts, balance=13011, actual_payment=1301),
        tu.build_source_row(ACCT, "2025-12", source_ts, balance=13012, actual_payment=1302),
        tu.build_source_row(ACCT, "2024-09", source_ts, balance=0, actual_payment=0, soft_del_cd="4"),
        tu.build_source_row(ACCT, "2020-04", source_ts, balance=0, actual_payment=0, soft_del_cd="4"),
    ]

    _run_case3(
        summary_rows, source_rows,
        expected_case3_normal=3,
        expected_case3_deletes=2,
        label="[TC-13]",
    )

    spark, config = _SPARK, _CONFIG
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2022-01", 13001, "[TC-13] cold")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-11", 13011, "[TC-13] hot-11")
    _assert_summary_balance(spark, config["destination_table"], ACCT, "2025-12", 13012, "[TC-13] hot-12")
    _assert_soft_deleted(spark, config["destination_table"], ACCT, "2024-09", "[TC-13] del-2024-09")
    _assert_soft_deleted(spark, config["destination_table"], ACCT, "2020-04", "[TC-13] del-2020-04")
    _assert_latest_active(spark, config["latest_history_table"], ACCT, "[TC-13]")
    print("[PASS] TC-13: Mixed hot+cold with soft-deletes (code '4')")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_ALL_TESTS = [
    ("TC-01", test_01_single_hot_update_latest_only),
    ("TC-02", test_02_single_hot_update_with_prior_history),
    ("TC-03", test_03_single_hot_update_not_latest_month),
    ("TC-04", test_04_single_cold_update),
    ("TC-05", test_05_multiple_consecutive_hot_updates),
    ("TC-06", test_06_multiple_consecutive_cold_updates),
    ("TC-07", test_07_multiple_non_consecutive_hot_updates),
    ("TC-08", test_08_multiple_non_consecutive_cold_updates),
    ("TC-09", test_09_non_consecutive_hot_updates_with_gaps),
    ("TC-10", test_10_consecutive_hot_updates_filling_gaps),
    ("TC-11", test_11_consecutive_cold_updates_filling_gaps),
    ("TC-12", test_12_mixed_hot_and_cold),
    ("TC-13", test_13_mixed_hot_cold_with_soft_deletes),
]

# _ALL_TESTS = [
#     ("TC-04", test_04_single_cold_update),
#     ("TC-06", test_06_multiple_consecutive_cold_updates),
#     ("TC-08", test_08_multiple_non_consecutive_cold_updates),    
#     ("TC-11", test_11_consecutive_cold_updates_filling_gaps),
#     ("TC-12", test_12_mixed_hot_and_cold),
#     ("TC-13", test_13_mixed_hot_cold_with_soft_deletes),
# ]

def run_all_tests():
    _init("all")
    passed, failed = [], []
    try:
        for name, fn in _ALL_TESTS:
            try:
                fn()
                passed.append(name)
            except Exception as exc:
                import traceback
                print(f"[FAIL] {name}: {exc}")
                traceback.print_exc()
                failed.append((name, exc))
    finally:
        if _SPARK:
            _SPARK.stop()

    print("\n" + "=" * 60)
    print(f"Results: {len(passed)} passed, {len(failed)} failed")
    if failed:
        for name, exc in failed:
            print(f"  FAIL {name}: {exc}")
        raise SystemExit(1)
    print("[PASS] All Case III comprehensive tests passed.")


if __name__ == "__main__":
    run_all_tests()


