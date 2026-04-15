"""
Central test suite manifests for main/docker_test/tests.
"""

from __future__ import annotations


CORE_TESTS = [
    "simple_test.py",
    "test_main_all_cases.py",
    "test_main_base_ts_propagation.py",
    "run_backfill_test.py",
    "test_all_scenarios.py",
    "test_all_scenarios_v942.py",
    "test_bulk_historical_load.py",
    "test_complex_scenarios.py",
    "test_case3_current_max_month.py",
    "test_hist_rpt_acct_dt_soft_delete_resolution.py",
    "test_backfill_hist_rpt_preload.py",
    "test_soft_delete_case_iii.py",
    "test_backfill_soft_delete_standalone.py",
    "test_backfill_soft_delete_audit_export.py",
    "test_comprehensive_50_cases.py",
    "test_comprehensive_edge_cases.py",
    "test_consecutive_backfill.py",
    "test_duplicate_records.py",
    "test_full_46_columns.py",
    "test_long_backfill_gaps.py",
    "test_non_continuous_backfill.py",
    "test_null_update_case_iii.py",
    "test_null_update_other_cases.py",
    "test_null_update.py",
    "test_idempotency.py",
    "test_latest_summary_consistency.py",
    "test_recovery.py",
]


DEFAULT_AGGRESSIVE_ARGS = ["--scale", "TINY", "--cycles", "2", "--reruns", "2"]


def build_test_plan(
    include_aggressive: bool = False,
    aggressive_args: list[str] | None = None,
) -> list[dict]:
    plan = [{"file": test_file, "args": []} for test_file in CORE_TESTS]
    if include_aggressive:
        plan.append(
            {
                "file": "test_aggressive_idempotency.py",
                "args": list(aggressive_args or DEFAULT_AGGRESSIVE_ARGS),
            }
        )
    return plan

