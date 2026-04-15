"""
Central test suite manifests for main/docker_test/tests.
"""

from __future__ import annotations

from pathlib import Path

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

# Canonical V4 unique-coverage tiers (no counterpart wrappers).
V4_SMOKE_TESTS = [
    "test_v4_module_smoke.py",
    "test_v4_config_contract.py",
    "test_v4_case_end_to_end_smoke.py",
    "test_v4_summary36_latest72_window_update.py",
    "test_v4_latest_summary_consistency.py",
    "test_v4_case3_current_max_month.py",
    "test_v4_case3_current_max_month_soft_delete.py",
    "test_v4_case3_hot_and_cold_split.py",
    "test_v4_case3_soft_delete_hot_cold_split.py",
    "test_v4_working_set_scope_mixed_cases.py",
]

V4_NIGHTLY_ADDITIONAL_TESTS = [
    "test_v4_case3_hot_cold_overlap_same_account.py",
    "test_v4_case3_hot_cold_overlap_same_account_soft_delete.py",
    "test_v4_case3_account_level_lane_routing.py",
    "test_v4_case3_account_level_lane_routing_mixed_with_latest.py",
    "test_v4_case3_soft_delete_account_level_lane_routing.py",
    "test_v4_case3_single_key_hot_cold_delete_mix.py",
    "test_v4_working_set_scope_case3_full_history.py",
    "test_v4_working_set_scope_case2_only.py",
    "test_v4_latest_summary_72_idempotent_bulk36.py",
    "test_v4_bulk_historical_load.py",
    "test_v4_null_update_case_iii_soft_delete.py",
    "test_v4_aggressive_idempotency.py",
    "test_v4_case3_comprehensive.py",
]

# One behavior label per canonical test in this unique suite.
V4_BEHAVIOR_LABELS = {
    "test_v4_module_smoke.py": "v4_module_contract",
    "test_v4_config_contract.py": "v4_config_contract",
    "test_v4_case_end_to_end_smoke.py": "v4_end_to_end_smoke",
    "test_v4_summary36_latest72_window_update.py": "v4_36_72_window_prefix",
    "test_v4_latest_summary_consistency.py": "v4_latest_summary_consistency",
    "test_v4_case3_current_max_month.py": "v4_case3_same_month_update",
    "test_v4_case3_current_max_month_soft_delete.py": "v4_case3_same_month_soft_delete",
    "test_v4_case3_hot_and_cold_split.py": "v4_case3_hot_cold_split",
    "test_v4_case3_soft_delete_hot_cold_split.py": "v4_case3_soft_delete_hot_cold_split",
    "test_v4_working_set_scope_mixed_cases.py": "v4_base_scope_mixed_cases",
    "test_v4_case3_hot_cold_overlap_same_account.py": "v4_case3_overlap_same_account",
    "test_v4_case3_hot_cold_overlap_same_account_soft_delete.py": "v4_case3_overlap_same_account_soft_delete",
    "test_v4_case3_account_level_lane_routing.py": "v4_case3_account_lane_routing",
    "test_v4_case3_account_level_lane_routing_mixed_with_latest.py": "v4_case3_account_lane_routing_mixed_latest",
    "test_v4_case3_soft_delete_account_level_lane_routing.py": "v4_case3_soft_delete_account_lane_routing",
    "test_v4_case3_single_key_hot_cold_delete_mix.py": "v4_case3_single_key_mixed_delete",
    "test_v4_working_set_scope_case3_full_history.py": "v4_base_scope_case3_full_history",
    "test_v4_working_set_scope_case2_only.py": "v4_base_scope_case2_only",
    "test_v4_latest_summary_72_idempotent_bulk36.py": "v4_latest72_idempotent_bulk36",
    "test_v4_bulk_historical_load.py": "v4_bulk_historical_load",
    "test_v4_null_update_case_iii_soft_delete.py": "v4_case3_soft_delete_null_payload",
    "test_v4_aggressive_idempotency.py": "v4_aggressive_idempotency",
    "test_v4_case3_comprehensive.py": "v4_case3_comprehensive_matrix",
}


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


def get_v4_tier_tests(tier: str = "smoke") -> list[str]:
    if tier not in {"smoke", "nightly"}:
        raise ValueError(f"Unsupported v4 tier: {tier}")
    if tier == "smoke":
        return list(V4_SMOKE_TESTS)
    return list(V4_SMOKE_TESTS) + list(V4_NIGHTLY_ADDITIONAL_TESTS)


def build_v4_test_plan(tier: str = "smoke") -> list[dict]:
    return [{"file": test_file, "args": []} for test_file in get_v4_tier_tests(tier)]


def validate_v4_unique_suite(
    tests_dir: Path,
    tier: str = "smoke",
) -> None:
    smoke = get_v4_tier_tests("smoke")
    nightly = get_v4_tier_tests("nightly")
    if not set(smoke).issubset(set(nightly)):
        raise ValueError("Invalid v4 manifest: nightly tier must be a strict superset of smoke tier")
    if len(set(smoke)) != len(smoke):
        raise ValueError("Invalid v4 manifest: duplicate test found in smoke tier")
    if len(set(nightly)) != len(nightly):
        raise ValueError("Invalid v4 manifest: duplicate test found in nightly tier")
    if len(set(nightly)) == len(smoke):
        raise ValueError("Invalid v4 manifest: nightly tier must add tests beyond smoke")

    selected = get_v4_tier_tests(tier)
    missing = [name for name in selected if not (tests_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing tests in v4 manifest: {missing}")

    labels = []
    for name in selected:
        label = V4_BEHAVIOR_LABELS.get(name)
        if label is None:
            raise ValueError(f"Missing behavior label for {name}")
        labels.append(label)

    if len(set(labels)) != len(labels):
        raise ValueError("Invalid v4 manifest: duplicate behavior label detected")

    wrapper_violations = []
    for name in selected:
        content = (tests_dir / name).read_text(encoding="utf-8")
        if "v4_counterpart_adapter" in content:
            wrapper_violations.append(name)
    if wrapper_violations:
        raise ValueError(
            "Invalid v4 unique suite: counterpart wrapper tests are disallowed "
            f"(found: {wrapper_violations})"
        )
