"""
Central test manifests and category metadata for regression_suite/tests.
"""

from __future__ import annotations

from pathlib import Path

CORE_TESTS = [
    "simple_test.py",
    "run_backfill_test.py",
    "test_all_scenarios.py",
    "test_all_scenarios_v942.py",
    "test_backfill_hist_rpt_preload.py",
    "test_backfill_soft_delete_audit_export.py",
    "test_backfill_soft_delete_from_deletion_flagger.py",
    "test_backfill_soft_delete_from_deletion_flagger_multidelete.py",
    "test_backfill_soft_delete_standalone.py",
    "test_bulk_historical_load.py",
    "test_case3_current_max_month.py",
    "test_complex_scenarios.py",
    "test_comprehensive_50_cases.py",
    "test_comprehensive_edge_cases.py",
    "test_consecutive_backfill.py",
    "test_duplicate_records.py",
    "test_full_46_columns.py",
    "test_generate_latest_summary_72_from_summary.py",
    "test_generate_latest_summary_from_summary.py",
    "test_hist_rpt_acct_dt_soft_delete_resolution.py",
    "test_idempotency.py",
    "test_latest_summary_consistency.py",
    "test_long_backfill_gaps.py",
    "test_main_all_cases.py",
    "test_main_base_ts_propagation.py",
    "test_non_continuous_backfill.py",
    "test_null_update.py",
    "test_null_update_case_iii.py",
    "test_null_update_other_cases.py",
    "test_performance_benchmark.py",
    "test_recovery.py",
    "test_soft_delete_case_iii.py",
    "test_aggressive_idempotency.py",
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
    "test_v4_case3_hot_lane_fail_fast_missing_latest.py",
    "test_v4_case3_hot_lane_fail_fast_missing_history_cols.py",
    "test_v4_case3_no_global_month_checks.py",
    "test_v4_all_leaf_subcases_single.py",
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
    "test_v4_case3_hot_lane_fail_fast_missing_latest.py": "v4_case3_hot_lane_fail_fast_missing_latest",
    "test_v4_case3_hot_lane_fail_fast_missing_history_cols.py": "v4_case3_hot_lane_fail_fast_missing_history_cols",
    "test_v4_case3_no_global_month_checks.py": "v4_case3_no_global_month_checks",
    "test_v4_all_leaf_subcases_single.py": "v4_all_leaf_subcases_single_matrix",
}

CATEGORY_DEFINITIONS = {
    "contracts": "Module/config/window/consistency contract tests.",
    "integration": "Cross-case end-to-end scenario/integration tests.",
    "case3_routing": "Case III hot/cold/overlap/lane routing behavior.",
    "case3_soft_delete": "Case III soft-delete routing and propagation.",
    "working_scope": "Working-set/base-scope behavior tests.",
    "idempotency_recovery": "Idempotency/recovery stability tests.",
    "bulk_history": "Bulk historical / long-history behavior.",
    "null_updates": "Null/sentinel/null-soft-delete update behavior.",
    "hist_rpt": "acct_dt + hist_rpt resolution/preload behavior.",
    "audit": "Before/after snapshot/export style tests.",
    "latest_generation": "latest_summary generation and 36/72 prefix checks.",
    "performance": "Benchmark/performance-oriented tests.",
}


def build_test_plan(
    include_aggressive: bool = False,
    aggressive_args: list[str] | None = None,
) -> list[dict]:
    plan = [{"file": test_file, "args": []} for test_file in CORE_TESTS]
    if include_aggressive and "test_aggressive_idempotency.py" not in CORE_TESTS:
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


def get_core_tests() -> list[str]:
    return list(CORE_TESTS)


def get_suite_tests(suite: str = "v4", tier: str = "smoke") -> list[str]:
    if suite not in {"v4", "core", "all"}:
        raise ValueError(f"Unsupported suite: {suite}")
    if suite == "v4":
        return get_v4_tier_tests(tier)
    if suite == "core":
        return get_core_tests()
    return sorted(set(get_core_tests()) | set(get_v4_tier_tests(tier)))


def build_v4_test_plan(tier: str = "smoke") -> list[dict]:
    return [{"file": test_file, "args": []} for test_file in get_v4_tier_tests(tier)]


def _derive_categories_for_test(test_file: str) -> set[str]:
    lower = test_file.lower()
    categories: set[str] = set()

    # Pattern-first assignment.
    if "case3" in lower:
        categories.add("case3_routing")
    if "soft_delete" in lower or "deletion_flagger" in lower:
        categories.add("case3_soft_delete")
    if "working_set_scope" in lower:
        categories.add("working_scope")
    if "idempotency" in lower or "recovery" in lower or "aggressive" in lower:
        categories.add("idempotency_recovery")
    if "bulk_historical" in lower or "long_backfill" in lower:
        categories.add("bulk_history")
    if "null_update" in lower:
        categories.add("null_updates")
    if "hist_rpt" in lower:
        categories.add("hist_rpt")
    if "audit" in lower:
        categories.add("audit")
    if (
        "generate_latest_summary" in lower
        or "summary36_latest72" in lower
        or "latest_summary_72" in lower
    ):
        categories.add("latest_generation")
    if "performance" in lower:
        categories.add("performance")

    if (
        "module_smoke" in lower
        or "config_contract" in lower
        or "latest_summary_consistency" in lower
        or "main_base_ts_propagation" in lower
        or "summary36_latest72_window_update" in lower
    ):
        categories.add("contracts")

    if (
        "all_scenarios" in lower
        or "main_all_cases" in lower
        or "case_end_to_end" in lower
        or "complex_scenarios" in lower
        or "comprehensive" in lower
        or "simple_test" in lower
        or "run_backfill_test" in lower
    ):
        categories.add("integration")

    # Explicit overrides for ambiguous files.
    overrides = {
        "test_case3_current_max_month.py": {"case3_routing", "integration"},
        "test_soft_delete_case_iii.py": {"case3_routing", "case3_soft_delete", "integration"},
        "test_v4_case3_comprehensive.py": {"case3_routing", "case3_soft_delete", "integration"},
        "test_v4_case_end_to_end_smoke.py": {"integration", "contracts"},
        "test_main_all_cases.py": {"integration", "contracts"},
        "test_generate_latest_summary_from_summary.py": {"latest_generation", "contracts"},
        "test_generate_latest_summary_72_from_summary.py": {"latest_generation", "contracts"},
        "test_backfill_soft_delete_audit_export.py": {"audit", "case3_soft_delete", "integration"},
        "test_backfill_soft_delete_standalone.py": {"case3_soft_delete", "integration"},
        "test_backfill_soft_delete_from_deletion_flagger.py": {"case3_soft_delete", "integration"},
        "test_backfill_soft_delete_from_deletion_flagger_multidelete.py": {
            "case3_soft_delete",
            "integration",
        },
        "test_v4_working_set_scope_mixed_cases.py": {"working_scope", "integration"},
        "test_v4_working_set_scope_case3_full_history.py": {"working_scope", "case3_routing"},
        "test_v4_working_set_scope_case2_only.py": {"working_scope", "integration"},
        "test_v4_case3_hot_lane_fail_fast_missing_latest.py": {"case3_routing", "contracts"},
        "test_v4_case3_hot_lane_fail_fast_missing_history_cols.py": {"case3_routing", "contracts"},
        "test_v4_case3_no_global_month_checks.py": {"case3_routing", "contracts"},
        "test_v4_all_leaf_subcases_single.py": {
            "integration",
            "case3_routing",
            "case3_soft_delete",
            "latest_generation",
            "contracts",
        },
        "test_aggressive_idempotency.py": {"idempotency_recovery", "performance"},
        "test_v4_aggressive_idempotency.py": {"idempotency_recovery", "performance"},
    }
    categories.update(overrides.get(test_file, set()))

    if not categories:
        categories.add("integration")
    return categories


_ALL_RUNNABLE_TESTS = sorted(set(get_core_tests()) | set(get_v4_tier_tests("nightly")))

TEST_CATEGORY_MAP = {
    test_file: _derive_categories_for_test(test_file)
    for test_file in _ALL_RUNNABLE_TESTS
}


def list_all_categories() -> list[str]:
    return sorted(CATEGORY_DEFINITIONS.keys())


def get_category_definitions() -> dict[str, str]:
    return dict(CATEGORY_DEFINITIONS)


def get_tests_by_categories(test_list: list[str], categories: set[str]) -> list[str]:
    if not categories:
        return list(test_list)
    requested = {c.strip() for c in categories if c.strip()}
    if not requested:
        return list(test_list)
    return sorted(
        test_name
        for test_name in test_list
        if TEST_CATEGORY_MAP.get(test_name, set()) & requested
    )


def validate_core_suite(tests_dir: Path) -> None:
    missing_core = [name for name in CORE_TESTS if not (tests_dir / name).exists()]
    if missing_core:
        raise FileNotFoundError(f"Missing core tests in manifest: {missing_core}")


def validate_category_map(tests_dir: Path) -> None:
    validate_core_suite(tests_dir)

    unknown_tests = sorted(set(TEST_CATEGORY_MAP.keys()) - set(_ALL_RUNNABLE_TESTS))
    if unknown_tests:
        raise ValueError(f"Category map references non-selectable tests: {unknown_tests}")

    missing_mappings = sorted(set(_ALL_RUNNABLE_TESTS) - set(TEST_CATEGORY_MAP.keys()))
    if missing_mappings:
        raise ValueError(f"Missing category mappings for tests: {missing_mappings}")

    for test_name, categories in TEST_CATEGORY_MAP.items():
        if not categories:
            raise ValueError(f"Test has empty category set: {test_name}")
        unknown_categories = sorted(set(categories) - set(CATEGORY_DEFINITIONS.keys()))
        if unknown_categories:
            raise ValueError(
                f"Test {test_name} uses unknown categories: {unknown_categories}"
            )
        if not (tests_dir / test_name).exists():
            raise FileNotFoundError(f"Category-mapped test file not found: {test_name}")

    for suite in ("v4", "core", "all"):
        for tier in ("smoke", "nightly"):
            selected = get_suite_tests(suite=suite, tier=tier)
            missing = [name for name in selected if not (tests_dir / name).exists()]
            if missing:
                raise FileNotFoundError(
                    f"Missing tests for suite={suite}, tier={tier}: {missing}"
                )


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
