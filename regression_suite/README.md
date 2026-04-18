# Regression Suite Guide

This document explains how regression tests are organized and executed under:

- `regression_suite/tests`

It covers:

- Suite model (`v4`, `core`, `all`)
- Tier model (`smoke`, `nightly`)
- Functional categories
- Runner CLI and selection precedence
- Common run recipes (Docker + local)
- Troubleshooting and maintenance rules

## Purpose And Scope

The regression suite is intentionally split into two logical groups:

1. `v4` suite:
- Focused on V4-specific and V4-unique behavior.
- Uses the existing `smoke` / `nightly` tier contract.

2. `core` suite:
- Scenario-style and integration-style regression tests replicated under `regression_suite/tests`.
- Includes non-`test_v4_*` tests (for example: `test_main_all_cases.py`, `test_backfill_*`, `test_generate_latest_summary_*`).

3. `all` suite:
- Union of `core` + selected `v4` tier.
- For `all`, `tier` applies only to the `v4` portion.

Relation to legacy `/tests`:
- `regression_suite/tests` is now self-contained for selected regression scenarios.
- Wrapper/counterpart tests from legacy `/tests` are intentionally not required here.

## Runner Interface

Runner:

- `regression_suite/tests/run_all_tests_v4.py`

### CLI Flags

- `--suite {v4,core,all}`:
  - Default: `v4`
  - Chooses test population.
- `--tier {smoke,nightly}`:
  - Default: `smoke`
  - Applies to `v4` selection.
- `--categories a,b,c`:
  - Comma-separated functional categories.
  - Category filter is OR across categories.
  - Applied as intersection with the suite/tier base set.
- `--tests file1.py,file2.py`:
  - Hard override for exact tests.
  - Bypasses suite/tier/category selection.
- `--list`:
  - Print selected plan only.
- `--list-categories`:
  - Print all categories and mapped tests.
- `--audit` / `--skip-audit`:
  - Existing V4 audit behavior.
  - Audit is supported only for default V4 tier mode (no category or explicit test overrides).

### Selection Precedence

Selection precedence is:

1. `--tests` (hard override)
2. base selection from `--suite` + `--tier`
3. category intersection from `--categories`

If category intersection yields no tests, the runner fails fast with an explicit error.

## Functional Categories

Single source of truth:

- `regression_suite/tests/suite_manifest.py`
- `CATEGORY_DEFINITIONS`
- `TEST_CATEGORY_MAP`

Categories:

- `contracts`:
  - Module/config/window/consistency contracts.
- `integration`:
  - Cross-case end-to-end and scenario tests.
- `case3_routing`:
  - Case III hot/cold/overlap/lane routing.
- `case3_soft_delete`:
  - Case III soft-delete routing and propagation.
- `working_scope`:
  - Working-set/base-scope behavior.
- `idempotency_recovery`:
  - Idempotency and recovery stability.
- `bulk_history`:
  - Bulk/long-history behavior.
- `null_updates`:
  - Null/sentinel/null-soft-delete behavior.
- `hist_rpt`:
  - `acct_dt` and `hist_rpt` behavior.
- `audit`:
  - Snapshot/export style audits.
- `latest_generation`:
  - latest-summary generation and 36/72 prefix validation.
- `performance`:
  - Benchmark and heavy performance tests.

To see the exact current mapping:

```bash
python regression_suite/tests/run_all_tests_v4.py --list-categories
```

## Run Recipes

### Local

List category map:

```bash
python regression_suite/tests/run_all_tests_v4.py --list-categories
```

Default V4 smoke (backward-compatible behavior):

```bash
python regression_suite/tests/run_all_tests_v4.py --tier smoke
```

V4 nightly:

```bash
python regression_suite/tests/run_all_tests_v4.py --tier nightly
```

V4 category subset:

```bash
python regression_suite/tests/run_all_tests_v4.py --suite v4 --tier nightly --categories case3_routing,case3_soft_delete --list
```

Core-only category run:

```bash
python regression_suite/tests/run_all_tests_v4.py --suite core --categories audit,hist_rpt --list
```

All-suite category run:

```bash
python regression_suite/tests/run_all_tests_v4.py --suite all --tier nightly --categories integration --list
```

Explicit override:

```bash
python regression_suite/tests/run_all_tests_v4.py --tests test_v4_case3_comprehensive.py
```

### Docker

Smoke:

```bash
docker compose -f docker/docker-compose.yml exec -T spark-iceberg-main \
  python3 "/workspace/Feature _Deployment/regression_suite/tests/run_all_tests_v4.py" \
  --tier smoke
```

Category-targeted:

```bash
docker compose -f docker/docker-compose.yml exec -T spark-iceberg-main \
  python3 "/workspace/Feature _Deployment/regression_suite/tests/run_all_tests_v4.py" \
  --suite all --tier nightly --categories case3_routing,case3_soft_delete --list
```

## Troubleshooting

Unknown category:

- Cause: typo or unsupported category.
- Action: run `--list-categories` and use exact names.

No tests selected:

- Cause: suite/tier base set and category filter have empty intersection.
- Action: relax category set or adjust suite/tier.

Audit command rejected:

- Cause: audit currently supports default V4 tier mode only.
- Action: run audit without `--suite` override, `--categories`, or `--tests`.

Long runtime:

- Expected for categories including `performance`, `bulk_history`, or comprehensive matrices.
- Use `--list` before execution to inspect selected tests.

## Maintenance Rules

When adding a new runnable test file under `regression_suite/tests`:

1. Add it to the appropriate suite list (`CORE_TESTS` or V4 lists).
2. Ensure it receives functional categories via `TEST_CATEGORY_MAP` derivation/overrides.
3. Keep V4 wrapper exclusion contract intact for V4 tier manifests.

Validation commands:

```bash
python -m py_compile regression_suite/tests/suite_manifest.py regression_suite/tests/run_all_tests_v4.py
python regression_suite/tests/run_all_tests_v4.py --list-categories
python regression_suite/tests/run_all_tests_v4.py --suite v4 --tier smoke --list
```
