# Main Docker Test Suite

## Folder Structure

- `test_*.py`, `simple_test.py`, `run_backfill_test.py`: executable test entry points
- `scenario_suite.py`: shared scenario implementations
- `test_utils.py`: Spark + table setup helpers
- `suite_manifest.py`: canonical list of tests included in suite runs
- `run_all_tests.py`: primary suite runner (logs + summary + audit integration)
- `audit_before_after_all_tests.py`: audit runner for before/after snapshot checks
- `artifacts/logs/`: per-run test logs and suite summaries
- `artifacts/audits/`: audit JSON outputs and audit CSV exports

## Run Commands

- Run full suite:
  - `python main/docker_test/tests/run_all_tests.py`
- Include aggressive idempotency test:
  - `python main/docker_test/tests/run_all_tests.py --include-aggressive`
- Include performance test:
  - `python main/docker_test/tests/run_all_tests.py --include-performance --performance-scale TINY`
- Skip audit run:
  - `python main/docker_test/tests/run_all_tests.py --skip-audit`
- Print planned tests only:
  - `python main/docker_test/tests/run_all_tests.py --list`
- Run selected tests only:
  - `python main/docker_test/tests/run_all_tests.py --tests simple_test.py,test_main_all_cases.py --skip-audit`

## Outputs

Each suite run creates:

- `artifacts/logs/run_<UTC_TIMESTAMP>/`
  - one `<test_name>.log` per test
  - `suite_summary.txt` (table)
  - `suite_summary.json` (machine-readable results)
- `artifacts/audits/run_<UTC_TIMESTAMP>/`
  - `audit_before_after_results.json` (when audit is enabled)

Compatibility pointers are still written to:

- `_last_run.log`
- `_last_run.json`
- `_audit_before_after_results.json`
