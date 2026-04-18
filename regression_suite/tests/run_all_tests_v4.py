from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from tests.suite_manifest import (
        TEST_CATEGORY_MAP,
        get_category_definitions,
        get_suite_tests,
        get_tests_by_categories,
        list_all_categories,
        validate_category_map,
        validate_v4_unique_suite,
    )
except ModuleNotFoundError:
    from suite_manifest import (
        TEST_CATEGORY_MAP,
        get_category_definitions,
        get_suite_tests,
        get_tests_by_categories,
        list_all_categories,
        validate_category_map,
        validate_v4_unique_suite,
    )


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _print_header(text: str) -> None:
    print("=" * 110)
    print(text)
    print("=" * 110)


def _stream_process(cmd: list[str], log_path: Path, cwd: Path, env: dict[str, str] | None = None) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_f:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            log_f.write(line)
        proc.wait()
        return proc.returncode


def _format_table(rows: list[dict]) -> str:
    headers = ["test", "status", "duration_sec", "exit_code", "log"]
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            widths[h] = max(widths[h], len(str(row.get(h, ""))))

    line = " | ".join(h.ljust(widths[h]) for h in headers)
    sep = "-+-".join("-" * widths[h] for h in headers)
    body = [" | ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers) for row in rows]
    return "\n".join([line, sep] + body)


def _run_one_test(
    tests_dir: Path,
    log_dir: Path,
    test_file: str,
    args: list[str] | None = None,
) -> dict:
    args = list(args or [])
    cmd = [sys.executable, str(tests_dir / test_file)] + args
    display = f"{test_file} {' '.join(args)}".strip()

    _print_header(f"RUNNING {display}")
    started = time.time()
    rc = _stream_process(cmd, log_dir / f"{test_file}.log", tests_dir)
    duration = round(time.time() - started, 2)
    status = "PASS" if rc == 0 else "FAIL"

    print(f"[RESULT] {display} -> {status} ({duration}s)")
    return {
        "test": display,
        "status": status,
        "duration_sec": duration,
        "exit_code": rc,
        "log": f"{test_file}.log",
    }


def _run_audit_runner(tests_dir: Path, audit_dir: Path, logs_dir: Path, tier: str) -> dict:
    _print_header("RUNNING audit_before_after_all_tests.py")

    env = dict(os.environ)
    env["AUDIT_OUTPUT_DIR"] = str(audit_dir)
    env["AUDIT_TIER"] = tier

    started = time.time()
    rc = _stream_process(
        [sys.executable, str(tests_dir / "audit_before_after_all_tests.py")],
        logs_dir / "audit_before_after_all_tests.py.log",
        tests_dir,
        env=env,
    )
    duration = round(time.time() - started, 2)
    status = "PASS" if rc == 0 else "FAIL"
    print(f"[RESULT] audit_before_after_all_tests.py -> {status} ({duration}s)")

    return {
        "test": "audit_before_after_all_tests.py",
        "status": status,
        "duration_sec": duration,
        "exit_code": rc,
        "log": "audit_before_after_all_tests.py.log",
    }


def _parse_categories(raw: str) -> set[str]:
    return {x.strip() for x in raw.split(",") if x.strip()}


def _print_categories() -> None:
    category_defs = get_category_definitions()
    all_categories = list_all_categories()
    tests_dir = Path(__file__).resolve().parent

    # Build reverse index for deterministic display.
    grouped: dict[str, list[str]] = {name: [] for name in all_categories}
    selected = get_suite_tests(suite="all", tier="nightly")
    for test_name in selected:
        for category in sorted(TEST_CATEGORY_MAP.get(test_name, set())):
            grouped.setdefault(category, []).append(test_name)

    print("Available categories:")
    for name in all_categories:
        tests = sorted(set(grouped.get(name, [])))
        print(f"- {name} ({len(tests)} tests): {category_defs.get(name, '')}")
        for test_name in tests:
            print(f"  - {test_name}")
    print(f"\nManifest source: {tests_dir / 'suite_manifest.py'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run regression suite with suite/tier/category selection")
    parser.add_argument(
        "--suite",
        default="v4",
        choices=["v4", "core", "all"],
        help="Test suite selection (default: v4)",
    )
    parser.add_argument(
        "--tier",
        default="smoke",
        choices=["smoke", "nightly"],
        help="Tier to run (smoke for PR, nightly for deep regression)",
    )
    parser.add_argument("--skip-audit", action="store_true", help="Skip audit_before_after_all_tests.py")
    parser.add_argument("--audit", action="store_true", help="Force audit run even for smoke tier")
    parser.add_argument(
        "--tests",
        default="",
        help="Comma-separated test files to run (hard override over suite/tier/categories)",
    )
    parser.add_argument(
        "--categories",
        default="",
        help="Comma-separated functional categories (OR semantics), intersected with suite/tier selection",
    )
    parser.add_argument("--list-categories", action="store_true", help="Print category catalog and exit")
    parser.add_argument("--list", action="store_true", help="Only print selected test plan and exit")
    args = parser.parse_args()

    tests_dir = Path(__file__).resolve().parent
    artifacts_dir = tests_dir / "artifacts"
    run_id = _utc_run_id()
    run_dir = artifacts_dir / "logs_v4" / f"run_{run_id}"
    audit_dir = artifacts_dir / "audits" / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    validate_category_map(tests_dir)
    if args.suite in {"v4", "all"}:
        validate_v4_unique_suite(tests_dir, tier=args.tier)

    if args.list_categories:
        _print_categories()
        return

    explicit_tests = [x.strip() for x in args.tests.split(",") if x.strip()]
    selected_categories = _parse_categories(args.categories)
    known_categories = set(list_all_categories())
    unknown_categories = sorted(selected_categories - known_categories)
    if unknown_categories:
        raise ValueError(
            "Unknown categories: "
            f"{unknown_categories}. Valid categories: {sorted(known_categories)}"
        )

    if explicit_tests:
        selected_tests = explicit_tests
    else:
        selected_tests = get_suite_tests(suite=args.suite, tier=args.tier)
        if selected_categories:
            selected_tests = get_tests_by_categories(selected_tests, selected_categories)
            if not selected_tests:
                raise ValueError(
                    "No tests selected after category intersection. "
                    f"suite={args.suite}, tier={args.tier}, categories={sorted(selected_categories)}"
                )

    missing_selected = [name for name in selected_tests if not (tests_dir / name).exists()]
    if missing_selected:
        raise FileNotFoundError(f"Selected tests not found: {missing_selected}")

    test_plan = [{"file": test_name, "args": []} for test_name in selected_tests]

    should_run_audit = (args.tier == "nightly" and not args.skip_audit) or args.audit
    if args.skip_audit:
        should_run_audit = False
    if should_run_audit and (args.suite != "v4" or selected_categories or explicit_tests):
        raise ValueError(
            "Audit runner is only supported for default v4 tier selection "
            "(no --suite override, no --categories, no --tests override)."
        )

    if args.list:
        print(
            "Planned tests "
            f"(suite={args.suite}, tier={args.tier}, categories={sorted(selected_categories) if selected_categories else 'none'}):"
        )
        for item in test_plan:
            full = f"{item['file']} {' '.join(item['args'])}".strip()
            print(f"- {full}")
        if should_run_audit:
            print("- audit_before_after_all_tests.py")
        print(f"log_root={run_dir}")
        print(f"audit_root={audit_dir}")
        return

    _print_header(
        "REGRESSION SUITE RUN STARTED | "
        f"suite={args.suite} | tier={args.tier} | categories={sorted(selected_categories) if selected_categories else 'none'} | run_id={run_id}"
    )
    print(f"log_root={run_dir}")
    print(f"audit_root={audit_dir}")

    results: list[dict] = []
    for item in test_plan:
        results.append(_run_one_test(tests_dir, run_dir, item["file"], item.get("args", [])))

    if should_run_audit:
        results.append(_run_audit_runner(tests_dir, audit_dir, run_dir, tier=args.tier))

    table = _format_table(results)
    print("\n=== REGRESSION SUITE SUMMARY ===")
    print(table)

    payload = {
        "run_id": run_id,
        "suite": args.suite,
        "tier": args.tier,
        "categories": sorted(selected_categories),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "log_root": str(run_dir),
        "audit_root": str(audit_dir),
        "results": results,
    }

    summary_json = run_dir / "suite_summary.json"
    summary_txt = run_dir / "suite_summary.txt"
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    summary_txt.write_text(table + "\n", encoding="utf-8")

    latest_json = tests_dir / "_last_run_v4.json"
    latest_log = tests_dir / "_last_run_v4.log"
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_log.write_text(table + "\n", encoding="utf-8")

    failures = [r for r in results if r["status"] != "PASS"]
    if failures:
        print("\nFAILED TESTS:")
        for row in failures:
            print(f"- {row['test']} (log: {run_dir / row['log']})")
        sys.exit(1)

    print("\nALL SELECTED TESTS PASSED")


if __name__ == "__main__":
    main()
