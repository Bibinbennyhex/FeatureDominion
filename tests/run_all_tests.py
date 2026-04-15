"""
Complete test suite runner for main/docker_test/tests.

Features:
- runs full test plan from suite_manifest
- streams logs to console while saving per-test logs
- saves JSON summary + table output under artifacts
- optionally runs audit_before_after_all_tests.py and saves its output
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from tests.suite_manifest import build_test_plan


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _print_header(text: str):
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


def _run_audit_runner(tests_dir: Path, audit_dir: Path, logs_dir: Path) -> dict:
    _print_header("RUNNING audit_before_after_all_tests.py")

    env = dict(os.environ)
    env["AUDIT_OUTPUT_DIR"] = str(audit_dir)

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


def main():
    parser = argparse.ArgumentParser(description="Run full main docker test suite with logs and audits")
    parser.add_argument("--include-performance", action="store_true", help="Run performance benchmark")
    parser.add_argument(
        "--performance-scale",
        default="TINY",
        choices=["TINY", "SMALL", "MEDIUM", "LARGE"],
        help="Scale for performance benchmark",
    )
    parser.add_argument("--include-aggressive", action="store_true", help="Run aggressive idempotency test")
    parser.add_argument("--skip-audit", action="store_true", help="Skip audit_before_after_all_tests.py")
    parser.add_argument(
        "--tests",
        default="",
        help="Comma-separated test files to run (overrides suite manifest)",
    )
    parser.add_argument("--list", action="store_true", help="Only print test plan and exit")
    args = parser.parse_args()

    tests_dir = Path(__file__).resolve().parent
    artifacts_dir = tests_dir / "artifacts"
    run_id = _utc_run_id()
    run_dir = artifacts_dir / "logs" / f"run_{run_id}"
    audit_dir = artifacts_dir / "audits" / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    test_plan = build_test_plan(include_aggressive=args.include_aggressive)

    if args.tests.strip():
        selected = [x.strip() for x in args.tests.split(",") if x.strip()]
        test_plan = [{"file": test_name, "args": []} for test_name in selected]

    if args.include_performance:
        test_plan.append(
            {
                "file": "test_performance_benchmark.py",
                "args": ["--scale", args.performance_scale],
            }
        )

    if args.list:
        print("Planned tests:")
        for item in test_plan:
            full = f"{item['file']} {' '.join(item['args'])}".strip()
            print(f"- {full}")
        if not args.skip_audit:
            print("- audit_before_after_all_tests.py")
        print(f"log_root={run_dir}")
        print(f"audit_root={audit_dir}")
        return

    _print_header(f"SUITE RUN STARTED | run_id={run_id}")
    print(f"log_root={run_dir}")
    print(f"audit_root={audit_dir}")

    results: list[dict] = []
    for item in test_plan:
        results.append(_run_one_test(tests_dir, run_dir, item["file"], item.get("args", [])))

    if not args.skip_audit:
        results.append(_run_audit_runner(tests_dir, audit_dir, run_dir))

    table = _format_table(results)
    print("\n=== SUITE SUMMARY ===")
    print(table)

    payload = {
        "run_id": run_id,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "log_root": str(run_dir),
        "audit_root": str(audit_dir),
        "results": results,
    }

    summary_json = run_dir / "suite_summary.json"
    summary_txt = run_dir / "suite_summary.txt"
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    summary_txt.write_text(table + "\n", encoding="utf-8")

    # Compatibility pointer for existing workflows.
    latest_json = tests_dir / "_last_run.json"
    latest_log = tests_dir / "_last_run.log"
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_log.write_text(table + "\n", encoding="utf-8")

    failures = [r for r in results if r["status"] != "PASS"]
    if failures:
        print("\nFAILED TESTS:")
        for row in failures:
            print(f"- {row['test']} (log: {run_dir / row['log']})")
        sys.exit(1)

    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    main()
