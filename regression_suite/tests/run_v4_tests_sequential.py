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
    from tests.v4_pipeline_selector import DEFAULT_PIPELINE_SCRIPT, V4_PIPELINE_SCRIPT_ENV
except ModuleNotFoundError:
    from v4_pipeline_selector import DEFAULT_PIPELINE_SCRIPT, V4_PIPELINE_SCRIPT_ENV
try:
    from tests.suite_manifest import get_v4_tier_tests, validate_v4_unique_suite
except ModuleNotFoundError:
    from suite_manifest import get_v4_tier_tests, validate_v4_unique_suite


def main() -> int:
    parser = argparse.ArgumentParser(description="Run v4 tests sequentially")
    parser.add_argument(
        "--pipeline-script",
        default=DEFAULT_PIPELINE_SCRIPT,
        help=(
            "Pipeline script selector for all tests. "
            "Examples: summary_inc_v4.py, summary_inc_v4.py, or repo/absolute path."
        ),
    )
    parser.add_argument(
        "--tier",
        default=os.environ.get("V4_TIER", "nightly"),
        choices=["smoke", "nightly"],
        help="V4 suite tier to execute",
    )
    args = parser.parse_args()

    tests_dir = Path('/workspace/main/summary_version_4/tests')
    art_root = tests_dir / 'artifacts' / 'sequential'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_dir = art_root / f'run_{run_id}'
    run_dir.mkdir(parents=True, exist_ok=True)

    validate_v4_unique_suite(tests_dir, tier=args.tier)
    tests = get_v4_tier_tests(args.tier)
    results = []

    for idx, name in enumerate(tests, 1):
        log_path = run_dir / f'{name}.log'
        cmd = ['python3', str(tests_dir / name)]
        env = dict(os.environ)
        env['PYTHONUNBUFFERED'] = '1'
        env[V4_PIPELINE_SCRIPT_ENV] = args.pipeline_script
        started = time.time()
        print(
            f'running [{idx}/{len(tests)}] {name} '
            f'({V4_PIPELINE_SCRIPT_ENV}={args.pipeline_script}, tier={args.tier})'
        )
        with log_path.open('w', encoding='utf-8', newline='\n') as lf:
            proc = subprocess.Popen(
                cmd,
                cwd=str(tests_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                print(line, end='')
                lf.write(line)
            rc = proc.wait()
        dur = round(time.time() - started, 2)
        status = 'PASS' if rc == 0 else 'FAIL'
        print(f'[{idx}/{len(tests)}] {name}: {status} ({dur}s)')
        results.append({'test': name, 'status': status, 'duration_sec': dur, 'exit_code': rc, 'log': log_path.name})

    headers = ['test', 'status', 'duration_sec', 'exit_code', 'log']
    widths = {h: len(h) for h in headers}
    for row in results:
        for h in headers:
            widths[h] = max(widths[h], len(str(row[h])))
    line = ' | '.join(h.ljust(widths[h]) for h in headers)
    sep = '-+-'.join('-' * widths[h] for h in headers)
    body = [' | '.join(str(r[h]).ljust(widths[h]) for h in headers) for r in results]
    table = '\n'.join([line, sep] + body)

    print('\n=== SUMMARY TABLE ===')
    print(table)

    payload = {
        'run_id': run_id,
        'run_dir': str(run_dir),
        'pipeline_script': args.pipeline_script,
        'tier': args.tier,
        'total': len(results),
        'passed': sum(1 for r in results if r['status'] == 'PASS'),
        'failed': sum(1 for r in results if r['status'] != 'PASS'),
        'results': results,
    }

    (run_dir / 'summary.json').write_text(json.dumps(payload, indent=2), encoding='utf-8')
    (run_dir / 'summary.txt').write_text(table + '\n', encoding='utf-8')

    print(f"\n{V4_PIPELINE_SCRIPT_ENV}={args.pipeline_script}")
    print(f"artifacts={run_dir}")
    print(f"pass={payload['passed']} fail={payload['failed']}")

    return 0 if payload['failed'] == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())

