from __future__ import annotations

import concurrent.futures as cf
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DOCKER_DIR = ROOT / "counterparts" / "docker"
V2_DIR = ROOT / "counterparts" / "v2"
ARTIFACT_ROOT = ROOT / "artifacts" / "counterparts"


def _discover():
    items = []
    for p in sorted(DOCKER_DIR.glob("test_*.py")):
        items.append(("docker", p))
    for p in sorted(V2_DIR.glob("test_*.py")):
        items.append(("v2", p))
    return items


def _run_one(suite: str, path: Path, run_dir: Path):
    log_dir = run_dir / suite
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{path.name}.log"
    cmd = [sys.executable, str(path)]
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as f:
        proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT, env=dict(os.environ))
        rc = proc.wait()
    duration = round(time.perf_counter() - started, 2)
    return {
        "suite": suite,
        "test": path.name,
        "status": "PASS" if rc == 0 else "FAIL",
        "duration_sec": duration,
        "exit_code": rc,
        "log": str(log_path.relative_to(run_dir)),
    }


def _table(rows):
    headers = ["test", "suite", "status", "duration_sec", "exit_code", "log"]
    widths = {h: len(h) for h in headers}
    for r in rows:
        for h in headers:
            widths[h] = max(widths[h], len(str(r[h])))
    line = " | ".join(h.ljust(widths[h]) for h in headers)
    sep = "-+-".join("-" * widths[h] for h in headers)
    body = [" | ".join(str(r[h]).ljust(widths[h]) for h in headers) for r in rows]
    return "\n".join([line, sep] + body)


def main(max_workers: int | None = None):
    tests = _discover()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ARTIFACT_ROOT / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    if max_workers is None:
        max_workers = int(os.environ.get("V4_COUNTERPART_MAX_WORKERS", "4"))
    workers = max(1, min(len(tests), max_workers))
    results = []
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_run_one, s, p, run_dir) for s, p in tests]
        for f in cf.as_completed(futs):
            results.append(f.result())

    results.sort(key=lambda x: (x["suite"], x["test"]))
    table = _table(results)
    print(table)

    payload = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "total": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] != "PASS"),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (run_dir / "summary.txt").write_text(table + "\n", encoding="utf-8")

    v4_case_cmd = [sys.executable, str(ROOT / "test_v4_case_end_to_end_smoke.py")]
    v4_case_log = run_dir / "test_v4_case_end_to_end_smoke.log"
    with v4_case_log.open("w", encoding="utf-8") as f:
        rc = subprocess.call(v4_case_cmd, cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT)
    print(f"v4_case_end_to_end_smoke: {'PASS' if rc == 0 else 'FAIL'} (log={v4_case_log.relative_to(run_dir)})")

    if payload["failed"] > 0 or rc != 0:
        return 1
    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run all v4 counterparts in parallel")
    parser.add_argument("--max-workers", type=int, default=None, help="Parallel worker cap (default: env V4_COUNTERPART_MAX_WORKERS or 4)")
    args = parser.parse_args()
    raise SystemExit(main(max_workers=args.max_workers))
