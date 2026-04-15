from __future__ import annotations

import concurrent.futures as cf
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


TESTS_DIR = Path(__file__).resolve().parent
MAIN_DIR = TESTS_DIR.parents[1]
DOCKER_TESTS_DIR = MAIN_DIR / "docker_test" / "tests"


def _sanitize_token(name: str) -> str:
    token = name.replace(".py", "").replace("test_v4_", "")
    token = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in token.lower())
    return token[:40] if len(token) > 40 else token


def _discover_tests(tests_dir: Path) -> list[str]:
    return sorted(
        p.name
        for p in tests_dir.glob("test_v4_*.py")
        if p.name != "test_v4_unified_all_cases.py"
    )


def _filter_tests(tests: list[str]) -> list[str]:
    raw = os.environ.get("V4_TEST_NAMES", "").strip()
    if not raw:
        return tests

    requested = [item.strip() for item in raw.split(",") if item.strip()]
    selected = [name for name in tests if name in requested]
    missing = [name for name in requested if name not in tests]
    if missing:
        print(f"[warn] requested tests not found: {', '.join(missing)}", flush=True)
    return selected


def _build_assignments(tests: list[str], run_token: str) -> list[dict]:
    assignments = []
    for idx, test_name in enumerate(tests, 1):
        safe_token = _sanitize_token(test_name)
        assignments.append(
            {
                "idx": idx,
                "test": test_name,
                "namespace": f"v4p_{run_token}_{idx:03d}_{safe_token}"[:120],
                "temp_catalog_warehouse": f"file:///tmp/v4_parallel/{run_token}/{idx:03d}_{safe_token}/",
            }
        )
    return assignments


def _create_namespace_session() -> SparkSession:
    return (
        SparkSession.builder.appName("v4_parallel_namespace_precreate")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.primary_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.primary_catalog.type", "rest")
        .config("spark.sql.catalog.primary_catalog.uri", "http://rest:8181")
        .config("spark.sql.catalog.primary_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.primary_catalog.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.primary_catalog.s3.endpoint", "http://minio:9000")
        .config("spark.sql.defaultCatalog", "primary_catalog")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _precreate_namespaces(assignments: list[dict]) -> None:
    spark = _create_namespace_session()
    try:
        for item in assignments:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS primary_catalog.{item['namespace']}")
            print(f"[precreate] namespace={item['namespace']}", flush=True)
    finally:
        spark.stop()


def _precreate_tables(assignments: list[dict]) -> None:
    docker_tests_dir = str(DOCKER_TESTS_DIR)
    if docker_tests_dir not in sys.path:
        sys.path.insert(0, docker_tests_dir)
    import tests.test_utils as docker_test_utils  # noqa: WPS433

    spark = docker_test_utils.create_spark_session("v4_parallel_table_precreate")
    try:
        for item in assignments:
            os.environ["TEST_NAMESPACE_OVERRIDE"] = item["namespace"]
            os.environ["TEST_TEMP_CATALOG_WAREHOUSE"] = item["temp_catalog_warehouse"]
            config = docker_test_utils.load_main_test_config(item["namespace"])
            docker_test_utils.precreate_tables(spark, config)
            print(f"[precreate] tables={item['namespace']}", flush=True)
    finally:
        os.environ.pop("TEST_NAMESPACE_OVERRIDE", None)
        os.environ.pop("TEST_TEMP_CATALOG_WAREHOUSE", None)
        spark.stop()


def _run_one(
    tests_dir: Path,
    run_dir: Path,
    total: int,
    assignment: dict,
) -> dict:
    idx = assignment["idx"]
    test_name = assignment["test"]
    namespace = assignment["namespace"]
    temp_wh = assignment["temp_catalog_warehouse"]
    log_path = run_dir / f"{test_name}.log"
    cmd = [sys.executable, str(tests_dir / test_name)]
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["TEST_NAMESPACE_OVERRIDE"] = namespace
    env["TEST_TEMP_CATALOG_WAREHOUSE"] = temp_wh
    env["TEST_USE_PRECREATED_TABLES"] = "1"
    env["TEST_PRECREATED_ASSUME_EMPTY"] = "1"

    started = time.time()
    with log_path.open("w", encoding="utf-8", newline="\n") as lf:
        lf.write(f"[isolation] test={test_name}\n")
        lf.write(f"[isolation] namespace={namespace}\n")
        lf.write(f"[isolation] temp_catalog_warehouse={temp_wh}\n")
        lf.flush()
        proc = subprocess.Popen(cmd, cwd=str(tests_dir), stdout=lf, stderr=subprocess.STDOUT, env=env)
        rc = proc.wait()
    duration = round(time.time() - started, 2)
    status = "PASS" if rc == 0 else "FAIL"
    print(f"[{idx}/{total}] {test_name}: {status} ({duration}s)", flush=True)
    return {
        "test": test_name,
        "status": status,
        "duration_sec": duration,
        "exit_code": rc,
        "log": log_path.name,
        "namespace": namespace,
        "temp_catalog_warehouse": temp_wh,
    }


def _format_table(results: list[dict]) -> str:
    headers = ["test", "status", "duration_sec", "exit_code", "log"]
    widths = {h: len(h) for h in headers}
    for row in results:
        for h in headers:
            widths[h] = max(widths[h], len(str(row[h])))
    line = " | ".join(h.ljust(widths[h]) for h in headers)
    sep = "-+-".join("-" * widths[h] for h in headers)
    body = [" | ".join(str(r[h]).ljust(widths[h]) for h in headers) for r in results]
    return "\n".join([line, sep] + body)


def main() -> int:
    tests_dir = TESTS_DIR
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_token = run_id.lower().replace("t", "").replace("z", "")
    run_dir = tests_dir / "artifacts" / "parallel_isolated" / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    tests = _filter_tests(_discover_tests(tests_dir))
    if not tests:
        print("No tests selected.", flush=True)
        return 1
    assignments = _build_assignments(tests, run_token)
    max_workers = int(os.environ.get("V4_PARALLEL_WORKERS", "4"))
    max_workers = max(1, min(max_workers, len(tests)))

    _precreate_namespaces(assignments)
    _precreate_tables(assignments)

    results: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for assignment in assignments:
            futures.append(ex.submit(_run_one, tests_dir, run_dir, len(tests), assignment))
        for f in cf.as_completed(futures):
            results.append(f.result())

    results.sort(key=lambda r: r["test"])
    table = _format_table(results)

    payload = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "max_workers": max_workers,
        "total": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] != "PASS"),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (run_dir / "summary.txt").write_text(table + "\n", encoding="utf-8")

    print("\n=== SUMMARY TABLE ===")
    print(table)
    print(f"\nartifacts={run_dir}")
    print(f"pass={payload['passed']} fail={payload['failed']}")

    return 0 if payload["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
