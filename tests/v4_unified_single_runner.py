from __future__ import annotations

import argparse
import concurrent.futures as cf
import importlib.util
import json
import os
import runpy
import subprocess
import sys
import time
import types
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve()
TESTS_DIR = SCRIPT_PATH.parent
REPO_ROOT = TESTS_DIR.parents[2]
MAIN_DIR = REPO_ROOT / "main"
DOCKER_TESTS_DIR = MAIN_DIR / "docker_test" / "tests"
V2_TESTS_DIR = MAIN_DIR / "v2_unbounded" / "tests"
V4_PIPELINE_PATH = MAIN_DIR / "summary_version_4" / "summary_inc_v4.py"
ARTIFACT_ROOT = TESTS_DIR / "artifacts" / "unified"
MODULE_CACHE_NAME = "_summary_version_4_unified_v4"
MODULE_ALIASES = ("summary_inc", "summary_inc_v2_unbounded", "summary_inc_v4")


@dataclass(frozen=True)
class TestSpec:
    suite: str
    filename: str
    source_path: Path


def _docker_test_names() -> list[str]:
    return sorted(
        path.name
        for path in DOCKER_TESTS_DIR.glob("test_*.py")
        if path.name != "test_utils.py"
    )


def _v2_test_names() -> list[str]:
    return sorted(
        path.name
        for path in V2_TESTS_DIR.glob("test_*.py")
        if path.name != "test_utils_v2.py"
    )


def _v2_to_docker_equivalent(filename: str) -> str:
    if not filename.startswith("test_v2_"):
        return filename
    return "test_" + filename[len("test_v2_"):]


def discover_tests() -> list[TestSpec]:
    docker_tests = _docker_test_names()
    docker_set = set(docker_tests)
    specs = [TestSpec("docker", name, DOCKER_TESTS_DIR / name) for name in docker_tests]

    for name in _v2_test_names():
        if _v2_to_docker_equivalent(name) not in docker_set:
            specs.append(TestSpec("v2", name, V2_TESTS_DIR / name))

    return specs


def _source_dir(spec: TestSpec) -> Path:
    return spec.source_path.parent.parent


def _source_tests_dir(spec: TestSpec) -> Path:
    return spec.source_path.parent


def _load_v4_module():
    _ensure_dependency_stubs()
    cached = sys.modules.get(MODULE_CACHE_NAME)
    if cached is not None:
        return cached

    spec = importlib.util.spec_from_file_location(MODULE_CACHE_NAME, V4_PIPELINE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load v4 pipeline module from {V4_PIPELINE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_CACHE_NAME] = module
    for alias in MODULE_ALIASES:
        sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


def _ensure_dependency_stubs() -> None:
    """
    Mirror lightweight dependency fallbacks used by existing local tests.
    This keeps unified runner behavior aligned with docker_test/v2 test_utils.
    """
    try:
        import boto3  # noqa: F401
    except Exception:
        boto3_stub = types.ModuleType("boto3")

        def _unsupported_client(*args, **kwargs):
            raise RuntimeError("boto3 client should not be used in unified tests")

        boto3_stub.client = _unsupported_client  # type: ignore[attr-defined]
        sys.modules["boto3"] = boto3_stub

    try:
        from dateutil.relativedelta import relativedelta  # noqa: F401
    except Exception:
        dateutil_mod = types.ModuleType("dateutil")
        relativedelta_mod = types.ModuleType("dateutil.relativedelta")

        class relativedelta:  # type: ignore
            def __init__(self, months: int = 0):
                self.months = months

        relativedelta_mod.relativedelta = relativedelta  # type: ignore[attr-defined]
        dateutil_mod.relativedelta = relativedelta_mod  # type: ignore[attr-defined]
        sys.modules["dateutil"] = dateutil_mod
        sys.modules["dateutil.relativedelta"] = relativedelta_mod


@contextmanager
def _patched_runtime(spec: TestSpec):
    previous_aliases = {name: sys.modules.get(name) for name in MODULE_ALIASES}
    previous_path = list(sys.path)
    previous_cwd = Path.cwd()
    try:
        module = _load_v4_module()
        for alias in MODULE_ALIASES:
            sys.modules[alias] = module

        for path in (
            str(_source_tests_dir(spec)),
            str(_source_dir(spec)),
            str(MAIN_DIR),
        ):
            if path not in sys.path:
                sys.path.insert(0, path)

        os.chdir(str(_source_tests_dir(spec)))
        yield
    finally:
        os.chdir(str(previous_cwd))
        sys.path[:] = previous_path
        for alias, previous in previous_aliases.items():
            if previous is None:
                sys.modules.pop(alias, None)
            else:
                sys.modules[alias] = previous


def _run_source_test(spec: TestSpec) -> None:
    if not spec.source_path.exists():
        raise FileNotFoundError(f"Source test not found: {spec.source_path}")

    with _patched_runtime(spec):
        runpy.run_path(str(spec.source_path), run_name="__main__")


def _worker_main(suite: str, test_name: str) -> int:
    spec = _spec_for(suite, test_name)
    print(f"[worker] suite={suite} test={test_name}")
    _run_source_test(spec)
    return 0


def _spec_for(suite: str, test_name: str) -> TestSpec:
    if suite == "docker":
        return TestSpec("docker", test_name, DOCKER_TESTS_DIR / test_name)
    if suite == "v2":
        return TestSpec("v2", test_name, V2_TESTS_DIR / test_name)
    raise ValueError(f"Unknown suite: {suite}")


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _format_table(results: list[dict]) -> str:
    headers = ["test", "suite", "status", "duration_sec", "log"]
    rows = [
        [
            result["test"],
            result["suite"],
            result["status"],
            f'{result["duration_sec"]:.2f}',
            result["log"],
        ]
        for result in results
    ]
    widths = [
        min(max(len(headers[idx]), max((len(row[idx]) for row in rows), default=0)), limit)
        for idx, limit in enumerate((42, 8, 8, 12, 48))
    ]

    def render_row(row: list[str]) -> str:
        cells = [
            _truncate(row[0], widths[0]).ljust(widths[0]),
            _truncate(row[1], widths[1]).ljust(widths[1]),
            _truncate(row[2], widths[2]).ljust(widths[2]),
            _truncate(row[3], widths[3]).rjust(widths[3]),
            _truncate(row[4], widths[4]),
        ]
        return f"{cells[0]}  {cells[1]}  {cells[2]}  {cells[3]}  {cells[4]}"

    lines = [
        render_row(headers),
        render_row(["-" * widths[0], "-" * widths[1], "-" * widths[2], "-" * widths[3], "-" * widths[4]]),
    ]
    lines.extend(render_row(row) for row in rows)
    return "\n".join(lines)


def _run_child_process(spec: TestSpec, run_dir: Path) -> dict:
    log_path = run_dir / spec.suite / f"{spec.filename}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(SCRIPT_PATH),
        "--worker",
        "--suite",
        spec.suite,
        "--test",
        spec.filename,
    ]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    started = time.perf_counter()
    rc = 1
    try:
        with log_path.open("w", encoding="utf-8", newline="\n") as log_file:
            process = subprocess.Popen(
                command,
                cwd=str(REPO_ROOT),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
            )
            rc = process.wait()
    except Exception as exc:
        log_path.write_text(f"[runner] failed to launch child process: {exc}\n", encoding="utf-8")
        rc = 1
    duration = time.perf_counter() - started
    status = "PASS" if rc == 0 else "FAIL"
    return {
        "suite": spec.suite,
        "test": spec.filename,
        "source_test": str(spec.source_path.relative_to(REPO_ROOT)),
        "status": status,
        "exit_code": rc,
        "duration_sec": duration,
        "log": str(log_path.relative_to(run_dir)),
    }


def _write_summaries(run_dir: Path, results: list[dict], started_utc: str) -> None:
    summary = {
        "generated_utc": started_utc,
        "run_dir": str(run_dir),
        "total": len(results),
        "passed": sum(1 for result in results if result["status"] == "PASS"),
        "failed": sum(1 for result in results if result["status"] != "PASS"),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "summary.txt").write_text(
        "\n".join(
            [
                f"run_dir: {run_dir}",
                f"generated_utc: {started_utc}",
                f"total: {summary['total']}",
                f"passed: {summary['passed']}",
                f"failed: {summary['failed']}",
                "",
                _format_table(results),
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def run_unified_harness() -> int:
    tests = discover_tests()
    started_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ARTIFACT_ROOT / f"run_{started_utc}"
    run_dir.mkdir(parents=True, exist_ok=True)

    max_workers = max(1, min(len(tests), os.cpu_count() or 1))
    results: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_run_child_process, spec, run_dir) for spec in tests]
        for future in cf.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {
                        "suite": "unknown",
                        "test": "unknown",
                        "source_test": "unknown",
                        "status": "FAIL",
                        "exit_code": 1,
                        "duration_sec": 0.0,
                        "log": f"unhandled future error: {exc}",
                    }
                )

    results.sort(key=lambda item: (item["suite"], item["test"]))
    _write_summaries(run_dir, results, started_utc)

    print(_format_table(results))
    print(f"\nartifacts: {run_dir}")

    failures = [result for result in results if result["status"] != "PASS"]
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Unified v4 test harness")
    parser.add_argument("--worker", action="store_true", help="Run a single test as a worker subprocess")
    parser.add_argument("--suite", choices=("docker", "v2"), help="Worker test suite")
    parser.add_argument("--test", help="Worker test filename")
    args = parser.parse_args(argv)

    if args.worker:
        if not args.suite or not args.test:
            parser.error("--worker requires --suite and --test")
        return _worker_main(args.suite, args.test)

    return run_unified_harness()


if __name__ == "__main__":
    raise SystemExit(main())
