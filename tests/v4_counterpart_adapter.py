"""
Generic launcher helpers for v4 counterpart test wrappers.
"""

from contextlib import contextmanager
import importlib.util
import importlib
import os
import runpy
import sys
import types
from pathlib import Path
from types import ModuleType

from tests.v4_pipeline_selector import resolve_pipeline_path


MODULE_ALIASES = ("summary_inc", "summary_inc_v2_unbounded")
MODULE_CACHE_NAME = "_summary_inc_v4_counterpart_module"


@contextmanager
def _temporary_cwd(path: Path):
    original = Path.cwd()
    try:
        os.chdir(str(path))
        yield
    finally:
        os.chdir(str(original))


@contextmanager
def _temporary_sys_path(path: Path):
    path_str = str(path)
    inserted = False
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        inserted = True
    try:
        yield
    finally:
        if inserted and path_str in sys.path:
            sys.path.remove(path_str)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _v4_module_path() -> Path:
    return resolve_pipeline_path(_repo_root(), default_script="summary_inc_v4.1.py")


def _source_tests_dir(suite: str) -> Path:
    if suite == "docker":
        return _repo_root() / "main" / "docker_test" / "tests"
    if suite == "v2":
        return _repo_root() / "main" / "v2_unbounded" / "tests"
    raise ValueError(f"Unknown suite: {suite}")


def _ensure_dependency_stubs() -> None:
    try:
        import boto3  # noqa: F401
    except Exception:
        boto3_stub = types.ModuleType("boto3")

        def _unsupported_client(*args, **kwargs):
            raise RuntimeError("boto3 client should not be used in counterpart tests")

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


def _load_v4_module() -> ModuleType:
    _ensure_dependency_stubs()
    cached = sys.modules.get(MODULE_CACHE_NAME)
    if isinstance(cached, ModuleType):
        for alias in MODULE_ALIASES:
            sys.modules[alias] = cached
        sys.modules["summary_inc_v4"] = cached
        return cached

    module_path = _v4_module_path()
    spec = importlib.util.spec_from_file_location(MODULE_CACHE_NAME, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load v4 pipeline module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_CACHE_NAME] = module
    sys.modules["summary_inc_v4"] = module
    for alias in MODULE_ALIASES:
        sys.modules[alias] = module

    spec.loader.exec_module(module)
    _patch_module_for_legacy_test_configs(module)
    return module


def _patch_module_for_legacy_test_configs(module: ModuleType) -> None:
    """
    Enforce V4 contract defaults for counterpart tests unless the source test
    explicitly overrides them:
      - summary history window: 36
      - latest_summary history window: 72
    """
    if getattr(module, "_counterpart_legacy_patch_applied", False):
        return

    original_run_pipeline = module.run_pipeline

    def _patched_run_pipeline(spark, config, *args, **kwargs):
        config.setdefault("history_length", 36)
        config.setdefault("latest_history_window_months", 72)
        config.setdefault("validate_latest_history_window", True)
        return original_run_pipeline(spark, config, *args, **kwargs)

    module.run_pipeline = _patched_run_pipeline
    module._counterpart_legacy_patch_applied = True


def run_counterpart_test(source_test_filename: str, suite: str) -> None:
    source_tests_dir = _source_tests_dir(suite)
    source_test_path = source_tests_dir / source_test_filename
    if not source_test_path.exists():
        raise FileNotFoundError(f"Source test not found: {source_test_path}")

    _load_v4_module()

    with _temporary_sys_path(source_tests_dir), _temporary_cwd(source_tests_dir):
        runpy.run_path(str(source_test_path), run_name="__main__")


@contextmanager
def _temporary_run_pipeline_config_overrides(module: ModuleType, overrides: dict, use_setdefault: bool = False):
    original_run_pipeline = module.run_pipeline

    def _patched_run_pipeline(spark, config, *args, **kwargs):
        for key, value in overrides.items():
            if use_setdefault:
                config.setdefault(key, value)
            else:
                config[key] = value
        return original_run_pipeline(spark, config, *args, **kwargs)

    module.run_pipeline = _patched_run_pipeline
    try:
        yield
    finally:
        module.run_pipeline = original_run_pipeline


def run_counterpart_test_with_overrides(
    source_test_filename: str,
    suite: str,
    config_overrides: dict,
    use_setdefault: bool = False,
) -> None:
    source_tests_dir = _source_tests_dir(suite)
    source_test_path = source_tests_dir / source_test_filename
    if not source_test_path.exists():
        raise FileNotFoundError(f"Source test not found: {source_test_path}")

    module = _load_v4_module()

    with _temporary_run_pipeline_config_overrides(module, config_overrides, use_setdefault):
        with _temporary_test_utils_history_override(source_tests_dir, config_overrides):
            with _temporary_sys_path(source_tests_dir), _temporary_cwd(source_tests_dir):
                runpy.run_path(str(source_test_path), run_name="__main__")


@contextmanager
def _temporary_test_utils_history_override(source_tests_dir: Path, config_overrides: dict):
    history_length = config_overrides.get("history_length")
    if not isinstance(history_length, int) or history_length <= 0:
        yield
        return

    with _temporary_sys_path(source_tests_dir):
        test_utils = importlib.import_module("test_utils")
        original_history = test_utils.history
        original_history_length = getattr(test_utils, "HISTORY_LENGTH", None)

        def _patched_history(positions, length=None):
            effective_length = history_length if length is None else length
            return original_history(positions, effective_length)

        test_utils.HISTORY_LENGTH = history_length
        test_utils.history = _patched_history
        try:
            yield
        finally:
            test_utils.history = original_history
            if original_history_length is None:
                try:
                    delattr(test_utils, "HISTORY_LENGTH")
                except Exception:
                    pass
            else:
                test_utils.HISTORY_LENGTH = original_history_length
