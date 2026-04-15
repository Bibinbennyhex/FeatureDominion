from pathlib import Path
import sys
import types
import importlib.util
from datetime import datetime

from tests.v4_pipeline_selector import resolve_pipeline_path


def _ensure_dependency_stubs():
    try:
        import boto3  # noqa: F401
    except Exception:
        boto3_stub = types.ModuleType("boto3")

        def _unsupported_client(*args, **kwargs):
            raise RuntimeError("boto3 client should not be used in v4 local tests")

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


def _load_v4_as_summary_inc(repo_root: Path):
    _ensure_dependency_stubs()
    v4_path = resolve_pipeline_path(repo_root, default_script="summary_inc_v4.1.py")
    spec = importlib.util.spec_from_file_location("_summary_inc_v4_case_test", v4_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {v4_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["summary_inc"] = module
    sys.modules["summary_inc_v2_unbounded"] = module
    return module


def test_v4_case_end_to_end_smoke():
    repo_root = Path(__file__).resolve().parents[3]
    module = _load_v4_as_summary_inc(repo_root)

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_case_end_to_end_smoke")
    namespace = "v4_case_end_to_end_smoke"
    config = tu.load_main_test_config(namespace)
    # V4 contract: summary=36, latest_summary=72.
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True

    try:
        tu.reset_tables(spark, config)

        # Seed an unrelated anchor row so destination metadata has a max month.
        # This avoids the known empty-summary edge in v4 smoke setup while
        # still validating target-account end-to-end flow under 36/72 contract.
        anchor_ts = datetime(2020, 1, 1, 0, 0, 0)
        anchor_summary = tu.build_summary_row(
            cons_acct_key=990999,
            rpt_as_of_mo="2026-12",
            base_ts=anchor_ts,
            balance=1000,
            actual_payment=100,
            balance_history=tu.history({0: 1000}, length=36),
            payment_history=tu.history({0: 100}, length=36),
            credit_history=tu.history({0: 5000}, length=36),
            past_due_history=tu.history({0: 0}, length=36),
            rating_history=tu.history({0: "0"}, length=36),
            dpd_history=tu.history({0: 0}, length=36),
            asset_history=tu.history({0: "A"}, length=36),
        )
        anchor_latest = tu.build_summary_row(
            cons_acct_key=990999,
            rpt_as_of_mo="2026-12",
            base_ts=anchor_ts,
            balance=1000,
            actual_payment=100,
            balance_history=tu.history({0: 1000}, length=72),
            payment_history=tu.history({0: 100}, length=72),
            credit_history=tu.history({0: 5000}, length=72),
            past_due_history=tu.history({0: 0}, length=72),
            rating_history=tu.history({0: "0"}, length=72),
            dpd_history=tu.history({0: 0}, length=72),
            asset_history=tu.history({0: "A"}, length=72),
        )
        tu.write_summary_rows(spark, config["destination_table"], [anchor_summary])
        anchor_summary_low = tu.build_summary_row(
            cons_acct_key=990998,
            rpt_as_of_mo="2020-01",
            base_ts=anchor_ts,
            balance=900,
            actual_payment=90,
            balance_history=tu.history({0: 900}, length=36),
            payment_history=tu.history({0: 90}, length=36),
            credit_history=tu.history({0: 4000}, length=36),
            past_due_history=tu.history({0: 0}, length=36),
            rating_history=tu.history({0: "0"}, length=36),
            dpd_history=tu.history({0: 0}, length=36),
            asset_history=tu.history({0: "A"}, length=36),
        )
        tu.write_summary_rows(spark, config["destination_table"], [anchor_summary_low])
        tu.write_summary_rows(spark, config["latest_history_table"], [anchor_latest])

        # Run 1: create initial month for target account (Case I),
        # plus an anchor account at a higher global month so run-2 forward
        # entry is inside the pipeline's configured month window.
        ts1 = datetime(2026, 1, 5, 0, 0, 0)
        tu.write_source_rows(
            spark,
            config["source_table"],
            [
                tu.build_source_row(990001, "2026-01", ts1, balance=5000, actual_payment=100),
                tu.build_source_row(990099, "2026-12", ts1, balance=9000, actual_payment=300),
            ],
        )
        module.run_pipeline(spark, config)

        s_jan = tu.fetch_single_row(spark, config["destination_table"], 990001, "2026-01")
        assert s_jan["balance_am_history"][0] == 5000

        # Run 2: forward month (Case II).
        ts2 = datetime(2026, 2, 5, 0, 0, 0)
        tu.write_source_rows(
            spark,
            config["source_table"],
            [tu.build_source_row(990001, "2026-02", ts2, balance=4800, actual_payment=200)],
        )
        module.run_pipeline(spark, config)

        s_feb = tu.fetch_single_row(spark, config["destination_table"], 990001, "2026-02")
        assert s_feb["balance_am_history"][0] == 4800
        assert s_feb["balance_am_history"][1] == 5000

        latest = tu.fetch_single_row(spark, config["latest_history_table"], 990001, "2026-02")
        assert latest["balance_am_history"][0] == 4800

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_case_end_to_end_smoke()
    print("[PASS] test_v4_case_end_to_end_smoke")
