from datetime import datetime
from pathlib import Path
import importlib.util
import sys
import types

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
    spec = importlib.util.spec_from_file_location("_summary_inc_v4_window_contract", v4_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {v4_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["summary_inc"] = module
    return module


def _h36(tu, value_now, value_prev):
    return tu.history({0: value_now, 1: value_prev}, length=36)


def _h72(tu, value_now, value_prev, value_older):
    return tu.history({0: value_now, 1: value_prev, 36: value_older}, length=72)


def test_v4_summary36_latest72_window_update():
    repo_root = Path(__file__).resolve().parents[3]
    module = _load_v4_as_summary_inc(repo_root)

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_summary36_latest72_window_update")
    namespace = "v4_summary36_latest72_window_update"
    config = tu.load_main_test_config(namespace)
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True

    acct = 990001
    base_month = "2026-01"
    update_month = "2026-01"
    seed_ts = datetime(2026, 1, 1, 0, 0, 0)
    update_ts = datetime(2026, 2, 1, 12, 0, 0)

    history_cols = [
        "actual_payment_am_history",
        "balance_am_history",
        "credit_limit_am_history",
        "past_due_am_history",
        "payment_rating_cd_history",
        "days_past_due_history",
        "asset_class_cd_4in_history",
    ]
    sentinel_at_36 = {
        "actual_payment_am_history": 50,
        "balance_am_history": 500,
        "credit_limit_am_history": 4500,
        "past_due_am_history": 0,
        "payment_rating_cd_history": "1",
        "days_past_due_history": 30,
        "asset_class_cd_4in_history": "B",
    }

    try:
        tu.reset_tables(spark, config)

        summary_seed = tu.build_summary_row(
            cons_acct_key=acct,
            rpt_as_of_mo=base_month,
            base_ts=seed_ts,
            balance=1000,
            actual_payment=100,
            credit_limit=5000,
            past_due=0,
            days_past_due=0,
            payment_rating="0",
            asset_class="A",
            payment_history=_h36(tu, 100, 90),
            balance_history=_h36(tu, 1000, 900),
            credit_history=_h36(tu, 5000, 5000),
            past_due_history=_h36(tu, 0, 0),
            rating_history=tu.history({0: "0", 1: "0"}, length=36),
            dpd_history=_h36(tu, 0, 0),
            asset_history=tu.history({0: "A", 1: "A"}, length=36),
        )

        latest_seed = tu.build_summary_row(
            cons_acct_key=acct,
            rpt_as_of_mo=base_month,
            base_ts=seed_ts,
            balance=1000,
            actual_payment=100,
            credit_limit=5000,
            past_due=0,
            days_past_due=0,
            payment_rating="0",
            asset_class="A",
            payment_history=_h72(tu, 100, 90, 50),
            balance_history=_h72(tu, 1000, 900, 500),
            credit_history=_h72(tu, 5000, 5000, 4500),
            past_due_history=_h72(tu, 0, 0, 0),
            rating_history=tu.history({0: "0", 1: "0", 36: "1"}, length=72),
            dpd_history=_h72(tu, 0, 0, 30),
            asset_history=tu.history({0: "A", 1: "A", 36: "B"}, length=72),
        )

        tu.write_summary_rows(spark, config["destination_table"], [summary_seed])
        tu.write_summary_rows(spark, config["latest_history_table"], [latest_seed])

        source_rows = [
            tu.build_source_row(
                cons_acct_key=acct,
                rpt_as_of_mo=update_month,
                base_ts=update_ts,
                balance=1100,
                actual_payment=120,
                credit_limit=5000,
                past_due=0,
                days_past_due=0,
                asset_class="A",
            )
        ]
        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.cleanup(spark)
        module.run_pipeline(spark, config)

        summary_after = tu.fetch_single_row(spark, config["destination_table"], acct, update_month)
        latest_after = tu.fetch_single_row(spark, config["latest_history_table"], acct, update_month)

        for col_name in history_cols:
            summary_len = len(summary_after[col_name])
            latest_len = len(latest_after[col_name])
            if summary_len != 36:
                raise AssertionError(f"{col_name} in summary must be length 36, got {summary_len}")
            if latest_len != 72:
                raise AssertionError(f"{col_name} in latest_summary must be length 72, got {latest_len}")
            if list(summary_after[col_name]) != list(latest_after[col_name])[:36]:
                raise AssertionError(f"{col_name} summary(36) is not prefix of latest_summary(72)")
            if latest_after[col_name][36] != sentinel_at_36[col_name]:
                raise AssertionError(
                    f"{col_name} sentinel at index 36 changed unexpectedly: "
                    f"expected={sentinel_at_36[col_name]}, got={latest_after[col_name][36]}"
                )

        if summary_after["balance_am_history"][0] != 1100:
            raise AssertionError("summary balance_am_history index 0 was not updated to current-month value")
        if latest_after["balance_am_history"][0] != 1100:
            raise AssertionError("latest_summary balance_am_history index 0 was not updated to current-month value")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_summary36_latest72_window_update()
    print("[PASS] test_v4_summary36_latest72_window_update")
