from datetime import datetime, timedelta
from pathlib import Path
import importlib.util
import sys
import types

from pyspark.sql import functions as F
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
    spec = importlib.util.spec_from_file_location("_summary_inc_v4_latest72", v4_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {v4_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["summary_inc"] = module
    return module


def _month_to_int(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 12 + int(mon)


def _int_to_month(value: int) -> str:
    year = value // 12
    mon = value % 12
    if mon == 0:
        year -= 1
        mon = 12
    return f"{year:04d}-{mon:02d}"


def _month_span(start_month: str, end_month: str):
    start = _month_to_int(start_month)
    end = _month_to_int(end_month)
    return [_int_to_month(v) for v in range(start, end + 1)]


def _build_72_history_row(tu, cons_acct_key: int, rpt_as_of_mo: str, base_ts: datetime, balance: int, payment: int):
    return tu.build_summary_row(
        cons_acct_key=cons_acct_key,
        rpt_as_of_mo=rpt_as_of_mo,
        base_ts=base_ts,
        balance=balance,
        actual_payment=payment,
        balance_history=tu.history({0: balance, 36: balance - 36}, length=72),
        payment_history=tu.history({0: payment, 36: payment - 36}, length=72),
        credit_history=tu.history({0: 10000, 36: 10000}, length=72),
        past_due_history=tu.history({0: 0, 36: 0}, length=72),
        rating_history=tu.history({0: "0", 36: "0"}, length=72),
        dpd_history=tu.history({0: 0, 36: 0}, length=72),
        asset_history=tu.history({0: "A", 36: "A"}, length=72),
    )


def _build_36_history_row(tu, cons_acct_key: int, rpt_as_of_mo: str, base_ts: datetime, balance: int, payment: int):
    return tu.build_summary_row(
        cons_acct_key=cons_acct_key,
        rpt_as_of_mo=rpt_as_of_mo,
        base_ts=base_ts,
        balance=balance,
        actual_payment=payment,
        balance_history=tu.history({0: balance}, length=36),
        payment_history=tu.history({0: payment}, length=36),
        credit_history=tu.history({0: 10000}, length=36),
        past_due_history=tu.history({0: 0}, length=36),
        rating_history=tu.history({0: "0"}, length=36),
        dpd_history=tu.history({0: 0}, length=36),
        asset_history=tu.history({0: "A"}, length=36),
    )


def test_v4_latest_summary_72_idempotent_bulk36():
    repo_root = Path(__file__).resolve().parents[3]
    module = _load_v4_as_summary_inc(repo_root)

    tests_dir = repo_root / "main" / "docker_test" / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_latest_summary_72_idempotent_bulk36")
    namespace = "v4_latest_summary_72_idempotent_bulk36"
    config = tu.load_main_test_config(namespace)
    config["history_length"] = 36
    config["latest_history_window_months"] = 72
    config["validate_latest_history_window"] = True

    target_acct = 880001
    anchor_acct = 880099
    start_month = "2024-01"
    end_month = "2026-12"
    bulk_months = _month_span(start_month, end_month)
    if len(bulk_months) != 36:
        raise AssertionError(f"Expected 36 months in bulk window, got {len(bulk_months)}")

    try:
        tu.reset_tables(spark, config)

        seed_ts = datetime(2026, 1, 1, 0, 0, 0)
        target_latest_seed = _build_72_history_row(
            tu, target_acct, end_month, seed_ts, balance=9000, payment=500
        )
        anchor_latest_seed = _build_72_history_row(
            tu, anchor_acct, start_month, seed_ts + timedelta(seconds=1), balance=1000, payment=50
        )
        target_summary_seed = _build_36_history_row(
            tu, target_acct, end_month, seed_ts, balance=9000, payment=500
        )
        anchor_summary_seed = _build_36_history_row(
            tu, anchor_acct, start_month, seed_ts + timedelta(seconds=1), balance=1000, payment=50
        )

        # Seed summary as 36-history, latest_summary as 72-history (V4 contract).
        tu.write_summary_rows(spark, config["destination_table"], [target_summary_seed, anchor_summary_seed])
        tu.write_summary_rows(spark, config["latest_history_table"], [target_latest_seed, anchor_latest_seed])

        source_rows = []
        batch_ts = datetime(2026, 2, 1, 0, 0, 0)
        month_to_balance = {}
        month_to_payment = {}
        for idx, month in enumerate(bulk_months):
            bal = 2000 + idx
            pay = 300 + idx
            month_to_balance[month] = bal
            month_to_payment[month] = pay
            source_rows.append(
                tu.build_source_row(
                    cons_acct_key=target_acct,
                    rpt_as_of_mo=month,
                    base_ts=batch_ts + timedelta(seconds=idx),
                    balance=bal,
                    actual_payment=pay,
                )
            )

        tu.write_source_rows(spark, config["source_table"], source_rows)

        module.cleanup(spark)
        module.run_pipeline(spark, config)

        latest_after_run1 = tu.fetch_single_row(spark, config["latest_history_table"], target_acct, end_month)
        history_cols = [
            "actual_payment_am_history",
            "balance_am_history",
            "credit_limit_am_history",
            "past_due_am_history",
            "payment_rating_cd_history",
            "days_past_due_history",
            "asset_class_cd_4in_history",
        ]
        for col_name in history_cols:
            if len(latest_after_run1[col_name]) != 72:
                raise AssertionError(f"{col_name} length must be 72, got {len(latest_after_run1[col_name])}")

        expected_balance_prefix = [month_to_balance[m] for m in reversed(bulk_months)]
        expected_payment_prefix = [month_to_payment[m] for m in reversed(bulk_months)]
        if latest_after_run1["balance_am_history"][:36] != expected_balance_prefix:
            raise AssertionError("balance_am_history first 36 values do not match expected bulk update")
        if latest_after_run1["actual_payment_am_history"][:36] != expected_payment_prefix:
            raise AssertionError("actual_payment_am_history first 36 values do not match expected bulk update")

        summary_month_count = (
            spark.table(config["destination_table"])
            .filter(F.col("cons_acct_key") == target_acct)
            .filter((F.col("rpt_as_of_mo") >= start_month) & (F.col("rpt_as_of_mo") <= end_month))
            .select("rpt_as_of_mo")
            .distinct()
            .count()
        )
        if summary_month_count != 36:
            raise AssertionError(f"Expected 36 distinct summary months for target account, got {summary_month_count}")

        run1_snapshot = {c: list(latest_after_run1[c]) for c in history_cols}

        # Rerun without new source input - must be idempotent for all 72-index arrays.
        module.cleanup(spark)
        module.run_pipeline(spark, config)

        latest_after_run2 = tu.fetch_single_row(spark, config["latest_history_table"], target_acct, end_month)
        for col_name in history_cols:
            if list(latest_after_run2[col_name]) != run1_snapshot[col_name]:
                raise AssertionError(f"{col_name} changed on idempotency rerun")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_latest_summary_72_idempotent_bulk36()
    print("[PASS] test_v4_latest_summary_72_idempotent_bulk36")
