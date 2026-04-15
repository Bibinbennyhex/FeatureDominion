from datetime import datetime
from pathlib import Path
import sys

from tests.v4_contract_utils import (
    HISTORY_COLS,
    LATEST_HISTORY_LEN,
    SUMMARY_HISTORY_LEN,
    assert_latest_matches_summary_v4,
    load_v4_as_summary_inc,
)


def _month_iter(start_year: int, start_month: int, count: int):
    year = start_year
    month = start_month
    out = []
    for _ in range(count):
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return out


def test_v4_bulk_historical_load():
    repo_root = Path(__file__).resolve().parents[2]
    module = load_v4_as_summary_inc("_summary_inc_v4_bulk_historical_load")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    spark = tu.create_spark_session("v4_bulk_historical_load")
    namespace = "main_bulk"
    config = tu.load_main_test_config(namespace)
    config["history_length"] = SUMMARY_HISTORY_LEN
    config["latest_history_window_months"] = LATEST_HISTORY_LEN
    config["validate_latest_history_window"] = True

    try:
        tu.reset_tables(spark, config)

        ts = datetime(2026, 2, 1, 0, 0, 0)
        months = _month_iter(2023, 1, 48)
        bal = 10000
        rows = []
        for mo in months:
            rows.append(tu.build_source_row(9001, mo, ts, balance=bal, actual_payment=max(100, bal // 20)))
            bal -= 50

        tu.write_source_rows(spark, config["source_table"], rows)

        module.cleanup(spark)
        module.run_pipeline(spark, config)

        latest = tu.fetch_single_row(spark, config["latest_history_table"], 9001, months[-1])
        summary_latest = tu.fetch_single_row(spark, config["destination_table"], 9001, months[-1])

        for col_name in HISTORY_COLS:
            if len(summary_latest[col_name]) != SUMMARY_HISTORY_LEN:
                raise AssertionError(f"{col_name} in summary must be length {SUMMARY_HISTORY_LEN}")
            if len(latest[col_name]) != LATEST_HISTORY_LEN:
                raise AssertionError(f"{col_name} in latest_summary must be length {LATEST_HISTORY_LEN}")
            if list(summary_latest[col_name]) != list(latest[col_name])[:SUMMARY_HISTORY_LEN]:
                raise AssertionError(f"{col_name} summary prefix mismatch in bulk historical load")

        assert_latest_matches_summary_v4(spark, config["destination_table"], config["latest_history_table"])

    finally:
        spark.stop()


if __name__ == "__main__":
    test_v4_bulk_historical_load()
    print("[PASS] test_v4_bulk_historical_load.py")

