from pathlib import Path
import sys

from tests.v4_contract_utils import (
    LATEST_HISTORY_LEN,
    SUMMARY_HISTORY_LEN,
    assert_latest_matches_summary_v4,
    load_python_module,
    load_v4_as_summary_inc,
    write_summary_rows_v4,
)


def test_v4_aggressive_idempotency():
    repo_root = Path(__file__).resolve().parents[2]
    load_v4_as_summary_inc("_summary_inc_v4_aggressive_idempotency")

    tests_dir = Path(__file__).resolve().parent
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    import tests.test_utils as tu  # noqa: WPS433

    source_module = load_python_module(
        tests_dir / "test_aggressive_idempotency.py",
        "_v4_source_test_aggressive_idempotency",
    )

    original_write_summary_rows = source_module.write_summary_rows
    original_assert_latest_matches = source_module._assert_latest_matches_summary

    def _patched_write_summary_rows(spark, table, rows):
        write_summary_rows_v4(tu, spark, table, rows, latest_len=LATEST_HISTORY_LEN)

    def _patched_assert_latest_matches_summary(spark, summary_table, latest_table):
        assert_latest_matches_summary_v4(
            spark,
            summary_table,
            latest_table,
            summary_len=SUMMARY_HISTORY_LEN,
            latest_len=LATEST_HISTORY_LEN,
        )

    source_module.write_summary_rows = _patched_write_summary_rows
    source_module._assert_latest_matches_summary = _patched_assert_latest_matches_summary

    try:
        source_module.run_test(scale="SMALL", cycles=4, reruns=1, seed=42)
    finally:
        source_module.write_summary_rows = original_write_summary_rows
        source_module._assert_latest_matches_summary = original_assert_latest_matches


if __name__ == "__main__":
    test_v4_aggressive_idempotency()
    print("[PASS] test_v4_aggressive_idempotency.py")

