from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_long_backfill_gaps():
    run_counterpart_test("test_long_backfill_gaps.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_long_backfill_gaps.py", "docker")
    print("[PASS] test_v4_long_backfill_gaps.py")
