from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_non_continuous_backfill():
    run_counterpart_test("test_non_continuous_backfill.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_non_continuous_backfill.py", "docker")
    print("[PASS] test_v4_non_continuous_backfill.py")
