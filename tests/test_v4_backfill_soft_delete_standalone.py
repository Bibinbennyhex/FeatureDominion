from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_backfill_soft_delete_standalone():
    run_counterpart_test("test_backfill_soft_delete_standalone.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_backfill_soft_delete_standalone.py", "docker")
    print("[PASS] test_v4_backfill_soft_delete_standalone.py")
