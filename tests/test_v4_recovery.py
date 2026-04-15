from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_recovery():
    run_counterpart_test("test_recovery.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_recovery.py", "docker")
    print("[PASS] test_v4_recovery.py")
