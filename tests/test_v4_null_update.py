from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_null_update():
    run_counterpart_test("test_null_update.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_null_update.py", "docker")
    print("[PASS] test_v4_null_update.py")
