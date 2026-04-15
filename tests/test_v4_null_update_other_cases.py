from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_null_update_other_cases():
    run_counterpart_test("test_null_update_other_cases.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_null_update_other_cases.py", "docker")
    print("[PASS] test_v4_null_update_other_cases.py")
