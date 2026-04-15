from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_full_46_columns():
    run_counterpart_test("test_full_46_columns.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_full_46_columns.py", "docker")
    print("[PASS] test_v4_full_46_columns.py")
