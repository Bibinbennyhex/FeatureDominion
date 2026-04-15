from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_duplicate_records():
    run_counterpart_test("test_duplicate_records.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_duplicate_records.py", "docker")
    print("[PASS] test_v4_duplicate_records.py")
