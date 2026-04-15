from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_soft_delete_case_iii():
    run_counterpart_test("test_soft_delete_case_iii.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_soft_delete_case_iii.py", "docker")
    print("[PASS] test_v4_soft_delete_case_iii.py")
