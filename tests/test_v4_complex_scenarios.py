from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_complex_scenarios():
    run_counterpart_test("test_complex_scenarios.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_complex_scenarios.py", "docker")
    print("[PASS] test_v4_complex_scenarios.py")
