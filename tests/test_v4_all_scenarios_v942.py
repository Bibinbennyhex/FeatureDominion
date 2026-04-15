from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_all_scenarios_v942():
    run_counterpart_test("test_all_scenarios_v942.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_all_scenarios_v942.py", "docker")
    print("[PASS] test_v4_all_scenarios_v942.py")
