from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_comprehensive_edge_cases():
    run_counterpart_test("test_comprehensive_edge_cases.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_comprehensive_edge_cases.py", "docker")
    print("[PASS] test_v4_comprehensive_edge_cases.py")
