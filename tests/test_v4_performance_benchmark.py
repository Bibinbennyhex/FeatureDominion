from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_performance_benchmark():
    run_counterpart_test("test_performance_benchmark.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_performance_benchmark.py", "docker")
    print("[PASS] test_v4_performance_benchmark.py")
