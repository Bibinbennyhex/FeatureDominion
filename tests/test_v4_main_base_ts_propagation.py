from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_main_base_ts_propagation():
    run_counterpart_test("test_main_base_ts_propagation.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_main_base_ts_propagation.py", "docker")
    print("[PASS] test_v4_main_base_ts_propagation.py")
