from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_generate_latest_summary_from_summary():
    run_counterpart_test("test_generate_latest_summary_from_summary.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_generate_latest_summary_from_summary.py", "docker")
    print("[PASS] test_v4_generate_latest_summary_from_summary.py")
