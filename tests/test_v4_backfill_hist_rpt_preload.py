from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_backfill_hist_rpt_preload():
    run_counterpart_test("test_backfill_hist_rpt_preload.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_backfill_hist_rpt_preload.py", "docker")
    print("[PASS] test_v4_backfill_hist_rpt_preload.py")
