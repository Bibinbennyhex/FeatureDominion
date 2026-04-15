from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_hist_rpt_acct_dt_soft_delete_resolution():
    run_counterpart_test("test_hist_rpt_acct_dt_soft_delete_resolution.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_hist_rpt_acct_dt_soft_delete_resolution.py", "docker")
    print("[PASS] test_v4_hist_rpt_acct_dt_soft_delete_resolution.py")
