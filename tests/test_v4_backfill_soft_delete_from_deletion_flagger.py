from tests.v4_counterpart_adapter import run_counterpart_test


def test_v4_backfill_soft_delete_from_deletion_flagger():
    run_counterpart_test("test_backfill_soft_delete_from_deletion_flagger.py", "docker")


if __name__ == "__main__":
    run_counterpart_test("test_backfill_soft_delete_from_deletion_flagger.py", "docker")
    print("[PASS] test_v4_backfill_soft_delete_from_deletion_flagger.py")
