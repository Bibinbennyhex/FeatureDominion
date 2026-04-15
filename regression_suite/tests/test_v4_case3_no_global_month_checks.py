from pathlib import Path


def _assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def test_v4_case3_no_global_month_checks():
    repo_root = Path(__file__).resolve().parents[2]
    src_file = repo_root / "src" / "summary_inc_v4.py"
    content = src_file.read_text(encoding="utf-8")

    forbidden_tokens = [
        "global_latest_month",
        "split by month: latest_month",
    ]
    for token in forbidden_tokens:
        _assert_true(token not in content, f"Forbidden global-month token still present: {token}")

    print("[PASS] test_v4_case3_no_global_month_checks")


if __name__ == "__main__":
    test_v4_case3_no_global_month_checks()
    print("[PASS] test_v4_case3_no_global_month_checks.py")
