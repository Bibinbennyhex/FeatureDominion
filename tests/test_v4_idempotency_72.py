from tests.v4_counterpart_adapter import run_counterpart_test_with_overrides


def test_v4_idempotency_72():
    run_counterpart_test_with_overrides(
        "test_idempotency.py",
        "docker",
        config_overrides={
            "history_length": 36,
            "latest_history_window_months": 72,
            "validate_latest_history_window": True,
        },
    )


if __name__ == "__main__":
    test_v4_idempotency_72()
    print("[PASS] test_v4_idempotency_72.py")
