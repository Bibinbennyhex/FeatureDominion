import json
from pathlib import Path


def run_test():
    root = Path(__file__).resolve().parents[1]
    cfg_path = root / "config_v4.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    assert cfg.get("latest_history_table"), "latest_history_table must be configured"
    assert int(cfg.get("latest_history_window_months", 0)) >= 72, "latest_history_window_months must be >= 72"
    assert bool(cfg.get("enable_case3_hot_cold_split", False)), "enable_case3_hot_cold_split must be true"
    assert bool(cfg.get("force_cold_case3_broadcast", False)), "force_cold_case3_broadcast must be true"
    assert int(cfg.get("cold_case3_broadcast_row_cap", 0)) == 10000000, "cold_case3_broadcast_row_cap should be 10M"

    print("[PASS] test_v4_config_contract")


if __name__ == "__main__":
    run_test()
