from pathlib import Path
import py_compile

from tests.v4_pipeline_selector import resolve_pipeline_path


def run_test():
    root = Path(__file__).resolve().parents[3]
    target = resolve_pipeline_path(root, default_script="summary_inc_v4.1.py")
    content = target.read_text(encoding="utf-8")

    py_compile.compile(str(target), doraise=True)

    required = [
        "get_summary_history_len",
        "get_latest_history_len",
        "align_history_arrays_to_length",
        "latest_history_preserve_tail_expr",
        "run_pipeline",
        "cleanup",
    ]
    for name in required:
        assert f"def {name}(" in content, f"missing required symbol: {name}"

    has_legacy_case3 = "def process_case_iii(" in content
    has_split_case3 = all(
        symbol in content
        for symbol in (
            "def process_case_iii_hot(",
            "def process_case_iii_cold(",
            "def process_case_iii_mixed(",
        )
    )
    assert has_legacy_case3 or has_split_case3, (
        "missing Case III entry points (expected legacy process_case_iii or split lane handlers)"
    )

    print("[PASS] test_v4_module_smoke")


if __name__ == "__main__":
    run_test()
