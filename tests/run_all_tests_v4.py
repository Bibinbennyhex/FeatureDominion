import subprocess
import sys
from pathlib import Path


TESTS = [
    "test_v4_module_smoke.py",
    "test_v4_config_contract.py",
]


def main():
    here = Path(__file__).resolve().parent
    failures = []
    for test in TESTS:
        test_path = here / test
        print(f"[RUN] {test}")
        result = subprocess.run([sys.executable, str(test_path)], cwd=str(here))
        if result.returncode != 0:
            failures.append(test)

    if failures:
        print("\n[FAIL] Some v4 tests failed:")
        for t in failures:
            print(f"- {t}")
        raise SystemExit(1)

    print("\n[PASS] All v4 tests passed")


if __name__ == "__main__":
    main()

