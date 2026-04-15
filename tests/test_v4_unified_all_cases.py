from pathlib import Path
import sys


TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from tests.v4_unified_single_runner import main


def test_v4_unified_all_cases():
    assert main([]) == 0


if __name__ == "__main__":
    raise SystemExit(main())
