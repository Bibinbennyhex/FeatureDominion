from __future__ import annotations

import os
from pathlib import Path


V4_PIPELINE_SCRIPT_ENV = "V4_PIPELINE_SCRIPT"
DEFAULT_PIPELINE_SCRIPT = "summary_inc_v4.py"


def repo_root() -> Path:
    # regression_suite/tests/v4_pipeline_selector.py -> repo root is parents[2]
    return Path(__file__).resolve().parents[2]


def resolve_pipeline_path(
    root: Path | None = None,
    default_script: str = DEFAULT_PIPELINE_SCRIPT,
) -> Path:
    """
    Resolve the v4 pipeline module path using env override when present.

    Supported V4_PIPELINE_SCRIPT values:
    - "summary_inc_v4.py"
    - "summary_inc_v4.py"
    - absolute file path
    - repo-relative path (for example: src/summary_inc_v4.py)
    """
    root = root or repo_root()
    raw = (os.environ.get(V4_PIPELINE_SCRIPT_ENV) or default_script).strip()
    candidate = Path(raw)

    if candidate.is_absolute():
        resolved = candidate
    else:
        # Try repo-relative first.
        direct = root / candidate
        if direct.exists():
            resolved = direct
        elif len(candidate.parts) == 1:
            # Backward-compatible fallbacks.
            fallbacks = [
                root / "src" / candidate.name,
                root / "main" / "summary_version_4" / candidate.name,
            ]
            resolved = next((p for p in fallbacks if p.exists()), fallbacks[0])
        else:
            resolved = direct

    if not resolved.exists():
        raise FileNotFoundError(
            f"Pipeline script not found: {resolved} "
            f"(set {V4_PIPELINE_SCRIPT_ENV}=src/summary_inc_v4.py)"
        )
    return resolved

