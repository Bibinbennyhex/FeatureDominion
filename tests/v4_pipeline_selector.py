from __future__ import annotations

import os
from pathlib import Path


V4_PIPELINE_SCRIPT_ENV = "V4_PIPELINE_SCRIPT"
DEFAULT_PIPELINE_SCRIPT = "summary_inc_v4.1.py"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_pipeline_path(
    root: Path | None = None,
    default_script: str = DEFAULT_PIPELINE_SCRIPT,
) -> Path:
    """
    Resolve the v4 pipeline module path using env override when present.

    Supported V4_PIPELINE_SCRIPT values:
    - "summary_inc_v4.py"
    - "summary_inc_v4.1.py"
    - absolute file path
    - repo-relative path (for example: main/summary_version_4/summary_inc_v4.py)
    """
    root = root or repo_root()
    raw = (os.environ.get(V4_PIPELINE_SCRIPT_ENV) or default_script).strip()
    candidate = Path(raw)

    if candidate.is_absolute():
        resolved = candidate
    elif len(candidate.parts) == 1:
        resolved = root / "main" / "summary_version_4" / candidate.name
    else:
        resolved = root / candidate

    if not resolved.exists():
        raise FileNotFoundError(
            f"Pipeline script not found: {resolved} "
            f"(set {V4_PIPELINE_SCRIPT_ENV}=summary_inc_v4.py or summary_inc_v4.1.py)"
        )
    return resolved

