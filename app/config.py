import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RULE_VERSION = "wafer-1.0.0"
DEFAULT_SAMPLE = "mixed"
MAX_INPUT_BYTES = 2 * 1024 * 1024


def database_path():
    return Path(os.environ.get("WAFER_DB", "wafer-review.sqlite3"))


def storage_budget():
    """Optional monthly archive budget in bytes; ``0``/unset means no advertised cap."""
    try:
        budget = int(os.environ.get("WAFER_STORAGE_BUDGET_BYTES", "0"))
    except ValueError:
        return None
    return budget if budget > 0 else None
