"""Driver-side shared contracts; no Python UDF needs this path on executors."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from snow_statistics.real_behavior import (  # noqa: E402
    AUX_FIELDS,
    auxiliary_manifest,
    observation_status,
    real_path,
    stamp,
    validate_auxiliary_manifest,
    validate_coverage,
)

__all__ = ["AUX_FIELDS", "auxiliary_manifest", "observation_status", "real_path", "stamp",
           "validate_auxiliary_manifest", "validate_coverage"]
