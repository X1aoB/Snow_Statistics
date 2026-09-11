import copy
import json
from datetime import UTC, datetime

import pytest

from snow_statistics.model import build
from snow_statistics.publication import publication_lock, validate
from snow_statistics.scheduling import resolve_window
from snow_statistics.simulator import generate


def package():
    model = build(generate(users=2))
    return {"schema_version": 1, "manifest": {"run_id": "fixture", "source": "synthetic",
            "date_from": "2026-01-01", "date_to": "2026-01-04", "cutoff": "2026-01-05T00:00:00Z",
            "quality": model["quality"] | {"after_cutoff": 0}}, "daily": model["daily"]}


def test_publication_golden_grain_and_empty_dates():
    manifest, rows, hashes, version, cutoff = validate(package())
    assert len(rows) == 6 and len(hashes) == 4
    assert version == int(datetime(2026, 1, 5, tzinfo=UTC).timestamp() * 1_000_000)
    assert cutoff.tzinfo is None
    empty = package()
    empty["daily"] = []
    assert len(validate(empty)[2]) == 4  # Empty-day pointers remove obsolete daily rows.
    assert hashes == validate(json.loads(json.dumps(package())))[2]


@pytest.mark.parametrize("mutation", [
    lambda p: p["daily"][0].update(raw_id="private"),
    lambda p: p["daily"][0].update(source="real"),
    lambda p: p["daily"][0].update(pv=-1),
    lambda p: p["daily"][0].update(pv=True),
    lambda p: p["daily"].append(copy.deepcopy(p["daily"][0])),
    lambda p: p["manifest"]["quality"].update(raw=999),
    lambda p: p["manifest"].update(cutoff="2026-01-05T00:00:00"),
])
def test_publication_rejects_bad_payloads(mutation):
    value = package()
    mutation(value)
    with pytest.raises(ValueError):
        validate(value)


def test_single_writer_lock_releases_on_exception(tmp_path):
    with pytest.raises(RuntimeError), publication_lock(tmp_path):
        with pytest.raises(OSError), publication_lock(tmp_path):
            pass
        raise RuntimeError("publisher failed")
    with publication_lock(tmp_path):
        pass


def test_hong_kong_window_and_explicit_backfill():
    result = resolve_window("2026-01-08T19:00:00Z", "scheduled:example")
    assert result["start"] == "2026-01-02" and result["end"] == "2026-01-08"
    backfill = resolve_window("2026-09-11T00:00:00Z", "manual", "2026-01-01", "2026-01-04", "2026-01-05T00:00:00Z")
    assert backfill["cutoff"] == "2026-01-05T00:00:00+00:00"
    with pytest.raises(ValueError):
        resolve_window("2026-01-08T19:00:00Z", "bad", "2026-01-09", "2026-01-01")
