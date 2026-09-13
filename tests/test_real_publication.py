import copy
import json
from datetime import UTC, datetime

import pytest

from snow_statistics.real_publication import (
    read_real_release,
    release_real,
    validate_real_model,
    validate_real_pair,
)

NOW = datetime(2026, 9, 14, tzinfo=UTC)


def packages():
    cutoff = "2026-09-13T16:00:00Z"
    coverage = dict(schema_version=1, source="real", instance_id="a854339f-9447-4e28-a22f-7fd48a8a8b96",
                    generation="d4184e1c-3cbe-4a59-a631-9f6215699c29", continuous_from="2026-08-31T16:00:00Z",
                    through=cutoff, gaps=[])
    common = dict(schema_version=1, source="real", run_id="test", cutoff=cutoff, date_from="2026-09-01", date_to="2026-09-13",
                  engine="Spark 3.5.7", master="yarn", application_id="application_123_0001", output="/snow/warehouse/real/test/runs/test",
                  input="hdfs://snow-control:9000/snow/ods/real/kafka/test/snapshots/" + "a" * 64 + "/_snapshot.json",
                  input_snapshot={"snapshot_id": "a" * 64, "source": "real", "batches": 1, "offsets": {"snow.real.events.v1:0": 2},
                                  "collector": {"schema_version": 1, "source": "real", "instance_id": coverage["instance_id"], "generation": coverage["generation"]}},
                  quality={"raw": 2, "valid": 2, "duplicates": 0, "quarantined": 0, "after_cutoff": 0})
    daily = dict(schema_version=1, manifest=common.copy(), daily=[dict(source="real", app="mywebsite", date="2026-09-05", pv=1, uv=1, requests=0, successes=0)])
    aggregates = dict(session_daily=[dict(source="real", app="mywebsite", date="2026-09-05", sessions=2, users=1, events=2,
                                         duration_seconds=0.0, closed_sessions=2)],
        retention=[dict(source="real", app="mywebsite", cohort_date="2026-09-05", users=1, eligible_d1=1, retained_d1=0,
                        eligible_d7=1, retained_d7=1, observation_d1="complete_accepted_prefix", observation_d7="complete_accepted_prefix")], funnel=[])
    model = dict(schema_version=2, manifest=common | dict(kind="real_behavior", output="hdfs://snow-control:9000/snow/warehouse/real/test/model-runs/test/behavior",
                   cohort_definition="first_observed_in_retained_30d_window", observation_scope="accepted_events_only",
                   coverage=coverage, retained_from=coverage["continuous_from"], complete_through="2026-09-13",
                   counts={"sessions": 2, "session_daily": 1, "retention": 1, "conversions": 0, "funnel": 0}), aggregates=aggregates)
    return daily, model


def test_real_release_is_event_only_deterministic_and_same_input_required(tmp_path):
    daily, model = packages()
    assert validate_real_pair(daily, model)
    first = release_real(daily, model, tmp_path, "first", now=NOW)
    model["manifest"]["run_id"] = "retry"
    second = release_real(daily, model, tmp_path, "second", now=NOW)
    assert first["content_hash"] == second["content_hash"]
    assert set(second) == {"schema_version", "source", "run_id", "daily", "behavior", "content_hash", "expires_at"}
    assert "operations" not in json.dumps(second)
    original = (tmp_path / "real-latest.json").read_bytes()
    model["aggregates"]["retention"][0]["retained_d1"] = 1
    with pytest.raises(ValueError, match="conflicting"):
        release_real(daily, model, tmp_path, "different", now=NOW)
    assert (tmp_path / "real-latest.json").read_bytes() == original
    assert read_real_release(tmp_path, NOW) == second
    model["manifest"]["input_snapshot"]["snapshot_id"] = "b" * 64
    daily["manifest"]["input_snapshot"] = copy.deepcopy(model["manifest"]["input_snapshot"])
    daily["manifest"]["input_snapshot"]["snapshot_id"] = "a" * 64
    with pytest.raises(ValueError, match="same immutable"):
        validate_real_pair(daily, model)


def test_real_dashboard_archive_is_cleaned_before_expired_read(tmp_path):
    daily, model = packages()
    release_real(daily, model, tmp_path, "first", now=NOW)
    pointer = json.loads((tmp_path / "real-latest.json").read_bytes())
    assert "daily" not in pointer and "behavior" not in pointer
    path = tmp_path / "data" / pointer["file"]
    with pytest.raises(FileNotFoundError, match="expired"):
        read_real_release(tmp_path, datetime(2026, 12, 1, tzinfo=UTC))
    assert not path.exists()
    with pytest.raises(ValueError, match="already expired"):
        release_real(daily, model, tmp_path, "copy", now=datetime(2026, 12, 1, tzinfo=UTC))


def test_gapped_or_unmatured_retention_must_remain_null():
    _, model = packages()
    model["manifest"]["coverage"]["gaps"] = [{"from": "2026-09-08T00:00:00Z", "to": "2026-09-08T01:00:00Z", "reason": "retention_gap"}]
    with pytest.raises(ValueError, match="status"):
        validate_real_model(model)
    row = model["aggregates"]["retention"][0]
    for lag in (1, 7):
        row[f"observation_d{lag}"] = "incomplete"
        row[f"eligible_d{lag}"] = 0
        row[f"retained_d{lag}"] = None
    assert validate_real_model(model)
    row["retained_d7"] = 0
    with pytest.raises(ValueError, match="null"):
        validate_real_model(model)


@pytest.mark.parametrize("mutation", [
    lambda model: model["manifest"].update(source="synthetic"),
    lambda model: model["manifest"].update(cohort_definition="first_lifetime_visit"),
    lambda model: model["manifest"].update(complete_through="2026-09-20"),
    lambda model: model["manifest"].update(retained_from="2025-01-01T00:00:00Z"),
    lambda model: model["aggregates"]["retention"][0].update(anonymous_id="never publish"),
    lambda model: model["aggregates"].update(ticket_daily=[]),
    lambda model: model["manifest"].update(raw_event={"anonymous_id": "private"}),
    lambda model: model["manifest"]["quality"].update(request_id="private"),
    lambda model: model["manifest"]["input_snapshot"]["collector"].update(generation="8be045b5-54e6-4d5c-bb99-9f3a642d811e"),
])
def test_real_contract_rejects_mixed_sources_private_fields_and_false_coverage(mutation):
    _, model = packages()
    changed = copy.deepcopy(model)
    mutation(changed)
    with pytest.raises(ValueError):
        validate_real_model(changed)
