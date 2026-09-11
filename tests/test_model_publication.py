import copy
import importlib.util
import json
from pathlib import Path

import pytest

from snow_statistics.behavior import analyze
from snow_statistics.behavior_fixture import generate_behavior
from snow_statistics.model_publication import release_models, validate_model


def packages():
    fixture = generate_behavior()
    window = {k: fixture[k] for k in ("date_from", "date_to", "cutoff")}
    model = analyze(fixture["events"], **window)
    shared = dict(source="synthetic", run_id="test-t1", input="hdfs://snow-control:9000/snow/ods/synthetic/kafka/test/snapshots/" + "a" * 64 + "/_snapshot.json",
                  input_snapshot={"snapshot_id": "a" * 64}, **window)
    behavior = dict(schema_version=1, manifest=shared | dict(kind="behavior", counts={k: len(model[k]) for k in ("sessions", "session_daily", "retention", "conversions", "funnel")},
                    quality=dict(raw=1, valid=1, duplicates=0, quarantined=0, after_cutoff=0)),
                    aggregates={k: model[k] for k in ("session_daily", "retention", "funnel")})
    operations = dict(schema_version=1, manifest=shared | dict(kind="operations", counts=dict(dim_content_scd2=0, fact_ticket_round=0, fact_ticket_daily=0)),
                      aggregates=dict(ticket_daily=[], current_categories=[]))
    return operations, behavior


def test_atomic_model_pair_and_replay(tmp_path):
    operations, behavior = packages()
    first = release_models(operations, behavior, tmp_path, "first")
    behavior["manifest"]["run_id"] = "retry"
    behavior["manifest"]["cutoff"] = behavior["manifest"]["cutoff"].replace("Z", "+00:00")
    assert release_models(operations, behavior, tmp_path, "second")["content_hash"] == first["content_hash"]
    old = (tmp_path / "models-latest.json").read_bytes()
    operations["manifest"]["counts"]["dim_content_scd2"] = 1
    with pytest.raises(ValueError, match="conflicting"):
        release_models(operations, behavior, tmp_path, "conflict")
    assert (tmp_path / "models-latest.json").read_bytes() == old
    operations["manifest"]["input_snapshot"] = {"snapshot_id": "b" * 64}
    with pytest.raises(ValueError, match="immutable"):
        release_models(operations, behavior, tmp_path, "mismatch")


@pytest.mark.parametrize("mutate", [
    lambda p: p["aggregates"]["funnel"][0].update(anonymous_id="private"),
    lambda p: p["aggregates"]["funnel"][0].update(converted=999),
    lambda p: p["manifest"]["quality"].update(valid=True),
    lambda p: p["aggregates"]["retention"][-1].update(retained_d7=0),
    lambda p: p["aggregates"]["session_daily"].append(copy.deepcopy(p["aggregates"]["session_daily"][0])),
])
def test_private_models_reject_invalid_results(mutate):
    _, behavior = packages()
    mutate(behavior)
    with pytest.raises(ValueError):
        validate_model(behavior)


def test_gateway_bounds_and_validated_retry(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("gateway", Path(__file__).resolve().parents[1] / "tools/airflow_gateway.py")
    gateway = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gateway)
    monkeypatch.setattr(gateway, "ROOT", tmp_path)
    (tmp_path / "lab").mkdir()
    (tmp_path / "lab/.env").write_text("CONTROL_IP=192.168.216.131\n")
    _, model = packages()
    m = model["manifest"]
    data = dict(SNOW_JOB_KIND="behavior", SNOW_DATE_FROM=m["date_from"], SNOW_DATE_TO=m["date_to"], SNOW_CUTOFF=m["cutoff"],
                SNOW_RUN_ID=m["run_id"], SNOW_SOURCE="synthetic", SNOW_ODS_PATH=m["input"], SNOW_WAREHOUSE_PATH="hdfs://snow-control:9000/snow/warehouse")
    with pytest.raises(ValueError, match="Unknown"):
        gateway.execute(data | dict(SNOW_JOB_KIND="shell"))
    with pytest.raises(ValueError, match="frozen"):
        gateway.execute(data | dict(SNOW_ODS_PATH="hdfs://snow-control:9000/snow/ods/synthetic/events"))
    with pytest.raises(ValueError, match="namespace"):
        gateway.execute(data | dict(SNOW_WAREHOUSE_PATH="/snow/warehouse/../private"))
    m["output"] = data["SNOW_WAREHOUSE_PATH"] + "/model-runs/" + m["run_id"] + "/behavior"
    (tmp_path / "runtime/publication").mkdir(parents=True)
    (tmp_path / ("runtime/publication/" + m["run_id"] + ".behavior.json")).write_text(json.dumps(model))
    monkeypatch.setattr(gateway.subprocess, "run", lambda *a, **kw: pytest.fail("Validated retry must not start Spark"))
    gateway.execute(data)
    with pytest.raises(ValueError, match="different window"):
        gateway.execute(data | dict(SNOW_DATE_FROM="2026-01-02"))
