import copy
import importlib.util
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from snow_statistics.public_catalog import ORIGINS, make_snapshot
from snow_statistics.public_dimensions import DimensionIndex, build_bundle, validate_bundle

ROOT = Path(__file__).resolve().parents[1]


def stamp(day, hour=0):
    return f"2026-01-{day:02d}T{hour:02d}:00:00+00:00"


def snapshot(number, day, paths, characters):
    return make_snapshot(dict(schema_version=1, stage="published", apps={
        app: dict(release_sha=str(number) * 40, release_at=stamp(day), origin=origin,
                  paths=paths if app == "mywebsite" else ["/"],
                  characters=[] if app == "mywebsite" else characters)
        for app, origin in ORIGINS.items()
    }), now=datetime(2026, 1, day, 1, tzinfo=UTC))


def specification(*, continuous=False, source="synthetic"):
    first = snapshot(1, 1, ["/", "/old"], ["old-role", "stable-role"])
    second = snapshot(2, 3, ["/", "/new"], ["new-role", "stable-role"])
    spec = dict(schema_version=1, source=source, knowledge_cutoff=stamp(6),
                snapshots=[first, second], attestations=[], coverage=[])
    for snap, day, observed_end, history_end in ((first, 1, 2, 3 if continuous else 2), (second, 3, 5, 5)):
        for app in ORIGINS:
            spec["attestations"].append(dict(snapshot_id=snap["snapshot_id"], app=app,
                effective_at=stamp(day), release_evidence_sha256="a" * 64,
                observed_at=stamp(day, 1), checked_through=stamp(observed_end),
                observation_evidence_sha256="b" * 64))
            spec["coverage"].append(dict(snapshot_id=snap["snapshot_id"], app=app,
                covered_from=stamp(day), covered_to=stamp(history_end), verified_at=stamp(6),
                evidence_sha256="c" * 64))
    return spec


def resolve(index, at, key="/old", **kw):
    return index.resolve(source="synthetic", app="mywebsite", kind="page", key=key, at=at, **kw)


def test_history_gaps_and_closed_observation_windows_are_independent():
    bundle = build_bundle(specification())
    index = DimensionIndex(bundle)
    assert resolve(index, stamp(1))["status"] == "present"
    assert resolve(index, "2025-12-31T23:59:59Z")["status"] == "unknown"
    assert resolve(index, stamp(2))["status"] == "unknown"
    assert resolve(index, stamp(2), mode="current")["status"] == "present"
    assert resolve(index, stamp(2, 1), mode="current")["status"] == "unknown"
    assert resolve(index, stamp(5, 1), mode="current")["status"] == "unknown"
    assert resolve(index, stamp(5))["status"] == "unknown"
    old = [row for row in bundle["releases"] if row["release_sha"] == "1" * 40]
    assert all(row["valid_to"] is None for row in old)


def test_continuous_evidence_closes_scd2_with_half_open_boundary_and_tombstones():
    bundle = build_bundle(specification(continuous=True))
    index = DimensionIndex(bundle)
    assert resolve(index, stamp(2, 23))["status"] == "present"
    removed = resolve(index, stamp(3))
    assert removed["status"] == "not_present" and removed["dimension_id"]
    assert resolve(index, stamp(3), "/new")["status"] == "present"
    first = [r for r in bundle["releases"] if r["release_sha"] == "1" * 40]
    assert all(r["valid_from"] == "2026-01-01T00:00:00.000000+00:00" for r in first)
    assert all(r["valid_to"] == "2026-01-03T00:00:00.000000+00:00" for r in first)


def test_late_fact_occurrence_and_current_have_different_meaning_without_identifier_persistence():
    index = DimensionIndex(build_bundle(specification()))
    fact = dict(source="synthetic", app="project_snow", occurred_at=stamp(1, 2), character_id="old-role")
    result = index.resolve_fact(fact, current_as_of=stamp(4))
    assert result["character"]["at_event"]["status"] == "present"
    assert result["character"]["current"]["status"] == "not_present"
    assert "old-role" not in json.dumps(result)
    assert "occurred_at" not in json.dumps(result)
    with pytest.raises(ValueError, match="minimal"):
        index.resolve_fact(fact | {"anonymous_id": "not-accepted"}, current_as_of=stamp(4))


def test_unknown_effective_time_never_uses_observation_as_valid_from():
    spec = specification()
    spec["coverage"] = []
    for att in spec["attestations"]:
        att.update(effective_at=None, release_evidence_sha256=None)
    bundle = build_bundle(spec)
    assert all(r["valid_from"] is None for r in bundle["releases"])
    index = DimensionIndex(bundle)
    assert resolve(index, stamp(1, 2))["status"] == "unknown"
    assert resolve(index, stamp(1, 2), mode="current")["status"] == "present"
    spec["coverage"] = specification()["coverage"]
    with pytest.raises(ValueError, match="verified release"):
        build_bundle(spec)


def test_duplicates_and_permutation_are_idempotent_for_materialized_tables():
    original = specification()
    duplicate = copy.deepcopy(original)
    duplicate["snapshots"] += copy.deepcopy(duplicate["snapshots"])
    duplicate["attestations"] += copy.deepcopy(duplicate["attestations"])
    duplicate["coverage"] += copy.deepcopy(duplicate["coverage"])
    for key in ("snapshots", "attestations", "coverage"):
        duplicate[key].reverse()
    left, right = build_bundle(original), build_bundle(duplicate)
    for key in ("releases", "observations", "coverage", "dimensions"):
        assert left[key] == right[key]
    assert left["artifact_id"] == right["artifact_id"]
    assert validate_bundle(left) == left


@pytest.mark.parametrize("change", [
    lambda s: s["coverage"][0].update(covered_to=stamp(4)),
    lambda s: s["coverage"][0].update(covered_from="2025-12-31T00:00:00Z"),
    lambda s: s["coverage"][0].update(verified_at=stamp(1)),
    lambda s: s["attestations"][0].update(checked_through=stamp(4)),
    lambda s: s["attestations"][0].update(effective_at=stamp(1, 1)),
    lambda s: s["attestations"][0].update(observed_at=stamp(1, 3), checked_through=stamp(1, 2)),
    lambda s: s["attestations"][0].update(release_evidence_sha256="not-a-hash"),
    lambda s: s["attestations"][0].update(checked_through=stamp(7)),
    lambda s: s["coverage"][0].update(covered_to=stamp(2).replace("+00:00", "")),
    lambda s: s["attestations"].pop(),
    lambda s: s.update(source="mixed"),
    lambda s: s.update(secret="not-admitted"),
])
def test_rejects_unverified_or_inconsistent_temporal_inputs(change):
    spec = specification()
    change(spec)
    with pytest.raises(ValueError):
        build_bundle(spec)


def test_same_git_sha_cannot_silently_change_catalogue():
    spec = specification()
    spec["snapshots"][1] = snapshot(1, 3, ["/", "/different"], ["other-role"])
    with pytest.raises(ValueError, match="Same Git release"):
        build_bundle(spec)


def test_same_timestamp_different_releases_rejected():
    spec = specification()
    other = snapshot(2, 1, ["/new"], ["new-role"])
    old_id = spec["snapshots"][1]["snapshot_id"]
    spec["snapshots"][1] = other
    for item in spec["attestations"]:
        if item["snapshot_id"] == old_id:
            item.update(snapshot_id=other["snapshot_id"], effective_at=stamp(1))
    with pytest.raises(ValueError, match="effective instant"):
        build_bundle(spec)


def test_source_isolation_unsafe_paths_unknown_keys_and_tampering():
    bundle = build_bundle(specification())
    index = DimensionIndex(bundle)
    with pytest.raises(ValueError, match="mismatch"):
        index.resolve(source="real", app="mywebsite", kind="page", key="/", at=stamp(1))
    with pytest.raises(ValueError):
        resolve(index, stamp(1), "/?secret=value")
    missing = resolve(index, stamp(1), "/non-public-personal-text")
    assert missing["status"] == "not_present" and missing["dimension_id"] is None
    assert "non-public" not in json.dumps(missing)
    broken = copy.deepcopy(bundle)
    broken["dimensions"][0]["present"] = not broken["dimensions"][0]["present"]
    with pytest.raises(ValueError, match="recomputed"):
        validate_bundle(broken)


def test_sql_join_matches_python_without_cross_source_or_gap_matches():
    bundle = build_bundle(specification())
    index = DimensionIndex(bundle)
    facts = [(0, "synthetic", "mywebsite", "page", "/old", stamp(1)),
             (1, "synthetic", "mywebsite", "page", "/old", stamp(2)),
             (2, "synthetic", "mywebsite", "page", "/old", stamp(3)),
             (3, "synthetic", "mywebsite", "page", "/new", stamp(3)),
             (4, "real", "mywebsite", "page", "/old", stamp(1))]
    with sqlite3.connect(":memory:") as db:
        db.executescript("""
            CREATE TABLE public_dim_coverage(source, app, release_id, covered_from, covered_to);
            CREATE TABLE public_dim_scd2(source, app, kind, dimension_key, release_id, dimension_id, present);
            CREATE TABLE public_dim_fact_projection(fact_ordinal,source,app,kind,dimension_key,occurred_at);
        """)
        for table, fields in (("public_dim_coverage", "source app release_id covered_from covered_to"),
                              ("public_dim_scd2", "source app kind dimension_key release_id dimension_id present")):
            rows = bundle["coverage" if table.endswith("coverage") else "dimensions"]
            names = fields.split()
            db.executemany(f"INSERT INTO {table} VALUES ({','.join('?' for _ in names)})",
                           [[row[name] for name in names] for row in rows])
        normalized = [row[:-1] + (datetime.fromisoformat(row[-1]).isoformat(timespec="microseconds"),) for row in facts]
        db.executemany("INSERT INTO public_dim_fact_projection VALUES (?,?,?,?,?,?)", normalized)
        actual = db.execute((ROOT / "warehouse/spark/public_dimensions_at_event.sql").read_text()).fetchall()
    assert len(actual) == len(facts)
    for row in actual:
        fact = facts[row[0]]
        expected = "unknown" if fact[1] == "real" else resolve(index, fact[-1], fact[-2])["status"]
        assert row[-1] == expected


def test_cli_default_does_not_read_even_an_input_argument(tmp_path, capsys):
    module_spec = importlib.util.spec_from_file_location("dim_cli", ROOT / "tools/public_dimensions.py")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    assert module.main(["--input", str(tmp_path / "does-not-exist.json")]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "describe" and result["input_read"] is False
    data = tmp_path / "public-input.json"
    data.write_text(json.dumps(specification()))
    assert module.main(["--input", str(data), "--check"]) == 0
    assert json.loads(capsys.readouterr().out)["files_written"] is False


def test_committed_example_matches_tested_synthetic_case():
    fixture = json.loads((ROOT / "examples/public-dimensions.synthetic.json").read_text(encoding="utf-8"))
    assert fixture == specification()
    assert build_bundle(fixture)["source"] == "synthetic"


def test_cli_execute_fresh_artifact_and_refuses_to_overwrite(tmp_path, capsys):
    module_spec = importlib.util.spec_from_file_location("dim_cli_write", ROOT / "tools/public_dimensions.py")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    checkout = tmp_path / "project"
    module.__file__ = str(checkout / "tools" / "public_dimensions.py")
    data = tmp_path / "input.json"
    data.write_text(json.dumps(specification()), encoding="utf-8")
    assert module.main(["--input", str(data), "--execute"]) == 0
    result = json.loads(capsys.readouterr().out)
    target = checkout / "runtime/public-dimensions/synthetic" / result["artifact_id"]
    assert validate_bundle(json.loads((target / "dimensions.json").read_bytes()))["source"] == "synthetic"
    assert json.loads((target / "manifest.json").read_bytes())["contains_fact_rows"] is False
    before = (target / "dimensions.json").read_bytes()
    with pytest.raises(FileExistsError):
        module.main(["--input", str(data), "--execute"])
    assert (target / "dimensions.json").read_bytes() == before
    assert not (checkout / "runtime/public-dimensions/real").exists()


def test_index_owns_validated_copy_and_candidate_not_admitted():
    bundle = build_bundle(specification())
    index = DimensionIndex(bundle)
    bundle["coverage"].clear()
    assert resolve(index, stamp(1))["status"] == "present"
    spec = specification()
    spec["snapshots"][0]["stage"] = "candidate"
    with pytest.raises(ValueError):
        build_bundle(spec)
