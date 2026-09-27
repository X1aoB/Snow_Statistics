import copy
import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from snow_statistics.public_catalog import (
    SOURCE_FILES,
    capture_releases,
    collector_allowlist_from_env,
    compare_collector,
    make_snapshot,
    save_snapshot,
    validate_snapshot,
)

NOW = datetime(2026, 9, 14, tzinfo=UTC)


def seed(stage="candidate"):
    return dict(schema_version=1, stage=stage, apps={
        "mywebsite": dict(release_sha="a" * 40, release_at=None, origin="https://xiaob.dev", paths=["/", "/statistics/"], characters=[]),
        "project_snow": dict(release_sha="b" * 40, release_at=None, origin="https://snow.xiaob.dev", paths=["/"], characters=["fixture_character"]),
    })


def environment():
    return ("SNOW_ALLOWED_PATHS=/statistics/,/\nSNOW_ALLOWED_CHARACTERS=fixture_character\n"
            "SNOW_ORIGINS=https://snow.xiaob.dev,https://xiaob.dev\nSNOW_READER_TOKEN=fixture-secret-never-copy\n")


def test_version_identity_is_order_independent_and_candidates_never_become_published(tmp_path):
    value = make_snapshot(seed(), now=NOW)
    changed = seed()
    changed["apps"]["mywebsite"]["paths"].reverse()
    repeated = make_snapshot(changed, now=NOW + timedelta(hours=1))
    assert repeated["snapshot_id"] == value["snapshot_id"]
    first = save_snapshot(tmp_path, value)
    assert save_snapshot(tmp_path, repeated) == first
    assert json.loads(first.read_bytes())["recorded_at"] == NOW.isoformat()
    released = seed("published")
    with pytest.raises(ValueError, match="timestamp"):
        make_snapshot(released, now=NOW)
    for app in released["apps"].values():
        app["release_at"] = "2026-09-13T12:00:00Z"
    published = make_snapshot(released, now=NOW)
    second = save_snapshot(tmp_path, published)
    assert first.parent.name == "candidate" and second.parent.name == "published"
    assert first.exists() and second.exists() and published["snapshot_id"] != value["snapshot_id"]
    assert published["apps"]["mywebsite"]["content_sha256"] == value["apps"]["mywebsite"]["content_sha256"]
    tampered = copy.deepcopy(published)
    tampered["apps"]["mywebsite"]["paths"].append("/not-in-commit/")
    with pytest.raises(ValueError, match="checksum"):
        validate_snapshot(tampered)


@pytest.mark.parametrize("path", ["/chat?token=private", "/#request", "//private-host/", "/a/../private", "/%2e%2e/private", "http://private/", "/user@example.com"])
def test_private_or_noncanonical_path_is_rejected(path):
    value = seed()
    value["apps"]["mywebsite"]["paths"] = [path]
    with pytest.raises(ValueError, match="public paths"):
        make_snapshot(value, now=NOW)


def test_unknown_fields_and_false_release_claims_are_rejected():
    changed = seed()
    changed["apps"]["project_snow"]["private_context"] = "not accepted"
    with pytest.raises(ValueError, match="Unexpected fields"):
        make_snapshot(changed, now=NOW)
    changed = seed()
    changed["apps"]["mywebsite"]["release_at"] = NOW.isoformat()
    with pytest.raises(ValueError, match="Candidate"):
        make_snapshot(changed, now=NOW)
    changed["stage"] = "published"
    changed["apps"]["mywebsite"]["release_at"] = (NOW + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="future"):
        make_snapshot(changed, now=NOW)


def test_config_comparison_is_exact_and_cannot_echo_credentials():
    snapshot = make_snapshot(seed(), now=NOW)
    parsed = collector_allowlist_from_env(environment())
    receipt = compare_collector(snapshot, parsed)
    assert receipt["exact_match"]
    assert "fixture-secret" not in json.dumps(parsed) + json.dumps(receipt)
    parsed["SNOW_ALLOWED_PATHS"] = ["/", "/extra/"]
    assert compare_collector(snapshot, parsed)["differences"] == {
        "SNOW_ALLOWED_PATHS": {"missing": ["/statistics/"], "extra": ["/extra/"]}}
    with pytest.raises(ValueError, match="Repeated"):
        collector_allowlist_from_env(environment() + "SNOW_ALLOWED_PATHS=/\n")
    with pytest.raises(ValueError, match="literal"):
        collector_allowlist_from_env(environment().replace("fixture_character", "$PRIVATE_TOKEN"))
    with pytest.raises(ValueError, match="all three"):
        collector_allowlist_from_env("SNOW_ALLOWED_PATHS=/\n")


def test_capture_reads_named_commits_and_only_exports_enabled_public_ids(tmp_path):
    repositories, shas = {}, {}
    for app, files in SOURCE_FILES.items():
        repository = tmp_path / app
        repository.mkdir()
        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        repositories[app] = repository
        if app == "mywebsite":
            contents = ['export default Object.freeze(["/", "/statistics/"]);']
        else:
            contents = ['export default Object.freeze({paths:["/"], characters:["fixture_character"]});',
                        json.dumps({"characters": [
                            {"character_id": "fixture_character", "selector_enabled": True, "private_example": "never copy"},
                            {"character_id": "hidden_fixture", "selector_enabled": False}]})]
        for name, content in zip(files, contents, strict=True):
            target = repository / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        subprocess.run(["git", "-C", str(repository), "add", "."], check=True)
        subprocess.run(["git", "-C", str(repository), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "-c", "commit.gpgsign=false", "commit", "-qm", "synthetic public catalog fixture"], check=True)
        shas[app] = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "HEAD"], text=True).strip()
        (repository / files[0]).write_text("Dirty working changes must never be read", encoding="utf-8")
    snapshot = capture_releases(repositories, shas, stage="candidate", now=NOW)
    assert snapshot["apps"]["project_snow"]["characters"] == ["fixture_character"]
    assert snapshot["apps"]["mywebsite"]["paths"] == ["/", "/statistics/"]
    assert "never copy" not in json.dumps(snapshot) and "hidden_fixture" not in json.dumps(snapshot)
    assert compare_collector(snapshot, collector_allowlist_from_env(environment()))["exact_match"]
