"""Versioned public dimensions, copied at an explicit product release boundary.

This is an operator-side tool. Business builds never import this package. Git
capture reads named commits rather than dirty working files; a published stage
is an operator declaration and does not substitute for a live release receipt.
"""
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .io import digest, write_json
from .publication import canonical, publication_lock

ORIGINS = {"mywebsite": "https://xiaob.dev", "project_snow": "https://snow.xiaob.dev"}
ENV_KEYS = {"SNOW_ALLOWED_PATHS", "SNOW_ALLOWED_CHARACTERS", "SNOW_ORIGINS"}
SOURCE_FILES = {
    "mywebsite": ("public/statistics/paths.mjs",),
    "project_snow": ("App/public_frontend/statistics/config.mjs", "App/backend/snow_app/mvp_character_registry.json"),
}


def _time(value):
    if not isinstance(value, str):
        raise ValueError("An explicit release timestamp is required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp needs an explicit timezone")
    return result.astimezone(UTC)


def _list(values, kind):
    if not isinstance(values, list) or len(values) > 1000 or any(not isinstance(v, str) for v in values):
        raise ValueError("Invalid bounded public dimension list")
    if len(values) != len(set(values)):
        raise ValueError("Duplicate public dimension")
    for value in values:
        if kind == "characters":
            okay = re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value)
        else:
            parsed = urlsplit(value)
            okay = (1 <= len(value) <= 200 and re.fullmatch(r"/[A-Za-z0-9_./-]*", value)
                    and not parsed.netloc and not parsed.query and not parsed.fragment and "//" not in value
                    and all(part not in {".", ".."} for part in value.split("/")))
        if not okay:
            raise ValueError("Only exact public paths or selector IDs may enter dimensions")
    return sorted(values)


def make_snapshot(seed, *, now=None):
    current = now or datetime.now(UTC)
    if (set(seed) != {"schema_version", "stage", "apps"} or seed["schema_version"] != 1
            or seed["stage"] not in {"candidate", "published"} or set(seed["apps"]) != set(ORIGINS)):
        raise ValueError("Explicit candidate/published catalogs for both products are required")
    apps = {}
    for app, data in seed["apps"].items():
        if set(data) != {"release_sha", "release_at", "origin", "paths", "characters"}:
            raise ValueError("Unexpected fields in public catalog")
        if not isinstance(data["release_sha"], str) or not re.fullmatch(r"[a-f0-9]{40}", data["release_sha"]):
            raise ValueError("Full product release SHA is required")
        if data["origin"] != ORIGINS[app]:
            raise ValueError("Unexpected public product origin")
        released = data["release_at"]
        if seed["stage"] == "published":
            release_time = _time(released)
            if release_time > current:
                raise ValueError("A future release cannot be marked published")
            released = release_time.isoformat()
        elif released is not None:
            raise ValueError("Candidate metadata must not claim a production release time")
        content = {"origin": data["origin"], "paths": _list(data["paths"], "paths"),
                   "characters": _list(data["characters"], "characters")}
        if not content["paths"] or app == "mywebsite" and content["characters"]:
            raise ValueError("Catalog needs public pages and the website has no selector catalog")
        apps[app] = content | {"release_sha": data["release_sha"], "release_at": released,
                               "content_sha256": digest(canonical(content))}
    body = dict(schema_version=1, kind="public_product_catalog", stage=seed["stage"], apps=apps,
                evidence="explicit_release_declaration", identity_scope="application_local_public_dimensions")
    return body | {"snapshot_id": digest(canonical(body)), "recorded_at": current.isoformat()}


def validate_snapshot(value):
    expected = make_snapshot({"schema_version": 1, "stage": value["stage"], "apps": {
        app: {k: v for k, v in data.items() if k != "content_sha256"} for app, data in value["apps"].items()
    }}, now=_time(value["recorded_at"]))
    if expected != value:
        raise ValueError("Catalog identity or content checksum differs")
    return value


def save_snapshot(directory, value):
    validate_snapshot(value)
    directory = Path(directory)
    # Stage is part of identity. Publishing never overwrites a candidate.
    target = directory / value["stage"] / (value["snapshot_id"] + ".json")
    with publication_lock(directory):
        if target.exists():
            old = validate_snapshot(json.loads(target.read_bytes()))
            if {k: v for k, v in old.items() if k != "recorded_at"} != {k: v for k, v in value.items() if k != "recorded_at"}:
                raise ValueError("Immutable catalog collision")
        else:
            write_json(target, value)
    return target


def collector_allowlist_from_env(text):
    """Extract three public values only; never return credentials or diagnostics."""
    if len(text.encode()) > 65536:
        raise ValueError("Collector environment file is too large")
    selected = {}
    for line in text.splitlines():
        name, separator, value = line.strip().partition("=")
        if not separator or name not in ENV_KEYS:
            continue
        if name in selected:
            raise ValueError("Repeated public allowlist setting")
        value = value.strip()
        if value[:1] in {"\"", "'"}:
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError("Invalid public allowlist quoting")
            value = value[1:-1]
        if any(c in value for c in ("$", "`", "#", "\n", "\r")):
            raise ValueError("Public allowlists must be literal values")
        selected[name] = [v.strip() for v in value.split(",") if v.strip()]
    if set(selected) != ENV_KEYS:
        raise ValueError("Collector config must explicitly set all three public allowlists")
    return selected


def compare_collector(snapshot, selected):
    validate_snapshot(snapshot)
    if set(selected) != ENV_KEYS:
        raise ValueError("Only public collector allowlists are accepted")
    expected = {
        "SNOW_ALLOWED_PATHS": {path for app in snapshot["apps"].values() for path in app["paths"]},
        "SNOW_ALLOWED_CHARACTERS": {item for app in snapshot["apps"].values() for item in app["characters"]},
        "SNOW_ORIGINS": set(ORIGINS.values()),
    }
    differences = {}
    for key, values in selected.items():
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise ValueError("Invalid collector allowlist values")
        # Validate before including a difference in a public-safe receipt.
        if key == "SNOW_ORIGINS":
            if any(item not in ORIGINS.values() for item in values):
                raise ValueError("Unexpected collector origin; inspect config privately")
        else:
            _list(values, "paths" if key == "SNOW_ALLOWED_PATHS" else "characters")
        if set(values) != expected[key]:
            differences[key] = dict(missing=sorted(expected[key] - set(values)), extra=sorted(set(values) - expected[key]))
    return dict(schema_version=1, catalog_snapshot_id=snapshot["snapshot_id"], exact_match=not differences,
                differences=differences, scope="collector_global_union_with_per_application_catalog")


def _git_file(repository, sha, relative):
    if not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("Capture requires full commit IDs")
    result = subprocess.run(["git", "-C", str(repository), "show", sha + ":" + relative],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=15)
    if result.returncode or len(result.stdout) > 262144:
        raise ValueError("Unable to read the bounded public catalog at the named commit")
    return result.stdout.decode("utf-8")


def _array(text, field=None):
    # The current product format uses JSON string arrays, not executable imports.
    pattern = (r"export\s+default\s+Object\.freeze\(\s*(\[[^\]]*\])\s*\)\s*;" if field is None
               else r"\b" + re.escape(field) + r"\s*:\s*(\[[^\]]*\])")
    found = re.findall(pattern, text)
    if len(found) != 1:
        raise ValueError("Expected one literal public dimension array; do not execute product JavaScript")
    return json.loads(found[0])


def capture_releases(repositories, shas, *, stage, release_times=None, now=None):
    """Use only whitelisted committed source files; never copy a full registry."""
    if set(repositories) != set(ORIGINS) or set(shas) != set(ORIGINS):
        raise ValueError("Both product repositories and exact commits are required")
    website = _git_file(repositories["mywebsite"], shas["mywebsite"], SOURCE_FILES["mywebsite"][0])
    snow = [_git_file(repositories["project_snow"], shas["project_snow"], path) for path in SOURCE_FILES["project_snow"]]
    roster = json.loads(snow[1])
    ids = [row["character_id"] for row in roster["characters"] if row.get("selector_enabled") is not False]
    if _list(ids, "characters") != _list(_array(snow[0], "characters"), "characters"):
        raise ValueError("Snow selector registry and adapter allowlist differ at this commit")
    apps = {"mywebsite": {"paths": _array(website), "characters": []},
            "project_snow": {"paths": _array(snow[0], "paths"), "characters": ids}}
    for app, values in apps.items():
        values.update(origin=ORIGINS[app], release_sha=shas[app], release_at=(release_times or {}).get(app))
    return make_snapshot(dict(schema_version=1, stage=stage, apps=apps), now=now)
