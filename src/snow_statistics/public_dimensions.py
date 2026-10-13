"""Gap-aware SCD2 of public catalogues; no event storage or engine side effects.

Release/coverage receipts are operator-attested inputs, not signatures verified
by this module. A published catalogue alone proves neither deployment nor
continuous historical coverage. See docs/public-dimensions.md.
"""
import json
import re
from datetime import UTC, datetime

from .io import digest
from .public_catalog import ORIGINS, _list, validate_snapshot
from .publication import canonical

MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 50000
POLICY = {
    "history": "explicit_half_open_version_coverage_only",
    "current": "explicit_as_of_inside_closed_observation_interval",
    "null_valid_to": "unknown_close_not_infinite_validity",
    "evidence": "operator_attested_hash_references_not_independently_verified",
    "fact_storage": "none",
    "engine_acceptance": "not_measured_by_this_artifact",
}


def _exact(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys.split()):
        raise ValueError("Unexpected public DIM fields")


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("An explicit SHA256 evidence reference is required")
    return value


def _time(value):
    if not isinstance(value, str):
        raise ValueError("An explicit timezone timestamp is required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Naive timestamps are forbidden")
    return result.astimezone(UTC)


def _instant(value):
    return _time(value).isoformat(timespec="microseconds")


def _bounded(values, limit):
    if not isinstance(values, list) or len(values) > limit:
        raise ValueError("Public DIM input exceeds its bounded scope")
    return values


def _id(value):
    return digest(canonical(value))


def build_bundle(spec):
    """Build only from public snapshots plus explicit release/history receipts.

    Coverage statements bind ONE release and must not cross another known
    effective release or conflicting observation. Missing statements stay
    unknown. No interval is inferred merely from successive known versions.
    """
    if len(canonical(spec)) > MAX_BYTES:
        raise ValueError("Public DIM metadata is too large")
    _exact(spec, "schema_version source knowledge_cutoff snapshots attestations coverage")
    if type(spec["schema_version"]) is not int or spec["schema_version"] != 1:
        raise ValueError("Unsupported public DIM schema")
    source = spec["source"]
    if source not in {"real", "synthetic"}:
        raise ValueError("Real and synthetic dimensions require separate sources")
    cutoff = _instant(spec["knowledge_cutoff"])
    snapshots, catalogues, by_sha = {}, {}, {}
    if not spec["snapshots"]:
        raise ValueError("At least one public snapshot is required")
    for item in _bounded(spec["snapshots"], 128):
        snapshot = validate_snapshot(item)
        if snapshot["stage"] != "published" or _instant(snapshot["recorded_at"]) > cutoff:
            raise ValueError("Only already observed published snapshots are admitted")
        sid = snapshot["snapshot_id"]
        if sid in snapshots and snapshots[sid] != snapshot:
            raise ValueError("Conflicting snapshot identity")
        snapshots[sid] = snapshot
        for app, data in snapshot["apps"].items():
            sha_key = (app, data["release_sha"])
            old = by_sha.setdefault(sha_key, data["content_sha256"])
            if old != data["content_sha256"]:
                raise ValueError("Same Git release contains different public catalogues")
            catalogues[(sid, app)] = data
    releases, observations, attested = {}, [], set()
    for item in _bounded(spec["attestations"], 256):
        _exact(item, "snapshot_id app effective_at release_evidence_sha256 observed_at checked_through "
                     "observation_evidence_sha256")
        key = (item["snapshot_id"], item["app"])
        if key not in catalogues:
            raise ValueError("Attestation must reference an admitted snapshot/application")
        data = catalogues[key]
        observed, checked = _instant(item["observed_at"]), _instant(item["checked_through"])
        if observed > checked or checked > cutoff:
            raise ValueError("Observation window exceeds knowledge cutoff")
        effective = None if item["effective_at"] is None else _instant(item["effective_at"])
        if effective is None:
            if item["release_evidence_sha256"] is not None:
                raise ValueError("An unknown release time cannot carry a verified release receipt")
        elif (effective != _instant(data["release_at"]) or effective > observed):
            raise ValueError("Verified business time must match actual declared release, before observation")
        else:
            _hash(item["release_evidence_sha256"])
        _hash(item["observation_evidence_sha256"])
        # Include effective declaration even when unverified; never promote it
        # into valid_from until the release attestation is supplied.
        rid = _id([source, item["app"], data["release_sha"], _instant(data["release_at"])])
        release = dict(source=source, app=item["app"], release_id=rid,
                       release_sha=data["release_sha"], content_sha256=data["content_sha256"],
                       valid_from=effective, valid_to=None,
                       release_evidence_sha256=item["release_evidence_sha256"])
        if rid in releases and releases[rid] != release:
            raise ValueError("Inconsistent verification of the same release")
        releases[rid] = release
        attested.add(key)
        observations.append(dict(source=source, app=item["app"], release_id=rid,
                                 snapshot_id=item["snapshot_id"], observed_at=observed, checked_through=checked,
                                 evidence_sha256=item["observation_evidence_sha256"]))
    if set(catalogues) != attested:
        raise ValueError("Each supplied application catalogue needs an explicit observation attestation")
    observations = sorted({canonical(row): row for row in observations}.values(),
                          key=lambda row: (row["app"], row["observed_at"], row["release_id"]))
    for index, left in enumerate(observations):
        for right in observations[index + 1:]:
            if (left["app"] == right["app"] and left["release_id"] != right["release_id"]
                    and max(left["observed_at"], right["observed_at"])
                    <= min(left["checked_through"], right["checked_through"])):
                raise ValueError("Different releases conflict at the same observed instant")
    known = [r for r in releases.values() if r["valid_from"] is not None]
    if len({(r["app"], r["valid_from"]) for r in known}) != len(known):
        raise ValueError("Conflicting releases at the same effective instant")
    for observation in observations:
        declared = _instant(catalogues[(observation["snapshot_id"], observation["app"])]["release_at"])
        if any(r["app"] == observation["app"] and r["release_id"] != observation["release_id"]
               and declared <= r["valid_from"] <= observation["checked_through"] for r in known):
            raise ValueError("Observation extends across another verified release")
    coverage = []
    for item in _bounded(spec["coverage"], 512):
        _exact(item, "snapshot_id app covered_from covered_to verified_at evidence_sha256")
        key = (item["snapshot_id"], item["app"])
        candidates = [o for o in observations if (o["snapshot_id"], o["app"]) == key]
        if not candidates:
            raise ValueError("Coverage requires an observed catalogue")
        rid = candidates[0]["release_id"]
        release = releases[rid]
        start, end = _instant(item["covered_from"]), _instant(item["covered_to"])
        verified = _instant(item["verified_at"])
        if release["valid_from"] is None or not release["valid_from"] <= start < end <= verified <= cutoff:
            raise ValueError("Coverage needs verified release time and bounded history evidence")
        _hash(item["evidence_sha256"])
        for other in known:
            if other["app"] == release["app"] and other["release_id"] != rid:
                if release["valid_from"] <= other["valid_from"] < end:
                    raise ValueError("Historical coverage crosses another known release")
        for other in observations:
            if (other["app"] == release["app"] and other["release_id"] != rid
                    and start <= other["checked_through"] and other["observed_at"] < end):
                raise ValueError("Historical coverage contradicts an observation")
        coverage.append(dict(source=source, app=item["app"], release_id=rid, covered_from=start,
                             covered_to=end, verified_at=verified, evidence_sha256=item["evidence_sha256"]))
    coverage = sorted({canonical(row): row for row in coverage}.values(),
                      key=lambda row: (row["app"], row["covered_from"], row["release_id"]))
    for index, left in enumerate(coverage):
        for right in coverage[index + 1:]:
            if (left["app"] == right["app"] and left["release_id"] != right["release_id"]
                    and max(left["covered_from"], right["covered_from"])
                    < min(left["covered_to"], right["covered_to"])):
                raise ValueError("Historical version coverage overlaps")
    # A known later release alone does not close the previous known release.
    # Only explicit same-version history reaching that boundary can do so.
    for release in known:
        ends = {c["covered_to"] for c in coverage if c["release_id"] == release["release_id"]}
        successors = [r["valid_from"] for r in known if r["app"] == release["app"]
                      and r["valid_from"] > release["valid_from"] and r["valid_from"] in ends]
        if successors:
            release["valid_to"] = min(successors)
    rows = []
    by_release = {o["release_id"]: catalogues[(o["snapshot_id"], o["app"])] for o in observations}
    for release in sorted(releases.values(), key=lambda r: (r["app"], r["valid_from"] or "", r["release_id"])):
        data = by_release[release["release_id"]]
        previous = [by_release[r["release_id"]] for r in known
                    if r["app"] == release["app"] and release["valid_from"] is not None
                    and r["valid_from"] < release["valid_from"]]
        for kind, field in (("page", "paths"), ("character", "characters")):
            keys = set(data[field]).union(*(set(p[field]) for p in previous))
            for key in sorted(keys):
                rows.append(dict(source=source, app=release["app"], kind=kind, dimension_key=key,
                                 dimension_id=_id([release["release_id"], kind, key]),
                                 release_id=release["release_id"], valid_from=release["valid_from"],
                                 valid_to=release["valid_to"], present=key in data[field]))
                if len(rows) > MAX_ROWS:
                    raise ValueError("Public DIM row budget exceeded")
    normalized = dict(schema_version=1, source=source, knowledge_cutoff=cutoff,
                      snapshots=sorted(snapshots.values(), key=lambda row: row["snapshot_id"]))
    for field in ("attestations", "coverage"):
        values = []
        for original in spec[field]:
            row = dict(original)
            for key in ("effective_at", "observed_at", "checked_through", "covered_from", "covered_to", "verified_at"):
                if row.get(key) is not None:
                    row[key] = _instant(row[key])
            values.append(row)
        normalized[field] = sorted({canonical(row): row for row in values}.values(), key=canonical)
    body = dict(schema_version=1, kind="public_catalog_scd2", source=source, knowledge_cutoff=cutoff,
                policy=dict(POLICY), inputs=normalized,
                releases=sorted(releases.values(), key=lambda r: (r["app"], r["release_id"])),
                observations=observations, coverage=coverage, dimensions=rows)
    if len(canonical(body)) > MAX_BYTES:
        raise ValueError("Public DIM artifact exceeds its byte budget")
    return body | {"artifact_id": _id(body)}


def validate_bundle(value):
    if not isinstance(value, dict) or build_bundle(value.get("inputs")) != value:
        raise ValueError("Public DIM artifact differs from its recomputed inputs")
    return value


class DimensionIndex:
    """In-memory lookups; facts/identifiers are never written by this class."""

    def __init__(self, bundle):
        self.bundle = json.loads(canonical(validate_bundle(bundle)))
        self.releases = {r["release_id"]: r for r in self.bundle["releases"]}
        self.rows = {(r["release_id"], r["kind"], r["dimension_key"]): r for r in self.bundle["dimensions"]}

    def resolve(self, *, source, app, kind, key, at, mode="at_event"):
        if source != self.bundle["source"] or app not in ORIGINS or kind not in {"page", "character"}:
            raise ValueError("Dimension source/application/kind mismatch")
        _list([key], "paths" if kind == "page" else "characters")
        point = _instant(at)
        if mode == "at_event":
            matches = {r["release_id"] for r in self.bundle["coverage"] if r["app"] == app
                       and r["covered_from"] <= point < r["covered_to"]}
        elif mode == "current":
            matches = {r["release_id"] for r in self.bundle["observations"] if r["app"] == app
                       and r["observed_at"] <= point <= r["checked_through"]}
        else:
            raise ValueError("Choose at_event or current explicitly")
        result = dict(source=source, app=app, kind=kind, mode=mode, status="unknown",
                      release_id=None, dimension_id=None)
        if len(matches) != 1:
            return result
        rid = next(iter(matches))
        row = self.rows.get((rid, kind, key))
        return result | dict(release_id=rid, dimension_id=None if row is None else row["dimension_id"],
                             status="present" if row and row["present"] else "not_present")

    def resolve_fact(self, projection, *, current_as_of):
        """Use a transient whitelist projection, NOT an entire event envelope."""
        if (not isinstance(projection, dict)
                or set(projection) - {"source", "app", "occurred_at", "path", "character_id"}
                or not {"source", "app", "occurred_at"} <= set(projection)):
            raise ValueError("Only the minimal transient fact projection is accepted")
        # Validate even an empty projection (e.g. request with no page/role).
        if projection["source"] != self.bundle["source"] or projection["app"] not in ORIGINS:
            raise ValueError("Fact source/application differs")
        _time(projection["occurred_at"])
        _time(current_as_of)
        result = {}
        for field, kind in (("path", "page"), ("character_id", "character")):
            if field in projection and projection[field] is not None:
                args = dict(source=projection["source"], app=projection["app"], kind=kind, key=projection[field])
                result[kind] = dict(at_event=self.resolve(**args, at=projection["occurred_at"]),
                                    current=self.resolve(**args, at=current_as_of, mode="current"))
        return result


def read_spec(path):
    """Read bounded public-only metadata; never load an environment/event file."""
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BYTES:
        raise ValueError("Expected a bounded ordinary public metadata file")
    with path.open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("Public metadata grew beyond its byte budget")
    return json.loads(data)
