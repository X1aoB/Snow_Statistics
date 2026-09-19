"""Analysis-owned Iceberg admission; aggregate copies never carry the registry.

The trusted Windows coordinator uses pinned-host SSH to invoke actual node
operations. JSON files are bounded transport artifacts, not backend adapters or
user approvals. The analysis authority is never initialized on another node.
"""
import json
import re
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .io import atomic_write, digest, write_json
from .lifecycle import RealLifecycle, timestamp
from .publication import canonical, publication_lock
from .real_lab import lifecycle_phase, validate_config
from .real_lake import LAKE_COLUMNS, prepare_bundle, validate_receipt
from .real_remote_lifecycle import RealRemoteLifecycle
from .real_transfer import MAX_BYTES, validate_payload
from .real_transfer import paths as release_paths
from .real_transfer import read as transfer_read

NAMESPACE = "runtime/real/lake-authority"
DATA_FILES = ("package.json", "package.json.tmp", "input.json", "input.json.tmp", "receipt.json", "receipt.tmp",
              "verify.json", "verify.json.tmp", "verify.tmp", "verified-engine.json", "verified-engine.tmp",
              "confirmed-proof.json", "confirmed-proof.json.tmp", "spark.log", "verify.log", "driver.cid")
DESCRIPTOR_FIELDS = {"schema_version", "source", "kind", "lane", "run_id", "attempt", "input_origin", "config_sha256",
                     "authority", "owner_sha256", "collector", "registry_sha256", "cleanup_sha256", "scope",
                     "pair_manifest", "pair_sha256", "package_sha256", "package_bytes", "input_sha256",
                     "original_at", "expires_at", "issued_at", "read_until", "engine"}
ENGINE_FILES = ("warehouse/spark/real_iceberg.py", "warehouse/spark/real_iceberg_verify.py", "tools/real_lake_spark.sh",
                "lab/spark-submit-locked.sh", "lab/locks/spark-jars.sha256", "src/snow_statistics/real_lake_stage.py",
                "tools/real_lake_stage.py")
JAR = "iceberg-spark-runtime-3.5_2.12-1.10.0.jar"
SPARK_IMAGE = "apache/spark@sha256:936ff39fd63e2bb5ed064f0fbe1518198473f1cdbfa2f863d087a9a8e58116ba"
RECEIPT_FIELDS = {"schema_version", "source", "run_id", "input_sha256", "warehouse", "expires_at", "engine", "master",
                  "application_id", "hive_registration", "column_lineage", "tables"}
TABLE_FIELDS = {"name", "rows", "original_snapshot", "current_snapshot", "exact_readback_equal",
                "historical_readback_equal", "location", "additive_column"}


def read(path, limit=MAX_BYTES):
    info = Path(path).stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("Lake artifacts must be unlinked regular files")
    return transfer_read(path, limit)


def location(root, config, run_id, attempt):
    validate_config(config)
    release_paths(config["lane"], run_id)
    if config["transport_node"] != "snow-analysis" or not re.fullmatch(r"[A-Za-z0-9_-]{1,60}", attempt or ""):
        raise ValueError("Lake authority requires analysis transport and an explicit bounded attempt")
    result = Path(root).absolute() / NAMESPACE / config["lane"] / run_id / attempt
    if result.resolve() != result:
        raise ValueError("Lake attempt cannot traverse a link")
    return result


def manager(root, config):
    return RealRemoteLifecycle(Path(root) / "runtime/real/lifecycle" / config["lane"])


def engine_identity(root, *, verify_jar=False):
    root = Path(root)
    images = (root / "lab/locks/images.env").read_text().splitlines()
    if [line for line in images if line.startswith("SPARK_IMAGE=")] != ["SPARK_IMAGE=" + SPARK_IMAGE]:
        raise ValueError("The lake driver must use the existing pinned Spark image")
    jar = json.loads(read(root / "lab/locks/jars.json"))[JAR]
    if verify_jar:
        path = root / "runtime/jars" / JAR
        if path.stat().st_size != jar["bytes"] or digest(path.read_bytes()) != jar["sha256"]:
            raise ValueError("The existing pinned Iceberg runtime differs")
    return dict(spark_image=SPARK_IMAGE, jar=JAR, jar_sha256=jar["sha256"], jar_bytes=jar["bytes"],
                files={name: digest(read(root / name)) for name in ENGINE_FILES})


def _immutable(path, payload):
    if path.exists():
        if read(path) != payload:
            raise ValueError("Immutable lake attempt already contains different bytes")
    else:
        atomic_write(path, payload)


def validate_descriptor(value, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    release_paths(config["lane"], run_id)
    if (not isinstance(value, dict) or set(value) != DESCRIPTOR_FIELDS or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["source"] != "real" or value["kind"] != "analysis_iceberg_admission"
            or value["authority"] != "snow-analysis" or value["lane"] != config["lane"]
            or value["run_id"] != run_id or value["attempt"] != attempt or value["input_origin"] != config["input_origin"]
            or value["config_sha256"] != digest(canonical(config))
            or type(value["package_bytes"]) is not int or not 1 <= value["package_bytes"] <= MAX_BYTES):
        raise ValueError("Lake admission identity or shape differs")
    for field in ("owner_sha256", "registry_sha256", "cleanup_sha256", "pair_sha256", "package_sha256", "input_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", value[field]):
            raise ValueError("Lake admission digest is invalid")
    from .real_transfer import validate_metadata
    manifest = validate_metadata(value["pair_manifest"], config, run_id, now=current)
    expected = f"hdfs://{config['nodes']['snow-control']}:9000/snow/warehouse/real/{config['lane']}/iceberg/{run_id}/{attempt}"
    if (value["collector"] != manifest["collector"] or value["scope"] != expected
            or value["original_at"] != manifest["original_at"] or value["expires_at"] != manifest["expires_at"]
            or not timestamp(value["issued_at"]) <= current < timestamp(value["read_until"])
            or timestamp(value["read_until"]) > min(timestamp(value["issued_at"]) + timedelta(minutes=15), timestamp(value["expires_at"]))):
        raise ValueError("Lake admission escaped its source, scope or original deadline")
    return value


def validate_package(payload, descriptor, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    validate_descriptor(descriptor, config, run_id, attempt, now=current)
    if len(payload) != descriptor["package_bytes"] or digest(payload) != descriptor["package_sha256"]:
        raise ValueError("Lake package checksum/size differs")
    value = json.loads(payload)
    if not isinstance(value, dict) or set(value) != {"release", "bundle"}:
        raise ValueError("Only the aggregate pair and its exact bundle may be copied")
    release = validate_payload(canonical(value["release"]), descriptor["pair_manifest"], config, run_id, now=current)
    bundle = prepare_bundle(release["daily"], release["behavior"], attempt, descriptor["scope"], now=current)
    if (value["bundle"] != bundle or digest(canonical(bundle)) != descriptor["input_sha256"]
            or bundle["validated_pair_sha256"] != descriptor["pair_sha256"]
            or timestamp(bundle["original_at"]) != timestamp(descriptor["original_at"])
            or timestamp(bundle["expires_at"]) != timestamp(descriptor["expires_at"])):
        raise ValueError("Lake bundle differs from the verified pair or lifetime")
    return value


def reserve(root, config, run_id, attempt, descriptor, *, now=None):
    current = now or datetime.now(UTC)
    directory = location(root, config, run_id, attempt)
    local = RealLifecycle(directory / "data")
    if local.owner.exists():
        local.cleanup(current)
    validate_descriptor(descriptor, config, run_id, attempt, now=current)
    with publication_lock(directory):
        _immutable(directory / "descriptor.json", canonical(descriptor))
        if not local.owner.exists():
            local.initialize()
        for name in DATA_FILES:
            local.register(name, "aggregate", descriptor["original_at"], now=current)
        local.cleanup(current)
    return local


def accept(root, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    directory = location(root, config, run_id, attempt)
    local = RealLifecycle(directory / "data")
    local.cleanup(current)
    descriptor = json.loads(read(directory / "descriptor.json", 65536))
    payload = read(local.readable("package.json.tmp", current))
    value = validate_package(payload, descriptor, config, run_id, attempt, now=current)
    _immutable(local.path("package.json"), payload)
    _immutable(local.path("input.json"), canonical(value["bundle"]))
    local.path("package.json.tmp").unlink(missing_ok=True)
    return descriptor


def read_attempt(root, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    directory = location(root, config, run_id, attempt)
    local = RealLifecycle(directory / "data")
    local.cleanup(current)
    descriptor = json.loads(read(directory / "descriptor.json", 65536))
    value = validate_package(read(local.readable("package.json", current)), descriptor, config, run_id, attempt, now=current)
    if read(local.readable("input.json", current)) != canonical(value["bundle"]):
        raise ValueError("Lake execution input changed")
    return descriptor, value, local


def prepare(root, config, run_id, attempt, *, now=None):
    """Only analysis calls this; it invokes real cleanup, never accepts a receipt argument."""
    current = now or datetime.now(UTC)
    directory = location(root, config, run_id, attempt)
    remote = manager(root, config)
    relative = release_paths(config["lane"], run_id)
    copied = RealLifecycle(Path(root) / relative["directory"] / "data")
    copied.cleanup(current)
    manifest = json.loads(read(Path(root) / relative["manifest"], 65536))
    release = validate_payload(read(copied.readable("pair.json", current)), manifest, config, run_id, now=current)
    if (directory / "descriptor.json").exists():
        descriptor = validate_descriptor(json.loads(read(directory / "descriptor.json", 65536)), config, run_id, attempt, now=current)
        check_authority(remote, descriptor, current)
        recorded = read(directory / "issued-cleanup.json") if (directory / "issued-cleanup.json").exists() else read(remote.directory / "last-cleanup.json")
        if digest(recorded) != descriptor["cleanup_sha256"]:
            raise ValueError("An interrupted preparation cannot replace the issued authority cleanup")
        bundle = prepare_bundle(release["daily"], release["behavior"], attempt, descriptor["scope"], now=current)
        payload = canonical(dict(release=release, bundle=bundle))
        validate_package(payload, descriptor, config, run_id, attempt, now=current)
        local = reserve(root, config, run_id, attempt, descriptor, now=current)
        _immutable(directory / "issued-cleanup.json", recorded)
        _immutable(local.path("package.json"), payload)
        _immutable(local.path("input.json"), canonical(bundle))
        return descriptor
    owner, _ = remote._read()
    collector = manifest["collector"]
    if any(owner[key] != collector[key] for key in ("instance_id", "generation")):
        raise ValueError("Lake pair differs from the authority collector generation")
    expected_root = f"hdfs://{config['nodes']['snow-control']}:9000/snow/warehouse/real/{config['lane']}"
    if owner["roots"]["warehouse"] != expected_root:
        raise ValueError("Lake authority root differs from the configured lane")
    scope = expected_root + "/iceberg/" + run_id + "/" + attempt
    bundle = prepare_bundle(release["daily"], release["behavior"], attempt, scope, now=current)
    payload = canonical(dict(release=release, bundle=bundle))
    if len(payload) > MAX_BYTES:
        raise ValueError("Aggregate lake package exceeds the bounded transfer limit")
    remote.register(scope, "aggregate", bundle["original_at"], now=current)
    # This executes the authority's actual HDFS/StoppedStorage/backend checks.
    # No callback or --cleanup-json can supply a fabricated successful receipt.
    cleanup = lifecycle_phase(config, Path(root), "cleanup", None)
    owner, registry = remote._read()
    recorded = read(remote.directory / "last-cleanup.json")
    if (json.loads(recorded) != cleanup or cleanup.get("source") != "real"
            or cleanup.get("registry_sha256") != digest(canonical(registry))
            or cleanup.get("owner") != {key: owner[key] for key in ("instance_id", "generation")}
            or not current - timedelta(seconds=5) <= timestamp(cleanup["checked_at"]) <= datetime.now(UTC) + timedelta(seconds=5)):
        raise ValueError("Actual authority cleanup did not verify this exact registry")
    issued = max(current, timestamp(cleanup["checked_at"]))
    until = min(issued + timedelta(minutes=15), timestamp(bundle["expires_at"]))
    if cleanup.get("next_expiry"):
        until = min(until, timestamp(cleanup["next_expiry"]))
    descriptor = dict(schema_version=1, source="real", kind="analysis_iceberg_admission", lane=config["lane"],
                      run_id=run_id, attempt=attempt, input_origin=config["input_origin"], config_sha256=digest(canonical(config)),
                      authority="snow-analysis", owner_sha256=digest(canonical(owner)), collector=collector,
                      registry_sha256=digest(canonical(registry)), cleanup_sha256=digest(recorded), scope=scope,
                      pair_manifest=manifest, pair_sha256=bundle["validated_pair_sha256"], package_sha256=digest(payload),
                      package_bytes=len(payload), input_sha256=digest(canonical(bundle)), original_at=manifest["original_at"],
                      expires_at=manifest["expires_at"], issued_at=issued.isoformat(), read_until=until.isoformat(),
                      engine=engine_identity(root))
    check_authority(remote, descriptor, issued)
    local = reserve(root, config, run_id, attempt, descriptor, now=issued)
    _immutable(directory / "issued-cleanup.json", recorded)
    _immutable(local.path("package.json"), payload)
    _immutable(local.path("input.json"), canonical(bundle))
    return descriptor


def check_authority(remote, descriptor, now):
    owner, registry = remote._read()
    scope = registry["artifacts"].get(descriptor["scope"])
    expected = dict(kind="aggregate", original_min_accepted_at=timestamp(descriptor["original_at"]).isoformat(),
                    expires_at=timestamp(descriptor["expires_at"]).isoformat(), copied_from=None, references=[])
    if (remote.journal.exists() or digest(canonical(owner)) != descriptor["owner_sha256"]
            or digest(canonical(registry)) != descriptor["registry_sha256"] or scope != expected
            or not timestamp(descriptor["issued_at"]) <= now < timestamp(descriptor["read_until"])):
        raise ValueError("Authority ownership, registered scope, cleanup or admission deadline changed")
    return owner


def checked_engine_receipt(receipt, bundle, *, now=None):
    if (not isinstance(receipt, dict) or set(receipt) != RECEIPT_FIELDS
            or type(receipt["schema_version"]) is not int or type(receipt["tables"]) is not dict):
        raise ValueError("Only exact engine receipt metadata may return")
    if any(type(table) is not dict or set(table) != TABLE_FIELDS or table["additive_column"] != "model_revision"
           or type(table["rows"]) is not int or table["rows"] < 0
           or any(table[name] is not None and type(table[name]) is not int for name in ("original_snapshot", "current_snapshot"))
           for table in receipt["tables"].values()):
        raise ValueError("Engine table receipt contains unexpected fields")
    return validate_receipt(receipt, bundle, now=now)


def confirm(root, config, run_id, attempt, evidence_sha256, *, now=None):
    """Called by the coordinator only after its actual SSH verification process succeeds."""
    current = now or datetime.now(UTC)
    descriptor, value, local = read_attempt(root, config, run_id, attempt, now=current)
    remote = manager(root, config)
    check_authority(remote, descriptor, current)
    issued = read(location(root, config, run_id, attempt) / "issued-cleanup.json")
    if digest(issued) != descriptor["cleanup_sha256"]:
        raise ValueError("Original issued authority cleanup changed")
    body = read(local.readable("verify.json", current), 65536)
    if not re.fullmatch(r"[a-f0-9]{64}", evidence_sha256) or digest(body) != evidence_sha256:
        raise ValueError("Verification differs from the coordinator's actual SSH readback")
    proof = json.loads(body)
    expected = {"schema_version", "source", "kind", "descriptor_sha256", "input_sha256", "receipt_sha256",
                "execution", "verification", "verified_at"}
    if (set(proof) != expected or type(proof["schema_version"]) is not int or proof["schema_version"] != 1 or proof["source"] != "real"
            or proof["kind"] != "iceberg_actual_readback" or proof["descriptor_sha256"] != digest(canonical(descriptor))
            or proof["input_sha256"] != descriptor["input_sha256"]
            or not timestamp(descriptor["issued_at"]) <= timestamp(proof["verified_at"]) <= current
            or current - timestamp(proof["verified_at"]) > timedelta(minutes=5)):
        raise ValueError("Fresh actual lake verification is not bound to the issued admission")
    checked_engine_receipt(proof["execution"], value["bundle"], now=current)
    checked_engine_receipt(proof["verification"], value["bundle"], now=current)
    if digest(canonical(proof["execution"])) != proof["receipt_sha256"]:
        raise ValueError("Execution receipt hash differs")
    if proof["verification"]["application_id"] == proof["execution"]["application_id"]:
        raise ValueError("Verification must execute its own actual read-only Spark application")
    for group in LAKE_COLUMNS:
        if proof["execution"]["tables"][group] != proof["verification"]["tables"][group]:
            raise ValueError("Current or original Iceberg snapshot changed after execution")
    fresh = lifecycle_phase(config, Path(root), "cleanup", None)
    if (json.loads(read(remote.directory / "last-cleanup.json")) != fresh
            or fresh.get("registry_sha256") != descriptor["registry_sha256"]
            or fresh.get("owner") != {key: descriptor["collector"][key] for key in ("instance_id", "generation")}):
        raise ValueError("Fresh authority cleanup no longer verifies the issued lake scope")
    completed = now or datetime.now(UTC)
    check_authority(remote, descriptor, completed)
    if (not current <= timestamp(fresh["checked_at"]) <= completed
            or fresh.get("next_expiry") and timestamp(fresh["next_expiry"]) <= completed):
        raise ValueError("Fresh cleanup completed outside the allowed live read window")
    result = dict(schema_version=1, source="real", input_origin=config["input_origin"], confirmed=True,
                  descriptor_sha256=digest(canonical(descriptor)), evidence_sha256=evidence_sha256,
                  owner_sha256=descriptor["owner_sha256"], registry_sha256=descriptor["registry_sha256"],
                  pair_sha256=descriptor["pair_sha256"], scope=descriptor["scope"], expires_at=descriptor["expires_at"],
                  confirmed_at=completed.isoformat(), engine="Spark 3.5.7", hive_registration=False, column_lineage=False)
    target = location(root, config, run_id, attempt) / "confirmed.json"
    if target.exists():
        previous = json.loads(read(target, 65536))
        if (any(previous[key] != result[key] for key in result if key not in {"confirmed_at", "evidence_sha256"})
                or digest(read(local.readable("confirmed-proof.json", completed), 65536)) != previous["evidence_sha256"]):
            raise ValueError("Confirmed lake attempt cannot replace its evidence")
        # A new successful live verification may have a different Spark app ID.
        # Preserve the original confirmation and its immutable proof on replay.
        return previous
    _immutable(local.path("confirmed-proof.json"), body)
    write_json(target, result)
    return result


def cleanup_copies(root, *, now=None):
    """Bounded inventory of this new namespace, including failed partial uploads."""
    current = now or datetime.now(UTC)
    base = Path(root).absolute() / NAMESPACE
    if not base.exists():
        return 0
    if base.resolve() != base:
        raise ValueError("Lake transfer root is linked")
    removed, count = 0, 0
    pending = [(base, 0)]
    patterns = (r"[a-z][a-z0-9-]{2,23}", r"[A-Za-z0-9_-]{1,100}", r"[A-Za-z0-9_-]{1,60}")
    metadata = {"publisher.lock", "descriptor.json", "incoming-descriptor.json", "issued-cleanup.json", "execution.json", "confirmed.json",
                "node-worker.json", "cancelled.json"}
    metadata |= {prefix + node + ".json" for prefix in ("stage-", "stage-result-")
                 for node in ("snow-control", "snow-compute", "snow-analysis")}
    metadata |= {name + ".tmp" for name in metadata if name.endswith(".json")}
    while pending:
        parent, depth = pending.pop()
        for child in parent.iterdir():
            count += 1
            if count > 10000 or child.resolve() != child.absolute():
                raise ValueError("Lake inventory is linked or exceeds its bound")
            if depth < 3:
                if not child.is_dir() or not re.fullmatch(patterns[depth], child.name):
                    raise ValueError("Unregistered lake namespace directory")
                pending.append((child, depth + 1))
            elif child.name == "data":
                local = RealLifecycle(child)
                if not child.is_dir() or not local.owner.exists():
                    raise ValueError("Unregistered lake copy has no local owner")
                removed += local.cleanup(current)["removed"]
            elif child.name == "worker-admission" and child.is_dir():
                if any(item.name != "publisher.lock" or not item.is_file() or item.resolve() != item.absolute()
                       for item in child.iterdir()):
                    raise ValueError("Unexpected worker admission metadata")
            elif child.name not in metadata or not child.is_file() or child.stat().st_size > 65536:
                raise ValueError("Unregistered lake attempt metadata")
    return removed
