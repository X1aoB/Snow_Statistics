"""Small explicit synthetic fixtures for the real engine branch; never users."""
import copy
import re
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

from .contracts import Event
from .lifecycle import timestamp
from .model import daily_metrics, deduplicate
from .real_epoch import verify_manifest


def test_scope(manifest):
    verify_manifest(manifest)
    if manifest["mode"] != "synthetic_engine_test" or manifest["input_origin"] != "synthetic fixtures":
        raise ValueError("Engine acceptance requires an explicit synthetic_engine_test epoch")
    lane = manifest["event_lane"]
    if not re.fullmatch(r"fixture_[a-z0-9_]{1,16}", lane):
        raise ValueError("Use a short new fixture-* epoch for engine acceptance")
    return dict(lane=lane, database="snow_real_" + lane, user="snow_real_" + lane,
                role="snow_real_" + lane + "_role",
                topics={name: "snow.real." + lane + "." + name + ".v1" for name in ("events", "late", "duplicates", "quarantine")})


test_scope.__test__ = False


def events(lane, now, count=20):
    if not 10 <= count <= 100 or not re.fullmatch(r"fixture_[a-z0-9_]{1,16}", lane):
        raise ValueError("Only 10-100 explicit fixture events are allowed")
    result = []
    base = now - timedelta(minutes=1)
    def identifier(value):
        return str(uuid5(NAMESPACE_URL, "snow-real-engine-fixture/" + lane + "/" + value))
    for index in range(count):
        wave, kind = divmod(index, 4)
        event = dict(schema_version=1, event_id=identifier(f"event/{index}"),
                     occurred_at=(base + timedelta(milliseconds=index * 10)).isoformat())
        visitor = identifier("web/" + str(wave % 3)) if kind == 0 else identifier("snow/" + str(wave % 2))
        if kind in (0, 1):
            event.update(app="mywebsite" if kind == 0 else "project_snow", event_type="page_view", path="/", anonymous_id=visitor)
        elif kind == 2:
            event.update(app="project_snow", event_type="request_observed", request_id=f"fixture_req_{wave}", anonymous_id=visitor)
        else:
            event.update(app="project_snow", event_type="request_complete", request_id=f"fixture_req_{wave}",
                         character_id="sample_character", success=wave % 2 == 0, elapsed_ms=50 + wave)
        Event.model_validate(event)
        result.append(event)
    return result


def diagnostics(rows, now):
    """Negative/late Kafka inputs are separate from the on-time HTTP metric scope."""
    request = copy.deepcopy(next(row for row in rows if row["event"]["event_type"] == "request_complete"))
    conflict = copy.deepcopy(rows[0])
    late = copy.deepcopy(rows[0])
    invalid = copy.deepcopy(rows[0])
    first = max(row["seq"] for row in rows) + 1
    for index, row in enumerate((request, conflict, late, invalid), first):
        row["seq"] = index
        row["accepted_at"] = now.isoformat()
    request["event"]["event_id"] = str(uuid5(NAMESPACE_URL, "request-duplicate/" + str(request["event"]["event_id"])))
    request["event"]["success"] = not request["event"]["success"]
    conflict["event"]["path"] = "/statistics/"
    late["event"]["event_id"] = str(uuid5(NAMESPACE_URL, "late/" + str(late["event"]["event_id"])))
    late["event"]["occurred_at"] = (now - timedelta(minutes=30)).isoformat()
    invalid["source"] = "synthetic"  # Explicit negative provenance test, not an on-time event.
    return dict(request_duplicate=request, conflict=conflict, late=late, wrong_source=invalid)


def oracle(rows):
    if any(row["source"] != "real" for row in rows):
        raise ValueError("On-time fixture oracle requires the unambiguous real source branch")
    valid, quality, rejected = deduplicate(rows)
    if rejected or quality["duplicates"]:
        raise ValueError("On-time oracle input must be the collector's unique accepted records")
    return daily_metrics(valid)


def java_environment(manifest, scope, identity, password, host):
    if scope != test_scope(manifest) or set(identity["topic_ids"]) != {scope["topics"]["events"]}:
        raise ValueError("Job ownership and topic identity differ")
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,100}", password):
        raise ValueError("Use a generated private fixture credential")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", identity["cluster_id"]) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,100}", identity["topic_ids"][scope["topics"]["events"]]):
        raise ValueError("Unsafe Kafka identity metadata")
    return dict(KAFKA_BOOTSTRAP=host + ":9092", SNOW_SOURCE="real", SNOW_REPLAY_LANE=scope["lane"],
                SNOW_INPUT_TOPIC=scope["topics"]["events"], DORIS_FE=host + ":8030",
                DORIS_TABLE=scope["database"] + ".events_realtime", DORIS_USER=scope["user"], DORIS_PASSWORD=password,
                SNOW_REAL_READABLE_FROM=timestamp(manifest["original_min_accepted_at"]).isoformat(),
                SNOW_REAL_RESTORE_NOT_AFTER=manifest["expires_at"], SNOW_REAL_START_OFFSET="0",
                SNOW_REAL_CLUSTER_ID=identity["cluster_id"], SNOW_REAL_TOPIC_ID=identity["topic_ids"][scope["topics"]["events"]])


def schema_statements(text, database):
    if not re.fullmatch(r"snow_real_fixture_[a-z0-9_]{1,16}", database):
        raise ValueError("Only a new synthetic-engine database may be bootstrapped")
    text = text.replace("snow.", database + ".").replace("EXISTS snow;", "EXISTS " + database + ";")
    return [statement for statement in text.split(";") if statement.strip()]


def account_statements(scope, account):
    """Host and password are driver parameters, avoiding literal-percent formatting."""
    database, role, user = scope["database"], scope["role"], scope["user"]
    if (not re.fullmatch(r"snow_real_fixture_[a-z0-9_]{1,16}", database)
            or user != database or role != database + "_role" or account["user"] != user):
        raise ValueError("Fixture account escaped its isolated namespace")
    return [(f"CREATE ROLE `{role}`", ()),
            (f"GRANT SELECT_PRIV,LOAD_PRIV ON {database}.* TO ROLE '{role}'", ()),
            (f"CREATE USER '{user}'@%s IDENTIFIED BY %s DEFAULT ROLE '{role}'", ("%", account["password"]))]


def be_integer_settings(text):
    """Only this frozen small profile's integer options enter the API readback."""
    values = {}
    for line in text.splitlines():
        match = re.fullmatch(r"\s*([a-z][a-z0-9_]*)\s*=\s*(-?[0-9]+)\s*", line)
        if match:
            key, value = match.groups()
            if key in values:
                raise ValueError("Ambiguous duplicate BE option")
            values[key] = int(value)
    mandatory = {"brpc_heavy_work_pool_threads", "brpc_light_work_pool_threads", "brpc_arrow_flight_work_pool_threads"}
    if any(values.get(key) != 4 for key in mandatory) or values.get("arrow_flight_sql_port") != -1:
        raise ValueError("All three eager BRPC work pools must have explicit small bounds")
    if any(value < 1 for key, value in values.items() if key != "arrow_flight_sql_port"):
        raise ValueError("Thread counts must be explicit positive values")
    return values


def verify_be_setting(name, expected, rows):
    if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], list)
            or len(rows[0]) < 3 or rows[0][0] != name or type(rows[0][2]) is not str
            or rows[0][2] != str(expected)):
        raise ValueError("Actual BE configuration differs from the frozen small profile")
    return dict(value=expected, type=rows[0][1], actual_api_read=True)


def checkpoint_path(value, job, identifier):
    """Normalize only Flink's two local URI spellings, never another epoch path."""
    if not re.fullmatch(r"[a-f0-9]{32}", job) or type(identifier) is not int or identifier < 1:
        raise ValueError("Invalid fixture checkpoint identity")
    expected = f"/checkpoints/{job}/chk-{identifier}"
    if value not in {"file:" + expected, "file://" + expected}:
        raise ValueError("Checkpoint is outside the exact job and checkpoint directory")
    return "file:" + expected


def retained_checkpoint(value, job):
    if (value.get("status") != "COMPLETED" or value.get("discarded") is not False
            or value.get("is_savepoint") is not False or type(value.get("num_subtasks")) is not int
            or value["num_subtasks"] < 1 or type(value.get("num_acknowledged_subtasks")) is not int
            or value.get("num_acknowledged_subtasks") != value["num_subtasks"]):
        raise ValueError("A fully acknowledged retained checkpoint is required")
    return checkpoint_path(value.get("external_path"), job, value.get("id"))


def restored_checkpoint(value, previous, previous_job):
    expected = retained_checkpoint(previous, previous_job)
    restored = value.get("latest", {}).get("restored", {})
    if (type(value.get("counts", {}).get("restored")) is not int or value["counts"]["restored"] < 1
            or restored.get("id") != previous["id"] or type(restored.get("is_savepoint")) is not bool
            or checkpoint_path(restored.get("external_path"), previous_job, restored.get("id")) != expected):
        raise ValueError("Flink did not restore the exact retained checkpoint")
    # Flink 1.20 reports is_savepoint=true when the retained checkpoint is supplied
    # through --fromSavepoint. Preserve that observation rather than relabel it.
    return restored
