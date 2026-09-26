"""Metadata-only real OpenLineage, kept separate from existing synthetic evidence."""
import json
import re
import uuid
from datetime import UTC, datetime
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .io import digest
from .lineage import PRODUCER, SCHEMA, Journal
from .publication import canonical, publication_lock

NAMESPACE = "snow-statistics.real"
JOBS = {"snow_real.compute_daily", "snow_real.compute_behavior", "snow_real.publish", "snow_real.iceberg_aggregates"}


def dataset(uri):
    parsed = urlsplit(uri)
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("Only metadata paths without credentials/query data are allowed")
    if parsed.scheme == "hdfs" and parsed.port == 9000 and re.fullmatch(r"/snow/(?:ods/real|warehouse/real|auxiliary/real)/[A-Za-z0-9_./-]{1,300}", parsed.path) and ".." not in parsed.path:
        return dict(namespace=parsed.scheme + "://" + parsed.netloc, name=parsed.path)
    if parsed.scheme == "file" and parsed.netloc == "snow-control" and re.fullmatch(r"/home/snow/Snow_Statistics/runtime/real/(?:publication|lake)/[A-Za-z0-9_./-]{1,200}", parsed.path) and ".." not in parsed.path:
        return dict(namespace="file://snow-control", name=parsed.path)
    raise ValueError("Dataset is outside the explicit real metadata namespaces")


def validate_event(event):
    if set(event) != {"eventType", "eventTime", "producer", "schemaURL", "run", "job", "inputs", "outputs"}:
        raise ValueError("Unexpected real lineage fields")
    if event["eventType"] not in {"START", "COMPLETE", "FAIL"} or event["producer"] != PRODUCER or event["schemaURL"] != SCHEMA:
        raise ValueError("Invalid real lineage state")
    if event["job"] not in [dict(namespace=NAMESPACE, name=name) for name in JOBS] or set(event["run"]) != {"runId"}:
        raise ValueError("Unexpected real job identity")
    uuid.UUID(event["run"]["runId"])
    if datetime.fromisoformat(event["eventTime"].replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("Timezone required")
    if event["eventType"] != "COMPLETE" and event["outputs"]:
        raise ValueError("Only verified successful jobs may declare outputs")
    for group in (event["inputs"], event["outputs"]):
        if not isinstance(group, list) or len(group) > 16 or len({canonical(item) for item in group}) != len(group):
            raise ValueError("Unbounded or duplicate metadata datasets")
        for value in group:
            if set(value) != {"namespace", "name"} or dataset(value["namespace"] + value["name"]) != value:
                raise ValueError("Only verified real file/table-location metadata is supported")
    if len(canonical(event)) > 65536:
        raise ValueError("Real lineage envelope too large")


class RealJournal(Journal):
    def append(self, job, key, state, inputs, outputs=(), at=None):
        if job not in JOBS or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", key):
            raise ValueError("Invalid real lineage identity")
        run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, NAMESPACE + "/" + job + "/" + key))
        event = dict(eventType=state, eventTime=at or datetime.now(UTC).isoformat(), producer=PRODUCER,
                     schemaURL=SCHEMA, run=dict(runId=run_id), job=dict(namespace=NAMESPACE, name=job),
                     inputs=sorted(inputs, key=canonical), outputs=sorted(outputs, key=canonical))
        validate_event(event)
        with self.connection() as db:
            if any(row[0] not in JOBS for row in db.execute("SELECT DISTINCT job FROM events")):
                raise ValueError("Real metadata journal cannot reuse a synthetic journal")
            db.execute("BEGIN IMMEDIATE")
            prior = {kind: json.loads(payload) for kind, payload in db.execute("SELECT state,payload FROM events WHERE run_id=?", (run_id,))}
            if state in prior:
                if (prior[state] | {"eventTime": event["eventTime"]}) != event:
                    raise ValueError("Conflicting real lineage replay")
                return run_id
            if state != "START" and "START" not in prior or any(kind in prior for kind in ("COMPLETE", "FAIL")):
                raise ValueError("Invalid real lineage transition")
            if prior and prior["START"]["inputs"] != event["inputs"]:
                raise ValueError("Input metadata identity changed during actual execution")
            payload = canonical(event)
            db.execute("INSERT INTO events(run_id,state,job,run_key,payload,sha256) VALUES(?,?,?,?,?,?)",
                       (run_id, state, job, key, payload, digest(payload)))
        return run_id


class RealCapture:
    def __init__(self, journal, job, key, inputs):
        self.journal, self.job, self.key, self.inputs = journal, job, key, inputs
        self.outputs = []

    def __enter__(self):
        if self.journal:
            self.journal.append(self.job, self.key, "START", self.inputs)
        return self

    def __exit__(self, exc_type, exc, trace):
        if self.journal:
            self.journal.append(self.job, self.key, "FAIL" if exc_type else "COMPLETE", self.inputs, [] if exc_type else self.outputs)
        return False


def flush_real(journal, url, target="real-marquez", limit=100, send=None):
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("Real lineage delivery requires a verified loopback tunnel")
    delivered = 0
    with publication_lock(journal.path.parent / ("real-lineage-delivery-" + target)):
        for seq, event, sha in journal.pending(target, limit):
            validate_event(event)
            if digest(canonical(event)) != sha:
                raise ValueError("Metadata journal checksum differs")
            if send:
                send(event)
            else:
                request = Request(url.rstrip("/") + "/api/v1/lineage", data=canonical(event), headers={"Content-Type": "application/json"}, method="POST")
                with urlopen(request, timeout=5) as response:
                    if not 200 <= response.status < 300:
                        raise ValueError("Real lineage delivery rejected")
            journal.acknowledge({"target": target, "acknowledgements": [{"seq": seq, "sha256": sha}]})
            delivered += 1
    return delivered
