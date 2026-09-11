"""Optional, bounded local OpenLineage journal; no network on the compute path."""
import json
import os
import re
import sqlite3
import sys
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .io import digest
from .model_publication import validate_model
from .publication import canonical, publication_lock

PRODUCER = "https://github.com/X1aoB/Snow_Statistics"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
NAMESPACE = "snow-statistics.synthetic"
JOBS = {"snow_models.compute_operations", "snow_models.compute_behavior", "snow_models.publish"}
ROOT = "/home/snow/Snow_Statistics/runtime/publication/"
STATES = {"START", "COMPLETE", "FAIL"}


def file_dataset(name):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[a-f0-9]{64}\.json)?", name) or ".." in name:
        raise ValueError("Invalid project artifact")
    return dict(namespace="file://snow-control", name=ROOT + name)


def input_dataset(path):
    parsed = urlsplit(path)
    if parsed.scheme != "hdfs" or parsed.query or parsed.fragment or not re.fullmatch(
            r"/snow/ods/synthetic/kafka/[a-z0-9-]{1,60}/snapshots/[a-f0-9]{64}/_snapshot.json", parsed.path):
        raise ValueError("Lineage requires an immutable synthetic input manifest")
    return [dict(namespace="hdfs://snow-control:9000", name=parsed.path)]


def compute_outputs(path, kind):
    m = validate_model(json.loads(Path(path).read_bytes()), kind)
    if m["engine"] != "Spark 3.5.7" or m["master"] != "yarn" or set(m["hive_tables"]) != set(m["counts"]):
        raise ValueError("Missing verified Hive outputs")
    tables = []
    for table in m["hive_tables"].values():
        if not re.fullmatch(r"snow_synthetic\.[a-zA-Z0-9_]+", table):
            raise ValueError("Invalid synthetic Hive table")
        tables.append(dict(namespace="hive://snow-control:9083", name=table))
    return tables + [file_dataset(m["run_id"] + "." + kind + ".json")]


def validate_event(event):
    if set(event) != {"eventType", "eventTime", "producer", "schemaURL", "run", "job", "inputs", "outputs"}:
        raise ValueError("Unexpected lineage fields")
    if event["eventType"] not in STATES or event["producer"] != PRODUCER or event["schemaURL"] != SCHEMA:
        raise ValueError("Unexpected lineage event")
    uuid.UUID(event["run"]["runId"])
    if set(event["run"]) != {"runId"} or event["job"] not in [dict(namespace=NAMESPACE, name=j) for j in JOBS]:
        raise ValueError("Unexpected lineage job")
    if datetime.fromisoformat(event["eventTime"].replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("Lineage time requires timezone")
    if event["eventType"] != "COMPLETE" and event["outputs"]:
        raise ValueError("Only completed jobs declare verified outputs")
    for datasets in (event["inputs"], event["outputs"]):
        if not isinstance(datasets, list) or len(datasets) > 16 or len({canonical(d) for d in datasets}) != len(datasets):
            raise ValueError("Invalid dataset list")
        for d in datasets:
            if set(d) != {"namespace", "name"}:
                raise ValueError("Unexpected dataset metadata")
            if d["namespace"] == "file://snow-control":
                if not d["name"].startswith(ROOT) or file_dataset(d["name"][len(ROOT):]) != d:
                    raise ValueError("Invalid artifact namespace")
            elif d["namespace"] == "hdfs://snow-control:9000":
                input_dataset(d["namespace"] + d["name"])
            elif d["namespace"] == "hive://snow-control:9083":
                if not re.fullmatch(r"snow_synthetic\.[A-Za-z0-9_]+", d["name"]):
                    raise ValueError("Invalid table")
            else:
                raise ValueError("Unexpected dataset namespace")
    if len(canonical(event)) > 65536:
        raise ValueError("Oversized lineage event")


class Journal:
    def __init__(self, path, max_bytes=16 * 1024 * 1024):
        if type(max_bytes) is not int or not 16384 <= max_bytes <= 16 * 1024 * 1024:
            raise ValueError("Journal budget must be within 16 KiB..16 MiB")
        self.path, self.max_bytes = Path(path), max_bytes

    def backup(self, destination):
        destination = Path(destination)
        if destination.exists() or destination.resolve() == self.path.resolve():
            raise ValueError("Choose a new journal snapshot file")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        with self.connection() as source, closing(sqlite3.connect(temporary)) as target:
            source.backup(target)
        with temporary.open("r+b") as stream:
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        if os.name != "nt":
            fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    @contextmanager
    def connection(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=2)
        try:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(f"PRAGMA max_page_count={self.max_bytes // db.execute('PRAGMA page_size').fetchone()[0]}")
            db.executescript("""CREATE TABLE IF NOT EXISTS events(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, state TEXT NOT NULL,
                job TEXT NOT NULL, run_key TEXT NOT NULL, payload BLOB NOT NULL, sha256 TEXT NOT NULL,
                UNIQUE(run_id,state));
                CREATE TABLE IF NOT EXISTS acknowledgements(
                target TEXT NOT NULL, seq INTEGER NOT NULL REFERENCES events(seq), sha256 TEXT NOT NULL,
                PRIMARY KEY(target,seq));""")
            with db:
                yield db
        finally:
            db.close()

    def append(self, job, key, state, inputs, outputs=(), at=None):
        if job not in JOBS or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", key) or state not in STATES:
            raise ValueError("Invalid lineage identity")
        run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, NAMESPACE + "/" + job + "/" + key))
        event = dict(eventType=state, eventTime=at or datetime.now(UTC).isoformat(), producer=PRODUCER,
                     schemaURL=SCHEMA, run=dict(runId=run_id), job=dict(namespace=NAMESPACE, name=job),
                     inputs=sorted(inputs, key=canonical), outputs=sorted(outputs, key=canonical))
        validate_event(event)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = {s: json.loads(p) for s, p in db.execute("SELECT state,payload FROM events WHERE run_id=?", (run_id,))}
            if state in previous:
                if (previous[state] | {"eventTime": event["eventTime"]}) != event:
                    raise ValueError("Conflicting lineage replay")
                return run_id
            if state != "START" and "START" not in previous or any(s in previous for s in ("COMPLETE", "FAIL")):
                raise ValueError("Invalid lineage transition")
            if previous and previous["START"]["inputs"] != event["inputs"]:
                raise ValueError("Input identity changed during a run")
            payload = canonical(event)
            db.execute("INSERT INTO events(run_id,state,job,run_key,payload,sha256) VALUES(?,?,?,?,?,?)",
                       (run_id, state, job, key, payload, digest(payload)))
        return run_id

    def pending(self, target, limit=100):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", target) or not 1 <= limit <= 1000:
            raise ValueError("Invalid target/batch limit")
        with self.connection() as db:
            return [(seq, json.loads(payload), sha) for seq, payload, sha in db.execute(
                "SELECT e.seq,e.payload,e.sha256 FROM events e LEFT JOIN acknowledgements a ON a.seq=e.seq AND a.target=? WHERE a.seq IS NULL ORDER BY e.seq LIMIT ?", (target, limit))]

    def acknowledge(self, receipt):
        target, entries = receipt["target"], receipt["acknowledgements"]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", target) or len(entries) > 10000:
            raise ValueError("Invalid acknowledgement receipt")
        with self.connection() as db:
            for item in entries:
                if db.execute("SELECT sha256 FROM events WHERE seq=?", (item["seq"],)).fetchone() != (item["sha256"],):
                    raise ValueError("Acknowledgement does not match this journal")
                db.execute("INSERT OR IGNORE INTO acknowledgements VALUES(?,?,?)", (target, item["seq"], item["sha256"]))

    def receipt(self, target):
        with self.connection() as db:
            return dict(target=target, acknowledgements=[dict(seq=n, sha256=h) for n, h in db.execute(
                "SELECT seq,sha256 FROM acknowledgements WHERE target=? ORDER BY seq", (target,))])

    def status(self, target):
        with self.connection() as db:
            count = db.execute("SELECT count(*) FROM events").fetchone()[0]
            ack = db.execute("SELECT count(*) FROM acknowledgements WHERE target=?", (target,)).fetchone()[0]
            opened = [dict(run_id=r, job=j, run_key=k) for r, j, k in db.execute("SELECT run_id,job,run_key FROM events GROUP BY run_id HAVING count(*)=1")]
        return dict(events=count, acknowledged=ack, pending=count - ack, open_runs=opened)

    def reconcile_failed(self, receipt):
        """Only explicit failed-attempt metadata can close an interrupted run."""
        if receipt["dag_id"] != "snow_models" or len(receipt["runs"]) != 1:
            raise ValueError("Unexpected Airflow receipt")
        key = "af-" + digest(receipt["runs"][0]["run_id"].encode())[:24]
        changes = 0
        for task in receipt["tasks"]:
            if task["state"] not in ("failed", "up_for_retry") or not task.get("end_date"):
                continue
            job, attempt = "snow_models." + task["task_id"], key + "-t" + str(task["try_number"])
            with self.connection() as db:
                rows = db.execute("SELECT payload FROM events WHERE job=? AND run_key=? ORDER BY seq", (job, attempt)).fetchall()
            if len(rows) == 1:
                self.append(job, attempt, "FAIL", json.loads(rows[0][0])["inputs"])
                changes += 1
        return changes


def flush(journal, url, target, limit=100, send=None, fail_after_send=False):
    endpoint = urlsplit(url)
    if endpoint.scheme != "http" or endpoint.hostname not in ("127.0.0.1", "localhost") or endpoint.username or endpoint.password or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment:
        raise ValueError("Use the local Marquez API or a verified SSH loopback tunnel")
    def post(event):
        request = Request(url.rstrip("/") + "/api/v1/lineage", data=canonical(event), headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=5) as response:
            if not 200 <= response.status < 300:
                raise ValueError("Lineage API rejected event")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", target):
        raise ValueError("Invalid target")
    delivered = 0
    with publication_lock(journal.path.parent / ("lineage-delivery-" + target)):
        for seq, event, sha in journal.pending(target, limit):
            validate_event(event)
            if digest(canonical(event)) != sha:
                raise ValueError("Lineage checksum mismatch")
            (send or post)(event)
            if fail_after_send:
                raise RuntimeError("Injected failure after HTTP acceptance, before local acknowledgement")
            journal.acknowledge(dict(target=target, acknowledgements=[dict(seq=seq, sha256=sha)]))
            delivered += 1
    return delivered


class Capture:
    def __init__(self, job, key, inputs):
        self.job, self.key, self.inputs_factory = job, key, inputs
        self.output_factory = lambda: []
        self.journal = None

    def __enter__(self):
        if os.getenv("SNOW_LINEAGE_ENABLED", "false").lower() == "true":
            try:
                self.inputs = self.inputs_factory()
                journal = Journal(os.environ["SNOW_LINEAGE_DB"])
                journal.append(self.job, self.key, "START", self.inputs)
                self.journal = journal
            except Exception as error:
                print("snow_lineage_capture_failed phase=START reason=" + type(error).__name__, file=sys.stderr)
        return self

    def outputs(self, factory):
        self.output_factory = factory

    def __exit__(self, error_type, error, trace):
        if self.journal:
            try:
                self.journal.append(self.job, self.key, "FAIL" if error_type else "COMPLETE", self.inputs,
                                    [] if error_type else self.output_factory())
            except Exception as failure:
                print("snow_lineage_capture_failed phase=TERMINAL reason=" + type(failure).__name__, file=sys.stderr)
        return False
