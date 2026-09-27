"""Actual initialization and submission for one production real storage epoch.

This module never promotes a synthetic fixture, reads a supplied success receipt,
or rewrites an epoch. It uses the owned engine APIs and a protected collector
status request. Secrets stay in private files and are excluded from receipts.
"""
import ipaddress
import json
import os
import re
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .io import digest, write_json
from .lifecycle import timestamp
from .publication import canonical, publication_lock
from .real_backend_lifecycle import KafkaClient
from .real_epoch import Epoch, expire_due, private_epoch_root
from .real_quiescent import (
    ROOT,
    STATES,
    TABLES,
    DockerStorage,
    WriterRegistry,
    bounded_json,
    expected_parameters,
    frozen,
    storage,
    topic_names,
    validate_doris_write,
    validate_initial,
    writer_hashes,
)
from .real_writer_bootstrap import verify_bootstrap
from .source_cursor import validate_status


def private_file(path):
    path = Path(path).absolute()
    if path.resolve() != path or not path.is_file() or path.stat().st_size > 262144:
        raise ValueError("Private configuration must be a bounded regular file without links")
    if os.name == "posix" and path.stat().st_mode & 0o077:
        raise ValueError("Private configuration must have mode 0600")
    return path


def collector_status(url, token_file):
    import httpx
    endpoint = urlsplit(url)
    if (endpoint.scheme != "http" or endpoint.hostname not in {"127.0.0.1", "localhost"}
            or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment
            or endpoint.path not in {"", "/"}):
        raise ValueError("Use a verified SSH loopback to the protected collector")
    token = private_file(token_file).read_text().strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", token):
        raise ValueError("Invalid private reader token format")
    with httpx.Client(base_url=url.rstrip("/"), timeout=10, trust_env=False, follow_redirects=False) as client:
        response = client.get("/analytics/private/v1/status", headers={"Authorization": "Bearer " + token})
        if response.status_code != 200 or len(response.content) > 262144:
            raise ValueError("Protected collector status is unavailable")
        value = response.json()
    identity = validate_status(value)
    if identity["source"] != "real" or value.get("aggregate_gap"):
        raise ValueError("A healthy real collector generation is required")
    return identity


def sql_statements(text, database):
    if not re.fullmatch(r"snow_real_[a-z][a-z0-9_]{2,23}", database):
        raise ValueError("Invalid owned real database")
    rewritten = text.replace("snow.", database + ".").replace("EXISTS snow;", "EXISTS " + database + ";")
    return [value for value in rewritten.split(";") if value.strip()]


def job_environment(manifest, registration, account, host):
    parameters = expected_parameters(manifest, registration["initial"]["kafka"])
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,100}", account["password"]):
        raise ValueError("Use an independently generated epoch credential")
    return dict(KAFKA_BOOTSTRAP=host + ":9092", SNOW_SOURCE="real", SNOW_REPLAY_LANE=parameters["lane"],
                SNOW_INPUT_TOPIC=parameters["input_topic"], DORIS_FE=host + ":8030",
                DORIS_TABLE=parameters["doris_table"], DORIS_USER=account["user"], DORIS_PASSWORD=account["password"],
                SNOW_REAL_READABLE_FROM=parameters["readable_from"], SNOW_REAL_RESTORE_NOT_AFTER=parameters["restore_not_after"],
                SNOW_REAL_START_OFFSET=str(parameters["start_offset"]), SNOW_REAL_CLUSTER_ID=parameters["cluster_id"],
                SNOW_REAL_TOPIC_ID=parameters["topic_id"])


def validate_namespaces(topics, databases, expected_topics=(), expected_databases=()):
    """A dedicated epoch must not adopt another lane's data or hidden databases."""
    systems = {"information_schema", "mysql", "__internal_schema"}
    if (set(topics) - {"__consumer_offsets", "__transaction_state"} != set(expected_topics)
            or set(databases) - systems != set(expected_databases)):
        raise ValueError("Unexpected user topics or databases in the isolated epoch")


def read_owned_tables(cursor, database):
    """Doris 3.0.6.2 returns four FULL TABLES columns, unlike MySQL's two."""
    if not re.fullmatch(r"snow_real_[a-z][a-z0-9_]{2,23}", database):
        raise ValueError("Invalid owned real database")
    cursor.execute(f"SHOW FULL TABLES FROM {database}")
    expected_columns = ["Tables_in_" + database, "Table_type", "Storage_format", "Inverted_index_storage_format"]
    if [column[0] for column in cursor.description] != expected_columns:
        raise ValueError("Unexpected locked Doris table metadata columns")
    rows = cursor.fetchall()
    expected = {name: "BASE TABLE" for name in TABLES} | {
        name: "VIEW" for name in ("daily_realtime", "daily_published", "report_published")}
    if (len(rows) != len(expected) or any(len(row) != 4 for row in rows) or
            {row[0]: row[1] for row in rows} != expected):
        raise ValueError("Unexpected table/view set in the owned real database")
    if any(tuple(row[2:]) != (("V2", "V2") if row[1] == "BASE TABLE" else ("NONE", "NONE")) for row in rows):
        raise ValueError("Unexpected locked Doris table storage format")
    return expected


def account_statements(database, account):
    if (not re.fullmatch(r"snow_real_[a-z][a-z0-9_]{2,23}", database)
            or account["user"] != "sr_" + database.removeprefix("snow_real_")):
        raise ValueError("Account is outside the owned real database")
    user, role = account["user"], account["user"] + "_role"
    return [(f"CREATE ROLE `{role}`", ()),
            (f"GRANT SELECT_PRIV,LOAD_PRIV ON {database}.* TO ROLE '{role}'", ()),
            (f"CREATE USER '{user}'@%s IDENTIFIED BY %s DEFAULT ROLE '{role}'", ("%", account["password"]))]


class ActualWriter:
    def __init__(self, root, epoch_id, url, token_file):
        self.root = private_epoch_root(root)
        self.epoch = Epoch(self.root / epoch_id, DockerStorage())
        self.manifest = frozen(self.epoch)  # Reject fixture mode before network/config access.
        self.registry = WriterRegistry(self.epoch)
        self.url, self.token_file = url, token_file
        self.directory = self.epoch.directory / "writer"
        self.directory.mkdir(mode=0o700, exist_ok=True)
        if self.directory.resolve() != self.directory or self.directory.stat().st_mode & 0o077:
            raise ValueError("Writer configuration directory must be private without links")
        spec = json.loads((self.epoch.directory / "compose.json").read_bytes())
        listener = spec["services"]["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"]
        self.host = listener.removeprefix("PLAINTEXT://").removesuffix(":9092")
        if not ipaddress.ip_address(self.host).is_private:
            raise ValueError("Only the frozen private analysis VM is supported")
        self.database = "snow_real_" + self.manifest["event_lane"]
        self.submitted = None

    def identity(self):
        return collector_status(self.url, self.token_file)

    def account(self):
        value = json.loads(private_file(self.directory / "account.json").read_bytes())
        if set(value) != {"user", "password"} or value["user"] != "sr_" + self.manifest["event_lane"]:
            raise ValueError("Credential is outside the exact epoch account")
        return value

    def connect(self, admin=False):
        import pymysql
        account = dict(user="root", password="") if admin else self.account()
        return pymysql.connect(host=self.host, port=9030, autocommit=True, connect_timeout=5,
                               read_timeout=30, write_timeout=30, **account)

    def sql(self, statement, values=(), *, admin=False):
        with self.connect(admin) as connection, connection.cursor() as cursor:
            cursor.execute(statement, values)
            return cursor.fetchall()

    def flink(self, path):
        import httpx
        with httpx.Client(base_url="http://127.0.0.1:8081", timeout=10, trust_env=False, follow_redirects=False) as client:
            response = client.get(path)
            if response.status_code != 200 or len(response.content) > 2097152:
                raise ValueError("Owned Flink metadata is unavailable")
        return response.json()

    def read_tables(self):
        with self.connect() as connection, connection.cursor() as cursor:
            return read_owned_tables(cursor, self.database)

    def read_initial_state(self, manifest, collector):
        if manifest != frozen(self.epoch) or self.identity() != collector:
            raise ValueError("Collector/epoch changed during initial readback")
        names = set(topic_names(manifest))
        self.check_namespaces(names, {self.database})
        broker = KafkaClient(self.host + ":9092", manifest["containers"]["kafka"])
        try:
            identity, bounds = broker.identity(names), broker.bounds(names)
        finally:
            broker.close()
        if len(bounds) != len(names) or any(partition != 0 for _, partition in bounds):
            raise ValueError("Only one partition per registered real topic is allowed")
        physical = [name for name, kind in self.read_tables().items() if kind == "BASE TABLE"]
        if set(physical) != set(TABLES):
            raise ValueError("Unexpected physical tables in the owned real database")
        counts = {name: int(self.sql(f"SELECT COUNT(*) FROM {self.database}.{name}")[0][0]) for name in physical}
        jobs = self.flink("/jobs/overview")["jobs"]
        bootstrap = verify_bootstrap(self.epoch.docker.command, manifest["containers"]["jobmanager"])
        state = {path: [] for path in STATES}
        return dict(kafka=dict(identity=identity, bounds={topic: dict(partition=0, start=start, end=end)
                                                        for (topic, _), (start, end) in bounds.items()}),
                    doris=dict(database=self.database, tables=counts), flink=dict(jobs=jobs), state=state,
                    bootstrap=bootstrap)

    def check_namespaces(self, topics=(), databases=()):
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=self.host + ":9092", request_timeout_ms=10000)
        try:
            validate_namespaces(admin.list_topics(), [row[0] for row in self.sql("SHOW DATABASES", admin=True)],
                                topics, databases)
        finally:
            admin.close()

    def initialize(self):
        from kafka.admin import KafkaAdminClient, NewTopic
        expire_due(self.root, self.epoch.docker)
        with publication_lock(self.directory):
            identity = self.identity()
            storage(self.epoch, running=True)
            if self.registry.path.exists() or (self.directory / "initializing.json").exists():
                raise ValueError("Initialization already attempted; preserve it and inspect or choose a new epoch")
            if self.flink("/jobs/overview")["jobs"]:
                raise ValueError("A new real epoch cannot adopt existing Flink jobs")
            admin = KafkaAdminClient(bootstrap_servers=self.host + ":9092", request_timeout_ms=10000)
            try:
                names = topic_names(self.manifest)
                validate_namespaces(admin.list_topics(), [row[0] for row in self.sql("SHOW DATABASES", admin=True)])
                write_json(self.directory / "initializing.json", dict(source="real", started_at=datetime.now(UTC).isoformat()))
                account = dict(user="sr_" + self.manifest["event_lane"], password=secrets.token_urlsafe(32))
                path = self.directory / "account.json"
                write_json(path, account)
                os.chmod(path, 0o600)
                with self.connect(True) as connection, connection.cursor() as cursor:
                    for name in ("schema.sql", "publication.sql"):
                        for statement in sql_statements((ROOT / "warehouse/doris" / name).read_text(), self.database):
                            cursor.execute(statement)
                    for statement, parameters in account_statements(self.database, account):
                        cursor.execute(statement, parameters)
                admin.create_topics([NewTopic(name, 1, 1, topic_configs={"retention.ms": "604800000",
                    "segment.ms": "60000", "segment.bytes": "16777216", "file.delete.delay.ms": "1000",
                    "cleanup.policy": "delete", "message.timestamp.type": "CreateTime"}) for name in names])
            finally:
                admin.close()
            return self.registry.initialize(identity, self)

    def finalize_initialization(self):
        """Finish an interrupted *empty* bootstrap by live readback, never DDL.

        The first attempt marker/account and original epoch remain untouched.
        A persisted intent binds this recovery to the actual empty storage,
        collector, original files and current reviewed executable hashes.
        """
        expire_due(self.root, self.epoch.docker)
        with publication_lock(self.directory):
            current = datetime.now(UTC)
            manifest = frozen(self.epoch)
            marker_file = self.directory / "initializing.json"
            marker = bounded_json(marker_file)
            if (set(marker) != {"source", "started_at"} or marker["source"] != "real"
                    or not timestamp(manifest["original_min_accepted_at"]) <= timestamp(marker["started_at"]) <= current
                    or current >= timestamp(manifest["expires_at"])):
                raise ValueError("Only a still-live interrupted initialization may be finalized")
            if any((self.directory / name).exists() for name in ("submission.json", "job.env")) or self.registry.job_path.exists():
                raise ValueError("An epoch with a submission attempt cannot be finalized as empty")
            self.account()  # Validate the original exact account; never create/reset it.
            account_file = private_file(self.directory / "account.json")
            identity = self.identity()
            before = storage(self.epoch, running=True)
            bindings = dict(schema_version=1, source="real", epoch_id=manifest["epoch_id"],
                            owner_manifest_sha256=digest(canonical(manifest)), collector=identity,
                            initializing_sha256=digest(marker_file.read_bytes()),
                            account_sha256=digest(account_file.read_bytes()), storage=before,
                            writers=writer_hashes(), expires_at=manifest["expires_at"])
            intent_path = self.directory / "initialization-finalization-intent.json"
            completed = self.directory / "initialization-finalization.json"
            if completed.exists():
                raise ValueError("Initialization finalization already completed; use status")
            initial = validate_initial(self.read_initial_state(manifest, identity), manifest)
            if storage(self.epoch, running=True) != before:
                raise ValueError("Storage changed during empty initialization recovery")
            if intent_path.exists():
                intent = bounded_json(private_file(intent_path))
                if (set(intent) != set(bindings) | {"started_at"}
                        or any(intent[key] != value for key, value in bindings.items())
                        or not timestamp(marker["started_at"]) <= timestamp(intent["started_at"]) <= current):
                    raise ValueError("Interrupted finalization no longer matches its immutable intent")
            else:
                if self.registry.path.exists():
                    raise ValueError("Cannot adopt an existing registration without a prior finalization intent")
                intent = bindings | {"started_at": current.isoformat()}
                write_json(intent_path, intent)
                os.chmod(intent_path, 0o600)
            if self.registry.path.exists():
                result = self.registry.read(identity)
            else:
                result = self.registry.initialize(identity, self)
            # A normal JVM restart can change only the names of fixed-byte
            # bootstrap files. Both readbacks independently validate those
            # exact resources; payload/state/topic identities must remain equal.
            registered_initial = validate_initial(result["initial"], manifest)
            if (result["storage"] != before or
                    {k: v for k, v in registered_initial.items() if k != "bootstrap"} !=
                    {k: v for k, v in initial.items() if k != "bootstrap"}):
                raise ValueError("Empty engine readback changed during registration")
            # Preserve the original marker and credential bytes, even on retry.
            if (digest(marker_file.read_bytes()) != bindings["initializing_sha256"]
                    or digest(account_file.read_bytes()) != bindings["account_sha256"]
                    or storage(self.epoch, running=True) != before or self.identity() != identity):
                raise ValueError("Initialization bindings changed during finalization")
            write_json(completed, dict(schema_version=1, source="real", completed_at=datetime.now(UTC).isoformat(),
                                       intent_sha256=digest(intent_path.read_bytes()),
                                       registration_sha256=digest(self.registry.path.read_bytes()),
                                       live_readback_sha256=digest(canonical(initial)),
                                       original_files_preserved=True, ddl_executed=False,
                                       original_expires_at=manifest["expires_at"]))
            os.chmod(completed, 0o600)
            return result

    def read_job(self, job_id):
        # A state fetched from Flink alone cannot attest arbitrary parameters.
        # Only this invocation's successful, hash-checked submission supplies them.
        if self.submitted is None or self.submitted["job_id"] != job_id:
            raise ValueError("No actual submission bound to this probe invocation")
        value = self.flink("/jobs/" + job_id)
        if value["jid"] != job_id or value["name"] != "Snow Statistics real " + self.manifest["event_lane"]:
            raise ValueError("Flink returned a different submitted job")
        return dict(job_id=job_id, state=value["state"], jar_sha256=self.submitted["jar_sha256"],
                    parameters=self.submitted["parameters"])

    def submit(self):
        expire_due(self.root, self.epoch.docker)
        with publication_lock(self.directory):
            registration = self.registry.read(self.identity())
            if self.registry.job_path.exists() or (self.directory / "submission.json").exists():
                raise ValueError("Submission already exists; inspect it, never silently submit another job")
            if storage(self.epoch, running=True) != registration["storage"] or self.flink("/jobs/overview")["jobs"]:
                raise ValueError("Only the unchanged initialized empty job cluster may accept the first writer")
            environment = job_environment(self.manifest, registration, self.account(), self.host)
            path = self.directory / "job.env"
            path.write_text("".join(key + "=" + value + "\n" for key, value in environment.items()))
            os.chmod(path, 0o600)
            jm = self.manifest["containers"]["jobmanager"]
            jar = "/opt/flink/usrlib/snow-realtime.jar"
            hashes = {role: self.epoch.docker.command(["exec", self.manifest["containers"][role], "sha256sum", jar]).decode().split()[0]
                      for role in ("jobmanager", "taskmanager")}
            if set(hashes.values()) != {registration["jar_sha256"]}:
                raise ValueError("Actual mounted JAR differs from the initialized epoch")
            write_json(self.directory / "submission.json", dict(source="real", state="submitting",
                environment_sha256=digest(path.read_bytes()), jar_sha256=registration["jar_sha256"]))
            output = self.epoch.docker.command(["exec", "--env-file", str(path), jm, "/opt/flink/bin/flink", "run", "-d",
                                                "-c", "dev.xiaob.snow.RealtimeJob", jar], timeout=120)
            match = re.search(rb"JobID\s+([a-f0-9]{32})", output)
            if not match:
                raise RuntimeError("Flink did not acknowledge one submitted JobID")
            job_id = match.group(1).decode()
            self.submitted = dict(job_id=job_id, jar_sha256=registration["jar_sha256"],
                                  parameters=expected_parameters(self.manifest, registration["initial"]["kafka"]))
            write_json(self.directory / "submission.json", dict(source="real", state="acknowledged", **self.submitted,
                environment_sha256=digest(path.read_bytes())))
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                result = self.read_job(job_id)
                if result["state"] == "RUNNING":
                    return self.registry.record_job(job_id, self)
                if result["state"] in {"FAILED", "CANCELED", "FINISHED"}:
                    raise ValueError("The registered real job terminated before readiness")
                time.sleep(0.5)
            raise TimeoutError("Real Flink writer did not become RUNNING within its bounded startup")

    def publish(self, release_directory):
        from .publication import publish
        from .real_publication import read_real_release
        expire_due(self.root, self.epoch.docker)
        release = read_real_release(release_directory)
        identity = self.identity()
        package = release["daily"]
        def guard():
            validate_doris_write(package, self.registry, identity)
        guard()
        with self.connect() as connection:
            result = publish(GuardedConnection(connection, guard), package,
                             self.directory / "publication", database=self.database)
        guard()
        write_json(self.directory / "publication" / (result["snapshot_id"] + ".json"), result)
        return result


class GuardedConnection:
    """Every SQL read and write rechecks physical identity and original expiry."""
    def __init__(self, connection, guard):
        self.connection, self.guard = connection, guard

    def cursor(self):
        return GuardedCursor(self.connection.cursor(), self.guard)


class GuardedCursor:
    def __init__(self, cursor, guard):
        self.cursor, self.guard = cursor, guard

    def __enter__(self):
        self.cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self.cursor.__exit__(*args)

    def execute(self, *args, **kwargs):
        self.guard()
        return self.cursor.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self.guard()
        return self.cursor.executemany(*args, **kwargs)

    def fetchall(self):
        self.guard()
        return self.cursor.fetchall()


def failure_metadata(error):
    # Driver errors can contain SQL, passwords or URLs. Record only their type.
    return dict(complete=False, error_class=type(error).__name__, source="real")
