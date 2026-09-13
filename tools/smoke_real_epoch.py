"""Real engine branch, synthetic-only source data, one exact owned VM epoch.

Run only after the host resource gate and explicit VM availability handoff. No
production endpoint, default synthetic database, or old checkpoint is accepted.
"""
import argparse
import hashlib
import ipaddress
import json
import os
import re
import secrets
import socket
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime

from snow_statistics.io import write_json
from snow_statistics.real_engine_fixture import (
    diagnostics,
    events,
    java_environment,
    oracle,
    restored_checkpoint,
    retained_checkpoint,
    schema_statements,
    test_scope,
)
from snow_statistics.real_epoch import ROOT, DockerEpoch, Epoch, expire_due, private_epoch_root


def until(check, label, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as error:
            # Driver exception bodies can contain credentials; never log them.
            if isinstance(error, (AssertionError, ValueError)):
                raise
        time.sleep(0.5)
    raise RuntimeError("Bounded synthetic engine check timed out: " + label)


class Acceptance:
    def __init__(self, root, name):
        self.root, self.docker = private_epoch_root(root), DockerEpoch()
        self.epoch = Epoch(self.root / name, self.docker)
        self.manifest = self.epoch.read()
        self.scope = test_scope(self.manifest)
        self.directory = self.epoch.directory / "engine-test"
        if self.directory.resolve() != self.directory.absolute():
            raise ValueError("Synthetic evidence directory cannot traverse links")
        self.directory.mkdir(mode=0o700, exist_ok=True)
        if os.name == "posix" and self.directory.stat().st_mode & 0o077:
            raise ValueError("Synthetic engine evidence directory must be private")
        self.compose = json.loads((self.epoch.directory / "compose.json").read_bytes())
        advertised = self.compose["services"]["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"]
        self.host = advertised.removeprefix("PLAINTEXT://").removesuffix(":9092")
        if not ipaddress.ip_address(self.host).is_private:
            raise ValueError("Synthetic engine target must be this private analysis VM")
        self.database = self.scope["database"]

    def current(self):
        if self.epoch.read() != self.manifest:
            raise ValueError("Engine epoch owner changed during acceptance")
        if datetime.now(UTC) >= datetime.fromisoformat(self.manifest["expires_at"]):
            raise ValueError("Engine fixture expired")
        return self.epoch._inspect(self.manifest)

    def credentials(self):
        return json.loads((self.directory / "secrets.json").read_bytes())

    def connect(self, admin=False):
        import pymysql
        account = {"user": "root", "password": ""} if admin else self.credentials()
        return pymysql.connect(host=self.host, port=9030, user=account["user"], password=account["password"],
                               autocommit=True, connect_timeout=5, read_timeout=30, write_timeout=30)

    def query(self, sql, values=(), admin=False):
        with self.connect(admin) as connection, connection.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchall()

    def storage(self):
        from kafka.admin import KafkaAdminClient, NewTopic

        from snow_statistics.real_backend_lifecycle import KafkaClient
        expire_due(self.root, self.docker)
        self.epoch.start("storage")
        self.current()
        def healthy():
            import pymysql
            if not self.docker.inspect_container(self.manifest["containers"]["doris-be"])["running"]:
                raise ValueError("Owned BE exited during bounded startup; inspect the private startup logs")
            with self.connect(True) as connection, connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SHOW BACKENDS")
                rows = cursor.fetchall()
                return len(rows) == 1 and str(rows[0]["Alive"]).lower() in {"true", "1"}
        until(healthy, "Doris FE/BE healthy", 180)
        be = self.manifest["containers"]["doris-be"]
        startup_hash = self.docker.command(["exec", be, "sha256sum", "/opt/apache-doris/be/bin/start_be.sh"]).decode().split()[0]
        assert startup_hash == "dcb0d8265e282cc1deec46ac039ea913df0833b877fe2a703f7e314a65954107"
        binary = self.docker.command(["exec", be, "stat", "-c", "%s %a", "/opt/apache-doris/be/lib/doris_be"]).decode().split()
        assert binary == ["2867606656", "755"]
        size = json.loads(self.docker.command(["inspect", "--size", be]))[0]["SizeRw"]
        assert 0 <= size < 32 * 1024**2
        startup = dict(patched_script_sha256=startup_hash, binary_bytes=int(binary[0]), binary_mode=binary[1],
                       writable_layer_bytes=size, large_binary_copyup_avoided=True)
        pids = int(self.docker.command(["exec", be, "cat", "/sys/fs/cgroup/pids.current"]))
        pids_limit = int(self.docker.command(["exec", be, "cat", "/sys/fs/cgroup/pids.max"]))
        pid_events = dict(line.split() for line in self.docker.command(["exec", be, "cat", "/sys/fs/cgroup/pids.events"]).decode().splitlines())
        logs = self.docker.command(["logs", "--tail", "500", be]).decode()
        jni = re.findall(r"set final LIBHDFS_OPTS: ([^\n]*)", logs)
        assert jni and "-Xmx256m" in jni[-1] and "-Xmx2048m" not in jni[-1]
        assert pids_limit == self.compose["services"]["doris-be"]["pids_limit"] == 512
        assert 0 < pids < pids_limit and int(pid_events["max"]) == 0
        startup.update(pids_current=pids, pids_limit=pids_limit, pids_limit_hit_count=int(pid_events["max"]),
                       effective_jni_max_heap_mib=256)
        if (self.directory / "storage.json").exists():
            assert self.query(f"SELECT COUNT(*) FROM {self.database}.events_realtime")[0][0] == 0
            return json.loads((self.directory / "storage.json").read_bytes())
        if (self.directory / "secrets.json").exists():
            raise ValueError("Partial bootstrap retained; inspect it or create a new explicit fixture epoch")
        assert self.query("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s", (self.database,), True)[0][0] == 0
        account = dict(user=self.scope["user"], password=secrets.token_urlsafe(32))
        write_json(self.directory / "secrets.json", account)
        os.chmod(self.directory / "secrets.json", 0o600)
        with self.connect(True) as connection, connection.cursor() as cursor:
            for name in ("schema.sql", "publication.sql"):
                text = (ROOT / "warehouse/doris" / name).read_text()
                for statement in schema_statements(text, self.database):
                    cursor.execute(statement)
            cursor.execute(f"CREATE ROLE '{self.scope['role']}'")
            cursor.execute(f"GRANT SELECT_PRIV,LOAD_PRIV ON {self.database}.* TO ROLE '{self.scope['role']}'")
            cursor.execute(f"CREATE USER '{self.scope['user']}'@'%' IDENTIFIED BY %s DEFAULT ROLE '{self.scope['role']}'", (account["password"],))
        assert self.query(f"SELECT COUNT(*) FROM {self.database}.events_realtime")[0][0] == 0
        admin = KafkaAdminClient(bootstrap_servers=self.host + ":9092", request_timeout_ms=10000)
        try:
            assert not set(self.scope["topics"].values()) & set(admin.list_topics())
            admin.create_topics([NewTopic(name, 1, 1, topic_configs={
                "retention.ms": "604800000", "segment.ms": "60000", "segment.bytes": "16777216",
                "file.delete.delay.ms": "1000", "cleanup.policy": "delete", "message.timestamp.type": "CreateTime"
            }) for name in self.scope["topics"].values()])
        finally:
            admin.close()
        broker = KafkaClient(self.host + ":9092", self.manifest["containers"]["kafka"])
        try:
            identity = broker.identity({self.scope["topics"]["events"]})
            assert list(broker.bounds({self.scope["topics"]["events"]}).values()) == [(0, 0)]
            retention = broker.retention_settings(set(self.scope["topics"].values()))
        finally:
            broker.close()
        env = java_environment(self.manifest, self.scope, identity, account["password"], self.host)
        path = self.directory / "job.env"
        path.write_text("".join(key + "=" + value + "\n" for key, value in env.items()))
        os.chmod(path, 0o600)
        result = dict(input_origin="synthetic fixtures", source="real", mode="synthetic_engine_test",
                      epoch_id=self.manifest["epoch_id"], generation=self.manifest["generation"],
                      database=self.database, user=self.scope["user"], role=self.scope["role"],
                      grants="SELECT_PRIV,LOAD_PRIV only on the new epoch database", topics=self.scope["topics"],
                      identity=identity, retention=retention, storage_health=True, be_startup_readback=startup)
        write_json(self.directory / "storage.json", result)
        return result

    def metrics(self):
        return [dict(source=source, app=app, date=str(day), pv=int(pv), uv=int(uv), requests=int(requests), successes=int(successes))
                for source, app, day, pv, uv, requests, successes in self.query(
                    f"SELECT source,app,business_date,pv,uv,requests,successes FROM {self.database}.daily_realtime ORDER BY source,business_date,app")]

    def side(self, suffix):
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=self.host + ":9092", enable_auto_commit=False, group_id=None,
                                 allow_auto_create_topics=False, max_partition_fetch_bytes=1048576)
        try:
            part = TopicPartition(self.scope["topics"][suffix], 0)
            consumer.assign([part])
            begin, end = consumer.beginning_offsets([part])[part], consumer.end_offsets([part])[part]
            if end - begin > 500:
                raise ValueError("Unexpected synthetic diagnostic expansion")
            consumer.seek(part, begin)
            rows, deadline = [], time.monotonic() + 20
            while consumer.position(part) < end:
                if time.monotonic() > deadline:
                    raise RuntimeError("Bounded side-topic scan timed out")
                for row in consumer.poll(timeout_ms=500, max_records=100).get(part, []):
                    if row.offset < end:
                        rows.append(dict(offset=row.offset, timestamp_ms=row.timestamp, value=json.loads(row.value)))
            return rows
        finally:
            consumer.close()

    def checkpoint_files(self):
        self.current()
        jm = self.manifest["containers"]["jobmanager"]
        output = self.docker.command(["exec", jm, "find", "/checkpoints", "-type", "f"])
        paths = output.decode().splitlines()
        if len(paths) > 200 or any(not re.fullmatch(r"/checkpoints/[a-zA-Z0-9_./-]+", path)
                                   or "/../" in path for path in paths):
            raise ValueError("Unexpected fixture checkpoint file scope")
        files = {}
        for path in paths:
            size = int(self.docker.command(["exec", jm, "stat", "-c", "%s", path]))
            digest = self.docker.command(["exec", jm, "sha256sum", path]).decode().split()[0]
            files[path] = dict(bytes=size, sha256=digest)
        if sum(row["bytes"] for row in files.values()) > 8 * 1024**2:
            raise ValueError("Synthetic checkpoint unexpectedly exceeds the bounded sample budget")
        return dict(files=files, total_bytes=sum(row["bytes"] for row in files.values()),
                    sha256=hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest())

    def pause_session(self, flink, job):
        def get(path=""):
            response = flink.get("/jobs/" + job + path)
            response.raise_for_status()
            return response.json()
        config = get("/checkpoints/config")
        assert config["externalization"] == dict(enabled=True, delete_on_cancellation=False)
        first = get("/checkpoints")["latest"]["completed"]["id"]
        until(lambda: get("/checkpoints")["latest"].get("completed", {}).get("id", -1) > first,
              "checkpoint after all diagnostics and SQL publication", 45)
        flink.patch("/jobs/" + job, params={"mode": "cancel"}).raise_for_status()
        until(lambda: get()["state"] == "CANCELED", "canceled before session stop", 45)
        checkpoint = get("/checkpoints")["latest"]["completed"]
        path = retained_checkpoint(checkpoint, job)
        files = self.checkpoint_files()
        assert path.removeprefix("file:") + "/_metadata" in files["files"]
        result = dict(epoch_id=self.manifest["epoch_id"], generation=self.manifest["generation"],
                      original_min_accepted_at=self.manifest["original_min_accepted_at"], expires_at=self.manifest["expires_at"],
                      input_origin="synthetic fixtures", previous_job_id=job, checkpoint=checkpoint,
                      checkpoint_config=config, checkpoint_files=files, runtime_jar_sha256=self.manifest["jar"]["sha256"])
        write_json(self.directory / "session-pause.json", result)
        return result

    def resume(self):
        import httpx
        from kafka import KafkaProducer
        previous = json.loads((self.directory / "session-pause.json").read_bytes())
        accepted = json.loads((self.directory / "acceptance.json").read_bytes())
        rows = json.loads((self.directory / "fixture-input.json").read_bytes())["on_time"]
        for key in ("epoch_id", "generation", "original_min_accepted_at", "expires_at"):
            assert previous[key] == self.manifest[key]
        assert previous["runtime_jar_sha256"] == self.manifest["jar"]["sha256"]
        assert accepted["session_recovery_pending"] is True
        assert not any(row["running"] for row in self.current()[0].values())
        path = retained_checkpoint(previous["checkpoint"], previous["previous_job_id"])
        self.epoch.start("storage")
        until(lambda: self.query(f"SELECT COUNT(*) FROM {self.database}.events_realtime")[0][0] > 0, "restored Doris storage", 180)
        self.epoch.start("realtime")
        jm = self.manifest["containers"]["jobmanager"]
        jar = "/opt/flink/usrlib/snow-realtime.jar"
        for role in ("jobmanager", "taskmanager"):
            assert self.docker.command(["exec", self.manifest["containers"][role], "sha256sum", jar]).decode().split()[0] == self.manifest["jar"]["sha256"]
        assert self.checkpoint_files() == previous["checkpoint_files"]
        job = None
        with httpx.Client(base_url="http://127.0.0.1:8081", timeout=10, trust_env=False) as flink:
            try:
                until(lambda: flink.get("/overview").json().get("taskmanagers") == 1, "new Flink session ready", 90)
                assert flink.get("/jobs/overview").json()["jobs"] == []
                output = self.docker.command(["exec", "--env-file", str(self.directory / "job.env"), jm,
                    "/opt/flink/bin/flink", "run", "-d", "--fromSavepoint", path, "--claimMode", "no_claim",
                    "-c", "dev.xiaob.snow.RealtimeJob", jar], timeout=120)
                match = re.search(rb"JobID\s+([a-f0-9]{32})", output)
                assert match
                job = match.group(1).decode()
                assert job != previous["previous_job_id"]
                def get(suffix=""):
                    response = flink.get("/jobs/" + job + suffix)
                    response.raise_for_status()
                    return response.json()
                until(lambda: get()["state"] == "RUNNING", "new job running from retained state", 120)
                actual = until(lambda: (value if (value := get("/checkpoints"))["counts"]["restored"] >= 1 else None),
                               "new session checkpoint restore acknowledgement", 45)
                restored_checkpoint(actual, previous["checkpoint"], previous["previous_job_id"])
                before = len(self.side("duplicates"))
                producer = KafkaProducer(bootstrap_servers=self.host + ":9092", acks="all", retries=2,
                                          value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode())
                try:
                    for row in rows[:2]:
                        producer.send(self.scope["topics"]["events"], partition=0, value=row,
                            timestamp_ms=int(datetime.fromisoformat(row["accepted_at"]).timestamp() * 1000)).get(timeout=10)
                finally:
                    producer.close(timeout=10)
                after = until(lambda: (value if len(value := self.side("duplicates")) >= before + 2 else None), "restored dedup state", 45)
                assert self.metrics() == accepted["integer_metrics"] == oracle(rows)
                current = self.checkpoint_files()
                assert all(current["files"].get(name) == record for name, record in previous["checkpoint_files"]["files"].items())
                result = dict(input_origin="synthetic fixtures", source_branch="real", production_requests=0,
                    previous_job_id=previous["previous_job_id"], new_job_id=job, checkpoint=previous["checkpoint"],
                    restored=actual, restore_claim_mode="no_claim", allow_non_restored_state=False,
                    retained_checkpoint_files=previous["checkpoint_files"], source_files_unchanged_after_restore=True,
                    duplicate_count_before=before, duplicate_count_after=len(after), integer_metrics=self.metrics(),
                    epoch_id=self.manifest["epoch_id"], generation=self.manifest["generation"],
                    original_expiry_unchanged=self.manifest["expires_at"], all_engines_stopped_then_restarted=True,
                    new_session_empty_before_submit=True, local_synthetic_evidence_only=True)
                write_json(self.directory / "session-resume.json", result)
                accepted.update(session_recovery_pending=False, full_session_checkpoint_restore_verified=True)
                write_json(self.directory / "acceptance.json", accepted)
                return result
            finally:
                if job:
                    flink.patch("/jobs/" + job, params={"mode": "cancel"}).raise_for_status()
                self.epoch.stop()

    def run(self, count=20):
        import httpx
        from kafka import KafkaProducer

        from snow_statistics.freshness import summarize
        from snow_statistics.publication import publication_database, publish, read_published
        if not 10 <= count <= 100:
            raise ValueError("Only 10-100 synthetic on-time events are allowed")
        if (self.directory / "job.json").exists() or (self.directory / "run-in-progress.json").exists():
            raise ValueError("Run evidence already exists; inspect it or use a new fixture epoch")
        storage = json.loads((self.directory / "storage.json").read_bytes())
        assert storage["generation"] == self.manifest["generation"]
        self.epoch.start("realtime")
        self.current()
        write_json(self.directory / "run-in-progress.json", dict(input_origin="synthetic fixtures", source="real"))
        jm, tm = self.manifest["containers"]["jobmanager"], self.manifest["containers"]["taskmanager"]
        jar_path = "/opt/flink/usrlib/snow-realtime.jar"
        runtime_hashes = {name: self.docker.command(["exec", name, "sha256sum", jar_path]).decode().split()[0] for name in (jm, tm)}
        assert set(runtime_hashes.values()) == {self.manifest["jar"]["sha256"]}
        with httpx.Client(base_url="http://127.0.0.1:8081", timeout=10, trust_env=False) as flink:
            until(lambda: flink.get("/overview").json().get("taskmanagers") == 1, "Flink ready", 90)
            assert not any(row["state"] not in {"CANCELED", "FINISHED", "FAILED"} for row in flink.get("/jobs/overview").json()["jobs"])
            output = self.docker.command(["exec", "--env-file", str(self.directory / "job.env"), jm,
                                         "/opt/flink/bin/flink", "run", "-d", "-c", "dev.xiaob.snow.RealtimeJob", jar_path], timeout=120)
            match = re.search(rb"JobID\s+([a-f0-9]{32})", output)
            if not match:
                raise RuntimeError("Flink did not return an exact submitted JobID")
            job = match.group(1).decode()
            write_json(self.directory / "job.json", dict(job_id=job, runtime_jar_hashes=runtime_hashes,
                                                        input_origin="synthetic fixtures", source="real"))
            def get(path=""):
                value = flink.get("/jobs/" + job + path)
                value.raise_for_status()
                return value.json()
            until(lambda: get()["state"] == "RUNNING", "job running", 120)
            producer = KafkaProducer(bootstrap_servers=self.host + ":9092", acks="all", retries=2,
                                     value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode())
            try:
                with local_collector(self.directory) as (collector, store):
                    generated = events(self.scope["lane"], datetime.now(UTC), count)
                    pending, samples = {}, []
                    def observe():
                        visible = self.query(f"SELECT app,business_key FROM {self.database}.events_realtime")
                        at = time.monotonic()
                        for key in visible:
                            if key in pending:
                                value = pending.pop(key)
                                samples.append(dict(event_id=value["id"], from_send_seconds=at-value["send"], from_ack_seconds=at-value["ack"]))
                        return not pending
                    def send(rows):
                        for row in rows:
                            producer.send(self.scope["topics"]["events"], partition=0, value=row,
                                          timestamp_ms=int(datetime.fromisoformat(row["accepted_at"]).timestamp() * 1000)).get(timeout=10)
                    def accept(batch):
                        start = time.monotonic()
                        response = collector.post("/analytics/v1/events", json={"events": batch})
                        acknowledgement = time.monotonic()
                        assert response.status_code == 202 and response.json()["accepted"] == len(batch)
                        for row in batch:
                            key = "request:" + row["request_id"] if row["event_type"] == "request_complete" else "event:" + row["event_id"]
                            pending[(row["app"], key)] = dict(id=row["event_id"], send=start, ack=acknowledgement)
                    first = count // 2
                    accept(generated[:first])
                    rows = store.read()["events"]
                    send(rows)
                    until(observe, "first HTTP events visible in Doris", 120)
                    completed = until(lambda: get("/checkpoints")["latest"].get("completed"), "first completed checkpoint", 45)
                    before_id = completed["id"]
                    before = until(lambda: (value if (value := get("/checkpoints"))["latest"].get("completed", {}).get("id", -1) > before_id else None),
                                   "checkpoint after first committed facts", 45)
                    assert before["latest"]["completed"]["external_path"].startswith("file:/")
                    self.current()
                    self.docker.command(["kill", tm])
                    try:
                        accept(generated[first:])
                        all_rows = store.read()["events"]
                        send(rows[:2] + all_rows[first:])  # Lost-ACK replay crosses the actual TM failure.
                    finally:
                        self.current()
                        self.docker.command(["start", tm])
                    until(lambda: get()["state"] == "RUNNING", "TaskManager recovery", 120)
                    until(observe, "all HTTP events after recovery", 120)
                    restored = until(lambda: (value if (value := get("/checkpoints"))["counts"]["restored"] >= 1 else None),
                                     "restored checkpoint counter", 45)
                    assert restored["latest"]["restored"]["id"] >= before["latest"]["completed"]["id"]
                    while store.aggregate():
                        pass
                    expected = oracle(all_rows)
                    with store.lock:
                        lite = [dict(row) for row in store.db.execute("SELECT source,app,day AS date,pv,uv,requests,successes FROM daily ORDER BY source,day,app")]
                    actual = self.metrics()
                    assert actual == lite == expected
                    extra = diagnostics(all_rows, datetime.now(UTC))
                    send(list(extra.values()))
                    sides = until(lambda: (value if len((value := {name: self.side(name) for name in ("duplicates", "quarantine", "late")})["duplicates"]) >= 3
                                          and len(value["quarantine"]) >= 2 and len(value["late"]) >= 1 else None), "three side topics", 45)
                    assert extra["late"]["seq"] in {row["value"].get("seq") for row in sides["late"]}
                    for name in ("late", "duplicates", "quarantine"):
                        for item in sides[name]:
                            value = item["value"]
                            if "event_id" in value or "event" in value:
                                assert value["source"] == "real" and "accepted_at" in value
                                assert item["timestamp_ms"] == int(datetime.fromisoformat(value["accepted_at"]).timestamp() * 1000)
                    assert self.metrics() == expected
                    os.environ["SNOW_DORIS_DATABASE"] = self.database
                    assert publication_database("real") == self.database
                    package = dict(schema_version=1, manifest=dict(run_id=self.manifest["event_lane"], source="real",
                        date_from=min(row["date"] for row in expected), date_to=max(row["date"] for row in expected),
                        cutoff=datetime.now(UTC).isoformat(), quality=dict(raw=count, valid=count, duplicates=0, quarantined=0, after_cutoff=0)), daily=expected)
                    with self.connect() as connection:
                        published = publish(connection, package, self.directory / "publication")
                        assert read_published(connection, "real")["daily"] == expected
                    write_json(self.directory / "fixture-input.json", dict(input_origin="synthetic fixtures", on_time=all_rows, diagnostics=extra))
                    result = dict(input_origin="synthetic fixtures", source_branch="real", production_requests=0,
                                  measurement_scope="local synthetic HTTP acceptance through Kafka/Flink/Doris including controlled TaskManager failure",
                                  epoch_id=self.manifest["epoch_id"], generation=self.manifest["generation"],
                                  event_lane=self.scope["lane"], on_time_events=count, runtime_jar_hashes=runtime_hashes,
                                  integer_metrics=actual, lite_oracle_realtime_equal=True, publication=published,
                                  checkpoint_before=before, checkpoint_restored=restored,
                                  duplicate_delivery="at_least_once side diagnostics; exact SQL integer metrics", sides=sides,
                                  freshness=summarize(samples, count), freshness_is_production_sla_evidence=False,
                                  session_recovery_pending=True, full_session_checkpoint_restore_verified=False)
                    self.pause_session(flink, job)
                    write_json(self.directory / "acceptance.json", result)
                    return result
            finally:
                producer.close(timeout=10)
                try:
                    if get()["state"] not in {"CANCELED", "FINISHED", "FAILED"}:
                        flink.patch("/jobs/" + job, params={"mode": "cancel"}).raise_for_status()
                finally:
                    self.epoch.stop()  # Preserve this fixture's registered engine state for review.


@contextmanager
def local_collector(directory):
    import httpx
    import uvicorn

    from snow_statistics.api import create_app
    from snow_statistics.config import Settings
    from snow_statistics.store import Store
    settings = Settings(mode="full", source="real", db=directory / "local-collector/statistics.db",
                        budget_bytes=8 * 1024**2, reader_token=secrets.token_urlsafe(32), server_token=secrets.token_urlsafe(32),
                        origins=("https://synthetic-engine-fixture.invalid",), aggregate_interval=1,
                        allowed_characters=frozenset({"sample_character"}))
    store = Store(settings)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    url = "http://127.0.0.1:" + str(listener.getsockname()[1])
    server = uvicorn.Server(uvicorn.Config(create_app(settings, store), access_log=False, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        until(lambda: server.started, "isolated loopback collector", 10)
        with httpx.Client(base_url=url, timeout=5, trust_env=False, headers={"Origin": settings.origins[0],
                "Authorization": "Bearer " + settings.server_token}) as client:
            yield client, store
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        store.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--action", choices=["storage", "run", "resume"], required=True)
    parser.add_argument("--events", type=int, default=20)
    args = parser.parse_args()
    task = None
    try:
        task = Acceptance(args.root, args.epoch)
        result = task.storage() if args.action == "storage" else task.run(args.events) if args.action == "run" else task.resume()
        print(json.dumps({key: result[key] for key in ("input_origin",) if key in result} | {"action": args.action, "complete": True}))
    except Exception as error:
        if task:
            stopped = False
            try:
                task.epoch.stop()
                stopped = True
            except Exception:
                pass
            write_json(task.directory / "failure.json", dict(error_class=type(error).__name__, action=args.action,
                                                               input_origin="synthetic fixtures", complete=False,
                                                               owned_readers_stopped=stopped))
        raise SystemExit("Synthetic engine acceptance failed; private metadata retained, no production action") from None


if __name__ == "__main__":
    main()
