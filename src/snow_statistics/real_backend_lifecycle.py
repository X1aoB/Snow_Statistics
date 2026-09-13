"""Actual, scoped backend adapters for the real-data startup permit.

Call with writers stopped. No adapter stops jobs, drops a topic/table, accepts a
passed flag, or uses a broker/backup modification timestamp as event age. Broker
prefix deletion is a logical read barrier; Kafka segment reclamation is separate.
"""
import json
import re
import shutil
import stat
import subprocess
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from .io import digest
from .lifecycle import RETENTION_DAYS, timestamp
from .publication import canonical


def scope(resources, allowed, backend):
    if not resources or set(resources) != set(allowed) or len(resources) > 100:
        raise ValueError("Registered and configured backend scopes must match exactly")
    for name, entry in resources.items():
        kind = entry["kind"]
        if kind not in RETENTION_DAYS or kind != allowed[name]:
            raise ValueError("Backend retention class changed")
        origin = timestamp(entry["original_min_accepted_at"])
        if timestamp(entry["expires_at"]) != origin + timedelta(days=RETENTION_DAYS[kind]):
            raise ValueError("Backend registration extended original retention")
        if backend == "kafka" and (not re.fullmatch(r"snow\.real\.[a-z0-9_.-]{1,150}", name) or ".." in name):
            raise ValueError("Only registered real Kafka topics are allowed")
        if backend == "doris" and not re.fullmatch(r"snow_real_[a-z0-9_]{1,40}\.(events_realtime|daily_offline)", name):
            raise ValueError("Only defined real Doris tables are allowed")


def receipt(backend, resources, now, evidence, expiries):
    checked = datetime.now(UTC)
    # Injected fixed clocks are useful for synthetic tests; actual clients never
    # receive a caller-controlled assertion in lieu of their reads below.
    if checked < now:
        checked = now
    return dict(backend=backend, resources_sha256=digest(canonical(resources)),
                checked_at=checked.isoformat(), remaining_expired=0,
                evidence_sha256=digest(canonical(evidence)), evidence=evidence,
                live_records=len(expiries),
                next_expiry=min(expiries).isoformat() if expiries else None)


class KafkaRetention:
    """Scan a frozen bounded log and remove only an all-expired offset prefix.

    Non-prefix expired replay blocks the permit without deleting newer records.
    The caller must explicitly re-materialize that lane from unexpired ODS data.
    """
    def __init__(self, client, topics, cluster_id, topic_ids, *, max_records=100000):
        if not 1 <= max_records <= 1000000 or not cluster_id or set(topics) != set(topic_ids):
            raise ValueError("Incomplete Kafka ownership or scan bound")
        self.client, self.allowed = client, dict(topics)
        self.identity = dict(cluster_id=cluster_id, topic_ids=dict(topic_ids))
        self.max_records = max_records

    def _scan(self, resources, now):
        if self.client.identity(set(resources)) != self.identity:
            raise ValueError("Kafka cluster/topic incarnation differs from its owner")
        bounds = self.client.bounds(set(resources))
        if not bounds or len(bounds) > 32 or {t for t, _ in bounds} != set(resources):
            raise ValueError("Invalid or missing registered Kafka partitions")
        deletes, expiries, scanned = {}, [], 0
        for key, (start, end) in sorted(bounds.items()):
            if type(start) is not int or type(end) is not int or not 0 <= start <= end:
                raise ValueError("Invalid Kafka log range")
            entry = resources[key[0]]
            if entry["kind"] not in {"raw", "request_detail"}:
                raise ValueError("Kafka envelopes have a seven-day retention class")
            latest, seen_live, cut = start - 1, False, start
            for offset, body in self.client.records(key, start, end):
                scanned += 1
                if scanned > self.max_records or not latest < offset < end or offset < start:
                    raise ValueError("Kafka scan bound or monotonicity violated")
                latest = offset
                if not isinstance(body, bytes) or len(body) > 65536:
                    raise ValueError("Invalid or oversized real Kafka record")
                row = json.loads(body)
                if row.get("source") != "real" or "accepted_at" not in row:
                    raise ValueError("Real Kafka payload lacks original acceptance provenance")
                original = timestamp(row["accepted_at"])
                if original > now or original < timestamp(entry["original_min_accepted_at"]):
                    raise ValueError("Kafka payload predates its registration or is future dated")
                expiry = original + timedelta(days=7)
                if expiry <= now:
                    if seen_live:
                        raise ValueError("Expired replay is not a deletable Kafka prefix")
                    cut = offset + 1
                else:
                    seen_live = True
                    expiries.append(expiry)
            if cut > start:
                deletes[key] = cut
        if self.client.bounds(set(resources)) != bounds or self.client.identity(set(resources)) != self.identity:
            raise ValueError("Kafka writers or topic ownership changed during cleanup")
        return bounds, deletes, expiries, scanned

    def purge_and_verify(self, resources, now):
        scope(resources, self.allowed, "kafka")
        settings = self.client.retention_settings(set(resources))
        if set(settings["topics"]) != set(resources) or not settings["brokers"]:
            raise ValueError("Missing actual Kafka retention configuration")
        for values in settings["topics"].values():
            if (values.get("cleanup.policy") != "delete" or values.get("message.timestamp.type") != "CreateTime"
                    or not 1 <= int(values["retention.ms"]) <= 604800000
                    or not 1 <= int(values["segment.ms"]) <= 60000
                    or not 1 <= int(values["file.delete.delay.ms"]) <= 60000):
                raise ValueError("Real Kafka retention and segment deletion must be finite and bounded")
        if any(not 1 <= int(value["log.retention.check.interval.ms"]) <= 300000
               for value in settings["brokers"].values()):
            raise ValueError("Kafka broker retention checks exceed the allowed bound")
        before, deletes, _, scanned = self._scan(resources, now)
        if deletes:
            self.client.delete_prefixes(deletes)
        after, remaining, expiries, _ = self._scan(resources, now)
        if remaining or any(after[key][0] < cut for key, cut in deletes.items()):
            raise RuntimeError("Kafka deletion readback retained expired offsets")
        if any(after[key][1] != end for key, (_, end) in before.items()):
            raise ValueError("Kafka writes continued across the deletion barrier")
        evidence = dict(identity=self.identity, scanned=scanned, retention_configuration=settings,
                        before={f"{t}:{p}": list(v) for (t, p), v in before.items()},
                        after={f"{t}:{p}": list(v) for (t, p), v in after.items()},
                        deletion_offsets={f"{t}:{p}": v for (t, p), v in deletes.items()},
                        physical_segment_erasure_verified=False)
        return receipt("kafka", resources, now, evidence, expiries)


class KafkaClient:
    """Pinned Kafka 3.9 CLI identity plus kafka-python 2.2.15 bounded reads."""
    def __init__(self, bootstrap, container):
        from kafka import KafkaAdminClient, KafkaConsumer
        if not re.fullmatch(r"[A-Za-z0-9.-]+:9092", bootstrap) or (container not in {
            "snow-lab-control-kafka-1", "snow-lab-realtime-kafka-1"
        } and not re.fullmatch(r"snow-real-[a-z][a-z0-9-]{2,31}-kafka", container)):
            raise ValueError("Unknown Kafka host or owned broker container")
        self.bootstrap, self.container = bootstrap, container
        self.admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                                      group_id=None, auto_offset_reset="none", allow_auto_create_topics=False,
                                      max_partition_fetch_bytes=1048576, fetch_max_bytes=2097152)

    def identity(self, topics):
        # Topic IDs prevent deleting a new topic that reused an old registered name.
        if self.container.startswith("snow-real-"):
            metadata = subprocess.run(["sudo", "docker", "inspect", "--format", "{{json .Config.Labels}}", self.container],
                                      capture_output=True, check=True, timeout=10)
            owned = json.loads(metadata.stdout)
            if (owned.get("org.snow-statistics.owner") != "Snow_Statistics" or owned.get("org.snow-statistics.source") != "real"
                    or self.container != "snow-real-" + owned.get("org.snow-statistics.epoch", "") + "-kafka"):
                raise ValueError("Kafka container does not belong to the registered real epoch")
        pattern = "(" + "|".join(re.escape(name) for name in sorted(topics)) + ")"
        result = subprocess.run(["sudo", "docker", "exec", self.container, "/opt/kafka/bin/kafka-topics.sh",
                                 "--bootstrap-server", self.bootstrap, "--describe", "--topic", pattern],
                                capture_output=True, check=True, timeout=30)
        if len(result.stdout) > 65536:
            raise ValueError("Kafka identity response exceeded metadata bound")
        ids = dict(re.findall(r"Topic:\s+(\S+)\s+TopicId:\s+(\S+)", result.stdout.decode()))
        return dict(cluster_id=self.admin.describe_cluster()["cluster_id"], topic_ids=ids)

    @staticmethod
    def partition(key):
        from kafka import TopicPartition
        return TopicPartition(*key)

    def bounds(self, topics):
        parts = []
        for topic in sorted(topics):
            ids = self.consumer.partitions_for_topic(topic)
            if not ids:
                raise ValueError("Missing registered Kafka topic")
            parts.extend(self.partition((topic, p)) for p in sorted(ids))
        self.consumer.assign(parts)
        starts, ends = self.consumer.beginning_offsets(parts), self.consumer.end_offsets(parts)
        return {(p.topic, p.partition): (starts[p], ends[p]) for p in parts}

    def records(self, key, start, end):
        tp = self.partition(key)
        self.consumer.assign([tp])
        self.consumer.seek(tp, start)
        deadline = time.monotonic() + 60
        while self.consumer.position(tp) < end:
            if time.monotonic() >= deadline:
                raise TimeoutError("Kafka bounded scan timed out")
            for row in self.consumer.poll(timeout_ms=500, max_records=500).get(tp, []):
                if row.offset >= end:
                    raise ValueError("Kafka writer advanced during lifecycle scan")
                yield row.offset, row.value

    def delete_prefixes(self, offsets):
        self.admin.delete_records({self.partition(k): v for k, v in offsets.items()}, timeout_ms=10000)

    def retention_settings(self, topics):
        from kafka.admin import ConfigResource, ConfigResourceType
        topic_keys = {name: None for name in ("retention.ms", "segment.ms", "file.delete.delay.ms",
                                              "cleanup.policy", "message.timestamp.type")}
        cluster = self.admin.describe_cluster()
        requests = [ConfigResource(ConfigResourceType.TOPIC, name, topic_keys) for name in sorted(topics)]
        requests += [ConfigResource(ConfigResourceType.BROKER, str(row["node_id"]),
                                    {"log.retention.check.interval.ms": None}) for row in cluster["brokers"]]
        return self.decode_settings(self.admin.describe_configs(requests))

    @staticmethod
    def decode_settings(responses):
        result = {"topics": {}, "brokers": {}}
        for response in responses:
            for item in response.to_object()["resources"]:
                if item["error_code"]:
                    raise ValueError("Cannot inspect actual Kafka retention settings")
                if item["resource_type"] not in (2, 4):
                    raise ValueError("Unexpected Kafka configuration resource type")
                group = "topics" if item["resource_type"] == 2 else "brokers"
                # kafka-python's pinned protocol schema spells this plural.
                result[group][item["resource_name"]] = {row["config_names"]: row["config_value"] for row in item["config_entries"]}
        return result

    def close(self):
        self.consumer.close(autocommit=False)
        self.admin.close()


class DorisRetention:
    """Delete using source acceptance time, then verify exact real table counts."""
    def __init__(self, connection, tables):
        self.connection, self.allowed = connection, dict(tables)

    def _query(self, sql, values=()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchall()

    def purge_and_verify(self, resources, now):
        scope(resources, self.allowed, "doris")
        expiries, evidence = [], {}
        for table, entry in sorted(resources.items()):
            detail = table.endswith(".events_realtime")
            if entry["kind"] != ("raw" if detail else "aggregate"):
                raise ValueError("Doris table does not match its defined retention model")
            column, days = ("accepted_at", 7) if detail else ("business_date", 90)
            # Doris DATETIMEV2 fields store UTC. Aggregate business dates are HK.
            threshold = now - timedelta(days=days)
            if not detail:
                from zoneinfo import ZoneInfo
                threshold = threshold.astimezone(ZoneInfo("Asia/Hong_Kong"))
                threshold = threshold.date()
            else:
                threshold = threshold.replace(tzinfo=None)
            schema = self._query("SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                                 tuple(table.split(".")))
            if len(schema) != 1 or schema[0][0] != "BASE TABLE":
                raise ValueError("Registered Doris resource is absent or is not a base table")
            invalid = self._query(f"SELECT COUNT(*) FROM {table} WHERE source IS NULL OR source<>%s OR {column} IS NULL", ("real",))[0][0]
            if invalid:
                raise ValueError("Mixed-source or unaged Doris rows block cleanup")
            earliest, latest = self._query(f"SELECT MIN({column}),MAX({column}) FROM {table}")[0]
            if earliest is not None:
                if detail:
                    origin = earliest.replace(tzinfo=UTC)
                    upper = latest.replace(tzinfo=UTC)
                else:
                    from zoneinfo import ZoneInfo
                    origin = datetime.combine(earliest, datetime.min.time(), ZoneInfo("Asia/Hong_Kong")).astimezone(UTC)
                    upper = datetime.combine(latest, datetime.min.time(), ZoneInfo("Asia/Hong_Kong")).astimezone(UTC)
                # DATETIMEV2(3) truncates sub-millisecond source precision. This
                # allowance can only retire a row earlier, never extend its age.
                precision = timedelta(milliseconds=1) if detail else timedelta(0)
                if origin + precision < timestamp(entry["original_min_accepted_at"]) or upper > now:
                    raise ValueError("Doris source range differs from original registered provenance")
            before = self._query(f"SELECT COUNT(*) FROM {table} WHERE {column}<=%s", (threshold,))[0][0]
            if before:
                self._query(f"DELETE FROM {table} WHERE source=%s AND {column}<=%s", ("real", threshold))
            remaining, live, oldest = self._query(
                f"SELECT SUM(IF({column}<=%s,1,0)),COUNT(*),MIN({column}) FROM {table}", (threshold,))[0]
            if remaining:
                raise RuntimeError("Doris readback still contains expired records")
            if live:
                if detail:
                    original = oldest.replace(tzinfo=UTC) if isinstance(oldest, datetime) else timestamp(str(oldest) + "+00:00")
                else:
                    from zoneinfo import ZoneInfo
                    original = datetime.combine(oldest, datetime.min.time(), ZoneInfo("Asia/Hong_Kong")).astimezone(UTC)
                precision = timedelta(milliseconds=1) if detail else timedelta(0)
                if original + precision < timestamp(entry["original_min_accepted_at"]) or original > now:
                    raise ValueError("Doris original provenance differs from registration")
                expiry = original + timedelta(days=days)
                if expiry <= now:
                    raise ValueError("Doris next expiry is already closed")
                expiries.append(expiry)
            evidence[table] = dict(deleted_rows=int(before), live_rows=int(live), logical_readback=True,
                                   physical_compaction_erasure_verified=False)
        result = receipt("doris", resources, now, evidence, expiries)
        result["live_records"] = sum(item["live_rows"] for item in evidence.values())
        return result


class CheckpointRetention:
    """Whole immutable checkpoint roots expire from their oldest source record."""
    def __init__(self, files, flink, paths):
        self.files, self.flink, self.allowed = files, flink, dict(paths)

    def purge_and_verify(self, resources, now):
        scope(resources, self.allowed, "checkpoint")
        if any(value["kind"] != "raw" for value in resources.values()):
            raise ValueError("Checkpoint buffers require conservative seven-day retention")
        self.flink.require_quiescent()
        # Inventory also rejects files not below a registered checkpoint path.
        self.files.inventory(set(resources))
        evidence, expiries = {}, []
        for path, entry in sorted(resources.items()):
            expired = timestamp(entry["expires_at"]) <= now
            if expired and self.files.exists(path):
                self.files.delete_exact(path)
            exists = self.files.exists(path)
            if expired and exists:
                raise RuntimeError("Expired checkpoint remains after exact deletion")
            if exists:
                expiries.append(timestamp(entry["expires_at"]))
            evidence[path] = dict(expired=expired, exists=exists)
        self.flink.require_quiescent()
        self.files.inventory(set(resources))
        return receipt("checkpoint", resources, now, evidence, expiries)


class FlinkReadBarrier:
    def __init__(self, url, job_ids):
        import httpx
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.port != 8081 or parsed.path not in ("", "/") or parsed.username:
            raise ValueError("Use this lab's explicit Flink REST endpoint")
        if not job_ids or any(not re.fullmatch(r"[a-f0-9]{32}", item) for item in job_ids):
            raise ValueError("Checkpoint ownership needs exact Flink job IDs")
        self.client = httpx.Client(base_url=url.rstrip("/"), timeout=10, trust_env=False, follow_redirects=False)
        self.jobs = set(job_ids)

    def require_quiescent(self):
        response = self.client.get("/jobs/overview")
        response.raise_for_status()
        if len(response.content) > 1048576:
            raise ValueError("Flink inventory exceeds bound")
        rows = response.json()["jobs"]
        terminal = {"FINISHED", "CANCELED", "FAILED"}
        for row in rows:
            if row["jid"] in self.jobs or row.get("name", "").startswith("Snow Statistics real "):
                if row["state"] not in terminal:
                    raise ValueError("Stop real Flink jobs before checkpoint lifecycle cleanup")
        # An absent history entry is normal after a JobManager restart. Startup
        # restoration is still gated by the expiry-bound permit, never by mtime.

    def close(self):
        self.client.close()


class CheckpointFiles:
    def __init__(self, roots, hdfs):
        self.roots, self.hdfs = tuple(roots), hdfs
        if not self.roots or len(self.roots) > 50:
            raise ValueError("Expected bounded exact checkpoint roots")
        for root in self.roots:
            if not re.fullmatch(r"(?:hdfs://[A-Za-z0-9.-]+:9000/snow/checkpoints/real/|/opt/snow/runtime/real/checkpoints/)[A-Za-z0-9_-]{1,100}", root):
                raise ValueError("Checkpoint root is outside the dedicated real namespace")

    def _owned(self, value):
        if not any(value.startswith(root + "/") for root in self.roots) or ".." in value or "\\" in value:
            raise ValueError("Checkpoint path escaped registered roots")
        if value.startswith("hdfs://"):
            self.hdfs.path(value)  # Validate the configured NameNode before reads/deletes.
            return None
        path = Path(value)
        if path.resolve() != path.absolute():
            raise ValueError("Checkpoint contains a filesystem link")
        return path

    def exists(self, value):
        path = self._owned(value)
        return self.hdfs.exists(value) if path is None else path.exists()

    def children(self, value):
        if value.startswith("hdfs://"):
            return self.hdfs.children(value)
        path = Path(value)
        if path.resolve() != path.absolute():
            raise ValueError("Checkpoint root contains a filesystem link")
        if not path.exists():
            return []
        result = []
        for child in path.iterdir():
            if child.is_symlink() or child.resolve() != child.absolute():
                raise ValueError("Checkpoint tree contains a link")
            if not child.is_file() and not child.is_dir():
                raise ValueError("Unexpected checkpoint object")
            result.append((child.as_posix(), "DIRECTORY" if child.is_dir() else "FILE"))
        return result

    def inventory(self, registered):
        for value in registered:
            self._owned(value)
        if any(a != b and a.startswith(b + "/") for a in registered for b in registered):
            raise ValueError("Overlapping checkpoint roots")
        pending, count = list(self.roots), 0
        while pending:
            parent = pending.pop()
            for child, kind in self.children(parent):
                count += 1
                if count > 10000:
                    raise ValueError("Checkpoint inventory exceeds bound")
                owned = any(child == key or child.startswith(key + "/") for key in registered)
                if kind == "DIRECTORY":
                    pending.append(child)
                elif not owned:
                    raise ValueError("Unregistered checkpoint payload blocks cleanup")

    def delete_exact(self, value):
        path = self._owned(value)
        if path is None:
            self.hdfs.delete_exact(value)
        elif path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
        if self.exists(value):
            raise RuntimeError("Checkpoint deletion readback failed")


@contextmanager
def backend_adapters(config_file, hdfs):
    """Build real adapters from a small private operator allowlist, not a receipt.

    The connection credentials stay in memory; cleanup never prints the config.
    Caller supplies the same pinned-host WebHdfsOwned used for ODS cleanup.
    """
    path = Path(config_file)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > 65536 or path.resolve() != path.absolute():
        raise ValueError("Expected a bounded regular backend configuration file")
    if hasattr(info, "st_uid") and __import__("os").name == "posix" and info.st_mode & 0o077:
        raise ValueError("Backend connection configuration must be private")
    config = json.loads(path.read_bytes())
    if set(config) - {"kafka", "doris", "checkpoint"}:
        raise ValueError("Unknown backend configuration")
    adapters, clients = {}, []
    try:
        if "kafka" in config:
            value = config["kafka"]
            client = KafkaClient(value["bootstrap"], value["container"])
            clients.append(client)
            adapters["kafka"] = KafkaRetention(client, value["topics"], value["cluster_id"], value["topic_ids"],
                                                 max_records=value.get("max_records", 100000))
        if "doris" in config:
            import pymysql
            value = config["doris"]
            if not re.fullmatch(r"snow_real_[a-z0-9_]{1,40}", value["user"]):
                raise ValueError("Use a separately permissioned real-data Doris account")
            client = pymysql.connect(host=value["host"], port=9030, user=value["user"], password=value["password"],
                                     autocommit=True, connect_timeout=10, read_timeout=30, write_timeout=30)
            clients.append(client)
            adapters["doris"] = DorisRetention(client, value["tables"])
        if "checkpoint" in config:
            value = config["checkpoint"]
            client = FlinkReadBarrier(value["flink_url"], value["job_ids"])
            clients.append(client)
            adapters["checkpoint"] = CheckpointRetention(CheckpointFiles(value["roots"], hdfs), client, value["paths"])
        yield adapters
    finally:
        for client in reversed(clients):
            client.close()
