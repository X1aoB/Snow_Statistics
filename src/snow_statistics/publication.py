"""Validated daily snapshots and one atomic publication pointer update in Doris.

Only the private lab uses this module. All publishers share one lock directory;
Airflow additionally limits active runs to one. Snapshot rows are never the UI API.
"""
import hashlib
import json
import os
import re
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

METRICS = ("pv", "uv", "requests", "successes")
ROW_FIELDS = {"source", "app", "date", *METRICS}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def validate(package):
    if set(package) != {"schema_version", "manifest", "daily"} or package["schema_version"] != 1:
        raise ValueError("Invalid publication package")
    manifest, rows = package["manifest"], package["daily"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", manifest["run_id"]):
        raise ValueError("Invalid run ID")
    if manifest["source"] not in ("synthetic", "real") or manifest["quality"]["quarantined"] != 0:
        raise ValueError("Source or quality gate failed")
    quality = manifest["quality"]
    if any(type(quality.get(k)) is not int or quality[k] < 0 for k in ("raw", "valid", "duplicates", "quarantined", "after_cutoff")):
        raise ValueError("Invalid quality counts")
    if quality["raw"] != sum(quality[k] for k in ("valid", "duplicates", "quarantined", "after_cutoff")):
        raise ValueError("Unreconciled input")
    start, end = date.fromisoformat(manifest["date_from"]), date.fromisoformat(manifest["date_to"])
    if not start <= end or (end - start).days > 365:
        raise ValueError("Invalid bounded publication window")
    cutoff = datetime.fromisoformat(manifest["cutoff"].replace("Z", "+00:00"))
    if cutoff.tzinfo is None:
        raise ValueError("Cutoff must have timezone")
    version = int(cutoff.timestamp() * 1_000_000)
    seen = set()
    for row in rows:
        if set(row) != ROW_FIELDS or row["source"] != manifest["source"] or row["app"] not in ("mywebsite", "project_snow"):
            raise ValueError("Unexpected daily fields/source/application")
        if not start <= date.fromisoformat(row["date"]) <= end:
            raise ValueError("Daily row outside declared window")
        key = (row["app"], row["date"])
        if key in seen:
            raise ValueError("Duplicate daily grain")
        seen.add(key)
        if any(type(row[k]) is not int or row[k] < 0 for k in METRICS) or row["successes"] > row["requests"]:
            raise ValueError("Invalid integer metrics")
    rows = sorted(rows, key=lambda row: (row["date"], row["app"]))
    dates = [str(start + timedelta(days=i)) for i in range((end - start).days + 1)]
    hashes = {day: hashlib.sha256(canonical([r for r in rows if r["date"] == day])).hexdigest() for day in dates}
    return manifest, rows, hashes, version, cutoff.astimezone(UTC).replace(tzinfo=None)


@contextmanager
def publication_lock(directory):
    """OS lock releases on process death. Keep the shared lock file, never unlink it."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "publisher.lock").open("a+b") as stream:
        if os.name == "nt":
            import msvcrt
            if stream.tell() == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def connect():
    import pymysql
    return pymysql.connect(host=os.environ.get("SNOW_DORIS_HOST", "127.0.0.1"),
                           port=int(os.environ.get("SNOW_DORIS_PORT", "9030")),
                           user=os.environ.get("DORIS_USER", "root"), password=os.environ.get("DORIS_PASSWORD", ""),
                           autocommit=True, connect_timeout=5, read_timeout=60, write_timeout=60)


def publication_database(source, override=None):
    database = override if override is not None else os.environ.get("SNOW_DORIS_DATABASE", "snow_real_warehouse" if source == "real" else "snow")
    if not re.fullmatch(r"snow(?:_[a-z0-9_]{1,40})?", database):
        raise ValueError("Invalid publication database")
    if source == "real" and not re.fullmatch(r"snow_real_[a-z0-9_]{1,30}", database):
        raise ValueError("Real publication requires an isolated snow_real_* database")
    if source == "synthetic" and database.startswith("snow_real_"):
        raise ValueError("Synthetic publication cannot use a real database")
    return database


def publish(db, package, lock_directory, fail_after_load=False, *, database=None):
    manifest, rows, hashes, version, cutoff = validate(package)
    run_id, source = manifest["run_id"], manifest["source"]
    database = publication_database(source, database)
    # Content-addressed ID makes retries identical and prevents run ID reuse mutating old snapshots.
    snapshot = hashlib.sha256(canonical({"daily": rows, "hashes": hashes, "cutoff": manifest["cutoff"]})).hexdigest()
    with publication_lock(lock_directory), db.cursor() as cursor:
        cursor.execute(f"SELECT business_date,business_version,content_hash FROM {database}.offline_releases WHERE source=%s AND business_date BETWEEN %s AND %s",
                       (source, manifest["date_from"], manifest["date_to"]))
        for day, old_version, old_hash in cursor.fetchall():
            if old_version > version:
                raise ValueError("Refusing to replace a newer cutoff")
            if old_version == version and old_hash != hashes[str(day)]:
                raise ValueError("Same cutoff produced conflicting results; investigate inputs")
        if rows:
            cursor.executemany(f"INSERT INTO {database}.daily_snapshots VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                               [(snapshot, source, r["app"], r["date"], *(r[k] for k in METRICS)) for r in rows])
        cursor.execute(f"SELECT app,business_date,pv,uv,requests,successes FROM {database}.daily_snapshots WHERE snapshot_id=%s AND source=%s",
                       (snapshot, source))
        actual = sorted((app, str(day), *map(int, metrics)) for app, day, *metrics in cursor.fetchall())
        expected = sorted((r["app"], r["date"], *(r[k] for k in METRICS)) for r in rows)
        if actual != expected:
            raise ValueError("Doris snapshot readback mismatch")
        if fail_after_load:
            raise RuntimeError("Injected failure before publication")
        # One INSERT statement is the commit point for the whole correction window.
        # Empty days also get a pointer: corrections can remove all prior rows safely.
        values = [(source, day, snapshot, run_id, digest, version, cutoff) for day, digest in hashes.items()]
        cursor.execute(f"INSERT INTO {database}.offline_releases VALUES " + ",".join(["(%s,%s,%s,%s,%s,%s,%s)"] * len(values)),
                       tuple(value for row in values for value in row))
    return {"run_id": run_id, "snapshot_id": snapshot, "source": source, "daily_rows": len(rows),
            "published_dates": len(hashes), "business_version": version, "readback_equal": True}


def read_published(db, source="synthetic", *, database=None):
    if source not in ("synthetic", "real"):
        raise ValueError("Invalid source")
    database = publication_database(source, database)
    with db.cursor() as cursor:
        # Read rows and provenance in one statement so a concurrent release cannot
        # label an old result with a new cutoff between two separate queries.
        cursor.execute(f"""SELECT source,business_date,run_id,cutoff,app,pv,uv,requests,successes
            FROM {database}.report_published WHERE source=%s ORDER BY business_date,app""", (source,))
        daily, releases = [], {}
        for s, day, run, cutoff, app, *metrics in cursor.fetchall():
            releases[str(day)] = dict(date=str(day), run_id=run, cutoff=str(cutoff))
            if app is not None:
                daily.append(dict(zip(("source", "app", "date", *METRICS), (s, app, str(day), *map(int, metrics)), strict=True)))
    return {"daily": daily, "releases": list(releases.values())}
