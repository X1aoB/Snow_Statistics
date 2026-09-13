"""One SQLite writer; WAL + FULL acknowledgement, atomic aggregation cursor.

Run one service process. Local warehouse clients only see the read API.
"""
import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from uuid import uuid4

from . import public_projection
from .config import Settings
from .contracts import Event, PublicSummary, Summary, business_day, instant, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT OR IGNORE INTO meta VALUES('schema_version','1'),('cursor','0'),('expired_through','0');
CREATE TABLE IF NOT EXISTS events(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, app TEXT NOT NULL,
 event_id TEXT NOT NULL, accepted_at TEXT NOT NULL, occurred_at TEXT NOT NULL,
 day TEXT NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS events_accepted ON events(accepted_at);
CREATE TABLE IF NOT EXISTS seen(
 source TEXT NOT NULL, app TEXT NOT NULL, event_id TEXT NOT NULL,
 digest TEXT NOT NULL, accepted_at TEXT NOT NULL, PRIMARY KEY(source,app,event_id));
CREATE TABLE IF NOT EXISTS requests_seen(
 source TEXT NOT NULL, app TEXT NOT NULL, request_id TEXT NOT NULL,
 day TEXT NOT NULL, accepted_at TEXT NOT NULL, PRIMARY KEY(source,app,request_id));
CREATE TABLE IF NOT EXISTS visitors(
 source TEXT NOT NULL, app TEXT NOT NULL, day TEXT NOT NULL, anonymous_id TEXT NOT NULL,
 PRIMARY KEY(source,app,day,anonymous_id));
CREATE TABLE IF NOT EXISTS daily(
 source TEXT NOT NULL, app TEXT NOT NULL, day TEXT NOT NULL,
 pv INTEGER NOT NULL DEFAULT 0, uv INTEGER NOT NULL DEFAULT 0,
 requests INTEGER NOT NULL DEFAULT 0, successes INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(source,app,day));
CREATE TABLE IF NOT EXISTS popularity(
 source TEXT NOT NULL, app TEXT NOT NULL, day TEXT NOT NULL,
 kind TEXT NOT NULL, name TEXT NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(source,app,day,kind,name));
CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
"""


class StorageFull(Exception):
    pass


class EventConflict(Exception):
    pass


class CursorExpired(Exception):
    pass


class AggregateGap(Exception):
    """Expired, unaggregated input needs an explicit audited recovery."""
    pass


class Store:
    def __init__(self, settings: Settings, clock=utcnow):
        self.settings, self.clock = settings, clock
        self.lock = threading.RLock()
        settings.db.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(settings.db, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.execute("PRAGMA wal_autocheckpoint=128")
        self.db.execute("PRAGMA journal_size_limit=1048576")
        # Bound database + worst-case WAL to the configured total. A quota volume is
        # the final physical hard limit; preflight leaves room for aggregate writes.
        self.db.execute(f"PRAGMA max_page_count={max(128, (settings.budget_bytes // 2) // 4096)}")
        self.db.executescript(SCHEMA)
        self.db.executescript(public_projection.SCHEMA)
        # Stable through ordinary restart/SQLite backup; a rebuilt database gets
        # a new generation. No identifiers are exposed by public summaries.
        with self.transaction() as db:
            for key, value in (("instance_id", str(uuid4())), ("generation", str(uuid4())),
                               ("public_start_day", business_day(self.clock()))):
                db.execute("INSERT OR IGNORE INTO meta VALUES(?,?)", (key, value))

    def close(self):
        with self.lock:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.db.close()

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise

    def disk_usage(self):
        return sum(p.stat().st_size for p in self.settings.db.parent.iterdir() if p.is_file())

    def ingest(self, events: list[Event]):
        now = self.clock()
        for e in events:
            if not now - timedelta(days=7) <= e.occurred_at <= now + timedelta(minutes=5):
                raise ValueError("timestamp outside acceptance window")
            if e.path is not None and e.path not in self.settings.allowed_paths:
                raise ValueError("path not allowed")
            if e.character_id is not None and e.character_id not in self.settings.allowed_characters:
                raise ValueError("character not allowed")
        accepted = duplicate = 0
        with self.lock:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            # Conservative allocation reservation; bounded batch and field sizes.
            reserve = 512 * 1024 + len(events) * 16 * 1024
            if self.disk_usage() + reserve > self.settings.budget_bytes:
                raise StorageFull()
            try:
                with self.transaction() as db:
                    for e in events:
                        payload = e.model_dump_json(exclude_none=True)
                        digest = hashlib.sha256(payload.encode()).hexdigest()
                        key = (self.settings.source, e.app, str(e.event_id))
                        old = db.execute("SELECT digest FROM seen WHERE source=? AND app=? AND event_id=?", key).fetchone()
                        if old:
                            if old[0] != digest:
                                raise EventConflict()
                            duplicate += 1
                            continue
                        db.execute("INSERT INTO seen VALUES(?,?,?,?,?)", (*key, digest, instant(now)))
                        db.execute("INSERT INTO events(source,app,event_id,accepted_at,occurred_at,day,payload) VALUES(?,?,?,?,?,?,?)",
                                   (*key, instant(now), instant(e.occurred_at), business_day(e.occurred_at), payload))
                        accepted += 1
            except sqlite3.OperationalError as error:
                if error.sqlite_errorcode == sqlite3.SQLITE_FULL:
                    raise StorageFull() from None
                raise
        return {"accepted": accepted, "duplicates": duplicate}

    def aggregate(self, limit=2000, fail_before_commit=False):
        with self.transaction() as db:
            if self._aggregate_gap(db):
                raise AggregateGap("Unaggregated events expired; explicit recovery required")
            cursor = int(db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
            rows = db.execute("SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (cursor, limit)).fetchall()
            for row in rows:
                e = json.loads(row["payload"])
                key = (row["source"], row["app"], row["day"])
                pv = uv = requests = successes = 0
                if e["event_type"] == "request_complete":
                    requests = db.execute("INSERT OR IGNORE INTO requests_seen VALUES(?,?,?,?,?)",
                                          (row["source"], row["app"], e["request_id"], row["day"], row["accepted_at"])).rowcount
                    if not requests:
                        cursor = row["seq"]
                        continue
                    successes = int(e["success"])
                db.execute("INSERT OR IGNORE INTO daily(source,app,day) VALUES(?,?,?)", key)
                if e["event_type"] == "page_view":
                    pv = 1
                if e.get("anonymous_id") and e["event_type"] != "request_complete":
                    uv = db.execute("INSERT OR IGNORE INTO visitors VALUES(?,?,?,?)", (*key, e["anonymous_id"])).rowcount
                db.execute("UPDATE daily SET pv=pv+?,uv=uv+?,requests=requests+?,successes=successes+? WHERE source=? AND app=? AND day=?",
                           (pv, uv, requests, successes, *key))
                kind, name = ("page", e.get("path")) if pv else ("character", e.get("character_id") if e["event_type"] == "character_select" else None)
                if name:
                    db.execute("INSERT INTO popularity VALUES(?,?,?,?,?,1) ON CONFLICT(source,app,day,kind,name) DO UPDATE SET count=count+1", (*key, kind, name))
                public_projection.accumulate(db, row, e, pv, requests, successes)
                cursor = row["seq"]
            db.execute("UPDATE meta SET value=? WHERE key='cursor'", (str(cursor),))
            # Publish only a complete accepted prefix; never mark a partial backlog fresh.
            pending = db.execute("SELECT 1 FROM events WHERE seq>? LIMIT 1", (cursor,)).fetchone()
            if not pending:
                payload = self._summary(db)
                db.execute("INSERT INTO snapshots VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (payload.model_dump_json(),))
            if fail_before_commit:
                raise RuntimeError("injected aggregate failure")
        # A failed public publication cannot roll back exact aggregation. The
        # separate snapshot transaction keeps its previous content and timestamp.
        if not pending:
            self.publish_public()
        return len(rows)

    def _summary(self, db):
        rows = db.execute("SELECT app,day AS date,pv,uv,requests,successes FROM daily WHERE source='real' ORDER BY day,app").fetchall()
        daily = [dict(r) | {"success_rate": r["successes"] / r["requests"] if r["requests"] else None} for r in rows]
        popularity = [dict(r) for r in db.execute("SELECT app,day AS date,kind,name,count FROM popularity WHERE source='real' ORDER BY day,app,kind,name")]
        return Summary(generated_at=instant(self.clock()), date_from=daily[0]["date"] if daily else None,
                       date_to=daily[-1]["date"] if daily else None, status="ok" if daily else "empty",
                       daily=daily, popularity=popularity)

    def summary(self, archived=False):
        """Exact internal snapshot. HTTP exposure requires the reader token."""
        with self.lock:
            row = self.db.execute("SELECT payload FROM snapshots WHERE id=1").fetchone()
        if not row:
            return Summary(generated_at=None, date_from=None, date_to=None, status="archived" if archived else "unavailable", daily=[], popularity=[])
        result = Summary.model_validate_json(row[0])
        earliest = business_day(self.clock() - timedelta(days=89))
        result.daily = [day for day in result.daily if day.date >= earliest]
        result.popularity = [day for day in result.popularity if day.date >= earliest]
        result.date_from = result.daily[0].date if result.daily else None
        result.date_to = result.daily[-1].date if result.daily else None
        with self.lock:
            gap = self._aggregate_gap(self.db)
        if gap:
            result.status = "unavailable"
        elif archived:
            result.status = "archived"
        elif self.clock() - datetime.fromisoformat(result.generated_at) > timedelta(minutes=3):
            result.status = "stale"
        return result

    def publish_public(self, fail_before_commit=False):
        with self.transaction() as db:
            if self._aggregate_gap(db):
                raise AggregateGap("Unaggregated events expired; public refresh blocked")
            cursor = int(db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
            if db.execute("SELECT 1 FROM events WHERE seq>? LIMIT 1", (cursor,)).fetchone():
                return False
            return public_projection.publish(db, self.clock(), fail_before_commit=fail_before_commit)

    def public_summary_v2(self, archived=False):
        with self.lock:
            row = self.db.execute("SELECT payload FROM public_snapshots WHERE id=1").fetchone()
        if not row:
            return PublicSummary(generated_at=None, cutoff_at=None, date_from=None, date_to=None,
                                 status="archived" if archived else "unavailable", daily=[])
        result = PublicSummary.model_validate_json(row[0])
        # A failed refresh must not keep expired aggregates publicly readable.
        earliest = business_day(self.clock() - timedelta(days=89))
        result.daily = [day for day in result.daily if day.date >= earliest]
        result.date_from = result.daily[0].date if result.daily else None
        result.date_to = result.daily[-1].date if result.daily else None
        with self.lock:
            gap = self._aggregate_gap(self.db)
        if gap:
            result.status = "unavailable"
        elif archived:
            result.status = "archived"
        elif self.clock() - datetime.fromisoformat(result.generated_at) > timedelta(hours=26):
            result.status = "stale"
        return result

    def public_summary_v1(self, archived=False):
        return public_projection.legacy_projection(self.public_summary_v2(archived))

    def sync_status(self):
        with self.lock:
            meta = dict(self.db.execute("SELECT key,value FROM meta"))
            expired = self._read_boundary(self.db)
            earliest = self.db.execute("SELECT MIN(seq) FROM events WHERE seq>?", (expired,)).fetchone()[0]
            sequence = self.db.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
            gap = self._aggregate_gap(self.db)
        return {"schema_version": 1, "source": self.settings.source,
                "instance_id": meta["instance_id"], "generation": meta["generation"],
                "earliest_available_seq": earliest, "latest_accepted_seq": sequence[0] if sequence else 0,
                "expired_through": expired, "aggregate_cursor": int(meta["cursor"]), "aggregate_gap": gap,
                "generated_at": instant(self.clock())}

    def _read_boundary(self, db):
        stored = int(db.execute("SELECT value FROM meta WHERE key='expired_through'").fetchone()[0])
        physical = db.execute("SELECT MAX(seq) FROM events WHERE accepted_at<=?",
                              (instant(self.clock() - timedelta(days=7)),)).fetchone()[0]
        return max(stored, physical or 0)

    def _aggregate_gap(self, db):
        receipt = db.execute("SELECT value FROM meta WHERE key='aggregate_gap'").fetchone()
        old = json.loads(receipt[0]) if receipt else None
        cursor = int(db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
        count, first, last = db.execute("SELECT COUNT(*),MIN(seq),MAX(seq) FROM events WHERE seq>? AND accepted_at<=?",
                                       (cursor, instant(self.clock() - timedelta(days=7)))).fetchone()
        if not count:
            return old
        return {"first_seq": min(first, old["first_seq"]) if old else first,
                "last_seq": max(last, old["last_seq"]) if old else last,
                "expired_events": count + (old["expired_events"] if old else 0),
                "reason": "unaggregated_input_expired", "recovery_required": True}

    def read(self, after=0, limit=500):
        with self.lock:
            expired = self._read_boundary(self.db)
            if after < expired:
                raise CursorExpired()
            rows = self.db.execute("SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (after, limit)).fetchall()
            return {"schema_version": 1, "events": [
                {"seq": r["seq"], "source": r["source"], "accepted_at": r["accepted_at"], "event": json.loads(r["payload"])} for r in rows
            ], "next_cursor": rows[-1]["seq"] if rows else after}

    def maintain(self):
        now = self.clock()
        with self.transaction() as db:
            gap = self._aggregate_gap(db)
            if gap:
                db.execute("INSERT INTO meta VALUES('aggregate_gap',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                           (json.dumps(gap),))
            end = self._read_boundary(db)
            # Expiry is independent of aggregation health. A durable gap receipt
            # blocks later publication instead of retaining raw input forever.
            db.execute("DELETE FROM events WHERE accepted_at<=?", (instant(now - timedelta(days=7)),))
            db.execute("UPDATE meta SET value=CAST(MAX(CAST(value AS INTEGER),?) AS TEXT) WHERE key='expired_through'", (end,))
            db.execute("DELETE FROM seen WHERE accepted_at<?", (instant(now - timedelta(days=30)),))
            db.execute("DELETE FROM requests_seen WHERE accepted_at<?", (instant(now - timedelta(days=30)),))
            db.execute("DELETE FROM visitors WHERE day<?", (business_day(now - timedelta(days=29)),))
            for table in ("daily", "popularity"):
                db.execute(f"DELETE FROM {table} WHERE day<?", (business_day(now - timedelta(days=89)),))
            cached = db.execute("SELECT payload FROM snapshots WHERE id=1").fetchone()
            if cached:
                old = Summary.model_validate_json(cached[0])
                first = business_day(now - timedelta(days=89))
                old.daily = [row for row in old.daily if row.date >= first]
                old.popularity = [row for row in old.popularity if row.date >= first]
                old.date_from = old.daily[0].date if old.daily else None
                old.date_to = old.daily[-1].date if old.daily else None
                db.execute("UPDATE snapshots SET payload=? WHERE id=1", (old.model_dump_json(),))
            public_projection.maintain(db, now)
        with self.lock:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if gap:
            raise AggregateGap("Unaggregated events expired; explicit recovery required")
