"""A frozen public projection, separate from exact internal metrics.

Only pre-cutoff accepted events populate candidates. A day is frozen once, even
when processing resumes after its publication time. Public history starts when
this projection is installed: legacy totals lack the required distinct evidence.
"""
from datetime import datetime, time, timedelta

from .contracts import BUSINESS_TZ, PublicDay, PublicSummary, Summary, business_day, instant

SCHEMA = """
CREATE TABLE IF NOT EXISTS public_daily(
 app TEXT NOT NULL, day TEXT NOT NULL, pv INTEGER NOT NULL DEFAULT 0,
 uv INTEGER NOT NULL DEFAULT 0, requests INTEGER NOT NULL DEFAULT 0,
 successes INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(app,day));
CREATE TABLE IF NOT EXISTS public_visitors(
 app TEXT NOT NULL, day TEXT NOT NULL, anonymous_id TEXT NOT NULL,
 PRIMARY KEY(app,day,anonymous_id));
CREATE TABLE IF NOT EXISTS public_popularity(
 app TEXT NOT NULL, day TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
 count INTEGER NOT NULL DEFAULT 0, visitors INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(app,day,kind,name));
CREATE TABLE IF NOT EXISTS public_popularity_visitors(
 app TEXT NOT NULL, day TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
 anonymous_id TEXT NOT NULL, PRIMARY KEY(app,day,kind,name,anonymous_id));
CREATE TABLE IF NOT EXISTS public_frozen(
 app TEXT NOT NULL, day TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(app,day));
CREATE TABLE IF NOT EXISTS public_snapshots(
 id INTEGER PRIMARY KEY CHECK(id=1), slot TEXT NOT NULL, payload TEXT NOT NULL);
"""


def cutoff(day):
    return datetime.combine(datetime.fromisoformat(day).date() + timedelta(days=3),
                            time(0, 15), BUSINESS_TZ)


def publication_slot(now):
    local = now.astimezone(BUSINESS_TZ)
    slot = datetime.combine(local.date(), time(0, 15), BUSINESS_TZ)
    return slot if now >= slot else slot - timedelta(days=1)


def accumulate(db, row, event, pv, requests, successes):
    if row["source"] != "real" or datetime.fromisoformat(row["accepted_at"]) >= cutoff(row["day"]):
        return
    start = db.execute("SELECT value FROM meta WHERE key='public_start_day'").fetchone()[0]
    if row["day"] < start:
        return
    key = row["app"], row["day"]
    db.execute("INSERT OR IGNORE INTO public_daily(app,day) VALUES(?,?)", key)
    uv = 0
    if event.get("anonymous_id") and event["event_type"] != "request_complete":
        uv = db.execute("INSERT OR IGNORE INTO public_visitors VALUES(?,?,?)",
                        (*key, event["anonymous_id"])).rowcount
    db.execute("UPDATE public_daily SET pv=pv+?,uv=uv+?,requests=requests+?,successes=successes+? "
               "WHERE app=? AND day=?", (pv, uv, requests, successes, *key))
    kind, name = (("page", event.get("path")) if pv else
                  ("character", event.get("character_id") if event["event_type"] == "character_select" else None))
    if name:
        pkey = (*key, kind, name)
        distinct = 0
        if event.get("anonymous_id"):
            distinct = db.execute("INSERT OR IGNORE INTO public_popularity_visitors VALUES(?,?,?,?,?)",
                                  (*pkey, event["anonymous_id"])).rowcount
        db.execute("INSERT INTO public_popularity VALUES(?,?,?,?,1,?) "
                   "ON CONFLICT(app,day,kind,name) DO UPDATE SET count=count+1,visitors=visitors+?",
                   (*pkey, distinct, distinct))


def group(state, value=None):
    return {"state": state, "value": value}


def freeze_day(db, app, day):
    metrics = db.execute("SELECT * FROM public_daily WHERE app=? AND day=?", (app, day)).fetchone()
    pv, uv, requests, successes = ([metrics[key] for key in ("pv", "uv", "requests", "successes")]
                                    if metrics else (0, 0, 0, 0))
    access = (group("empty") if not pv and not uv else group("published", {"pv": pv, "uv": uv})
              if uv >= 10 else group("suppressed"))
    failures = requests - successes
    quality = (group("empty") if not requests else
               group("published", {"requests": requests, "successes": successes,
                                   "success_rate": successes / requests})
               if requests >= 10 and (successes == 0 or successes >= 10) and (failures == 0 or failures >= 10)
               else group("suppressed"))
    categories = db.execute("SELECT kind,name,count,visitors FROM public_popularity "
                            "WHERE app=? AND day=? ORDER BY kind,name", (app, day)).fetchall()
    popularity = (group("empty") if not categories else group("suppressed")
                  if any(row["visitors"] < 10 for row in categories) else
                  group("published", [{key: row[key] for key in ("kind", "name", "count")} for row in categories]))
    return PublicDay(app=app, date=day, cutoff_at=instant(cutoff(day)),
                     access=access, quality=quality, popularity=popularity)


def publish(db, now, *, fail_before_commit=False):
    slot = publication_slot(now)
    previous = db.execute("SELECT slot FROM public_snapshots WHERE id=1").fetchone()
    if previous and previous[0] == instant(slot):
        return False
    start = db.execute("SELECT value FROM meta WHERE key='public_start_day'").fetchone()[0]
    day = max(datetime.fromisoformat(start).date(), now.astimezone(BUSINESS_TZ).date() - timedelta(days=89))
    end = now.astimezone(BUSINESS_TZ).date()
    days = []
    while day <= end:
        for app in ("mywebsite", "project_snow"):
            label = day.isoformat()
            if cutoff(label) > slot:
                result = PublicDay(app=app, date=label, cutoff_at=instant(cutoff(label)),
                                   access=group("pending"), quality=group("pending"), popularity=group("pending"))
            else:
                frozen = db.execute("SELECT payload FROM public_frozen WHERE app=? AND day=?", (app, label)).fetchone()
                result = PublicDay.model_validate_json(frozen[0]) if frozen else freeze_day(db, app, label)
                if not frozen:
                    db.execute("INSERT INTO public_frozen VALUES(?,?,?)", (app, label, result.model_dump_json()))
            days.append(result)
        day += timedelta(days=1)
    snapshot = PublicSummary(generated_at=instant(now), cutoff_at=instant(slot),
                             date_from=days[0].date if days else None, date_to=days[-1].date if days else None,
                             status="ok" if days else "empty", daily=days)
    if fail_before_commit:
        raise RuntimeError("injected public snapshot failure")
    db.execute("INSERT INTO public_snapshots VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET "
               "slot=excluded.slot,payload=excluded.payload", (instant(slot), snapshot.model_dump_json()))
    return True


def legacy_projection(snapshot):
    """V1 cannot mask an integer: omit the entire row if either group is hidden."""
    daily, popular = [], []
    for row in snapshot.daily:
        if row.access.state not in {"published", "empty"} or row.quality.state not in {"published", "empty"}:
            continue
        access = row.access.value.model_dump() if row.access.value else {"pv": 0, "uv": 0}
        quality = (row.quality.value.model_dump() if row.quality.value else
                   {"requests": 0, "successes": 0, "success_rate": None})
        daily.append({"app": row.app, "date": row.date, **access, **quality})
        if row.popularity.state == "published":
            popular.extend({"app": row.app, "date": row.date, **value.model_dump()} for value in row.popularity.value)
    return Summary(generated_at=snapshot.generated_at, date_from=daily[0]["date"] if daily else None,
                   date_to=daily[-1]["date"] if daily else None, status=snapshot.status,
                   daily=daily, popularity=popular)


def maintain(db, now):
    for table in ("public_visitors", "public_popularity_visitors"):
        db.execute(f"DELETE FROM {table} WHERE day<?", (business_day(now - timedelta(days=29)),))
    for table in ("public_daily", "public_popularity", "public_frozen"):
        db.execute(f"DELETE FROM {table} WHERE day<?", (business_day(now - timedelta(days=89)),))
    cached = db.execute("SELECT payload FROM public_snapshots WHERE id=1").fetchone()
    if cached:
        result = PublicSummary.model_validate_json(cached[0])
        retained = [row for row in result.daily if row.date >= business_day(now - timedelta(days=89))]
        if len(retained) != len(result.daily):
            result.daily = retained
            result.date_from = retained[0].date if retained else None
            result.date_to = retained[-1].date if retained else None
            db.execute("UPDATE public_snapshots SET payload=? WHERE id=1", (result.model_dump_json(),))
