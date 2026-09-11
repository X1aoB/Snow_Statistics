"""Business-date windows shared by the scheduler and acceptance tests."""
import hashlib
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo


def retry_seconds(value):
    seconds = int(value)
    if not 10 <= seconds <= 600:
        raise ValueError("Experimental retry delay must be within 10..600 seconds")
    return seconds


def resolve_window(interval_end, run_id, date_from=None, date_to=None, cutoff=None):
    end_time = datetime.fromisoformat(str(interval_end).replace("Z", "+00:00"))
    if end_time.tzinfo is None:
        raise ValueError("Interval must have timezone")
    end = date.fromisoformat(date_to) if date_to else end_time.astimezone(ZoneInfo("Asia/Hong_Kong")).date() - timedelta(days=1)
    start = date.fromisoformat(date_from) if date_from else end - timedelta(days=6)
    cutoff_time = datetime.fromisoformat(cutoff.replace("Z", "+00:00")) if cutoff else end_time
    if start > end or (end - start).days > 365 or cutoff_time.tzinfo is None:
        raise ValueError("Invalid correction window/cutoff")
    return dict(start=str(start), end=str(end), cutoff=cutoff_time.astimezone(UTC).isoformat(),
                run_id="af-" + hashlib.sha256(run_id.encode()).hexdigest()[:24])
