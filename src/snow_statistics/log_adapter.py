"""Transform a timestamped structured log. Never copy arbitrary log fields."""
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from .contracts import Event


def from_log(record, timestamp):
    if record.get("event") != "public_generation_complete":
        return None
    # The caller supplies the log envelope's timestamp, never the replay clock.
    value = Event(event_id=uuid5(NAMESPACE_URL, "snow/request/" + record["request_id"]),
                  app="project_snow", event_type="request_complete",
                  occurred_at=datetime.fromisoformat(timestamp), request_id=record["request_id"],
                  character_id=record["character_id"],
                  success=record.get("stage") == "complete" and not record.get("terminal_error") and not record.get("exception_type"),
                  elapsed_ms=record["elapsed_ms"])
    return value.model_dump(mode="json", exclude_none=True)
