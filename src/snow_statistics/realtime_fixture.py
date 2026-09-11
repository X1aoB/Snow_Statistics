"""Fixed event-time boundaries for actual Flink/Kafka/Doris recovery acceptance."""
from copy import deepcopy
from uuid import NAMESPACE_URL, uuid5


def phases(lane, accepted_at):
    def event(n, kind, at, app="mywebsite", **values):
        return dict(source="synthetic", seq=n, accepted_at=accepted_at,
                    event=dict(schema_version=1, event_id=str(uuid5(NAMESPACE_URL, f"snow-realtime/{lane}/{n}")),
                               app=app, event_type=kind, occurred_at="2026-01-01T" + at + "Z", **values))
    visitor = str(uuid5(NAMESPACE_URL, "snow-realtime/visitor"))
    other = str(uuid5(NAMESPACE_URL, "snow-realtime/other"))
    a = [event(1, "page_view", "15:59:55", path="/", anonymous_id=visitor),
         event(2, "request_observed", "15:59:56", "project_snow", request_id="req_A", anonymous_id=other),
         event(3, "request_complete", "15:59:58", "project_snow", request_id="req_A", character_id="snow", success=True, elapsed_ms=50),
         event(4, "page_view", "15:59:59", path="/statistics/", anonymous_id=visitor)]
    duplicate = deepcopy(a[0])
    duplicate["seq"] = 5
    conflict = deepcopy(a[3])
    conflict["seq"] = 8
    conflict["event"]["path"] = "/"
    b = [duplicate,
         event(6, "request_complete", "16:00:05", "project_snow", request_id="req_A", character_id="snow", success=False, elapsed_ms=100),
         event(7, "page_view", "16:00:20", path="/", anonymous_id=visitor), conflict,
         event(9, "request_complete", "16:00:30", "project_snow", request_id="bad", character_id="snow", elapsed_ms=10),
         event(10, "page_view", "16:15:00", path="/", anonymous_id=visitor)]
    c = [event(11, "page_view", "16:05:00", path="/", anonymous_id=other),
         event(12, "page_view", "16:04:29", path="/", anonymous_id=other),
         event(13, "page_view", "16:04:29.999", path="/", anonymous_id=visitor),
         event(14, "page_view", "16:04:29.998", path="/", anonymous_id=visitor)]
    duplicate = deepcopy(c[0])
    duplicate["seq"] = 15
    c += [duplicate, event(16, "request_complete", "16:06:00", "project_snow", request_id="req_B", character_id="snow", success=False, elapsed_ms=90),
          event(17, "page_view", "16:06:00", path="/")]
    c[-1]["source"] = "real"  # Intentional provenance rejection; this is still synthetic fixture data.
    return [a, b, c]
