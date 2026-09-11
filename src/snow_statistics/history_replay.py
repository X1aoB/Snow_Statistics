"""Reorder a bounded historical fixture only when first-acceptance winners survive."""

from datetime import datetime


def order_history(rows):
    if not rows or any(row["source"] != "synthetic" for row in rows):
        raise ValueError("Synthetic nonempty history required")
    if len({row["seq"] for row in rows}) != len(rows):
        raise ValueError("Unique original collector positions required")

    def winners(records):
        result = {}
        for row in records:
            event = row["event"]
            keys = [(event["app"], "event", event["event_id"])]
            if event["event_type"] == "request_complete":
                keys.append((event["app"], "request", event["request_id"]))
            for key in keys:
                result.setdefault(key, row["seq"])
        return result

    original = sorted(rows, key=lambda row: row["seq"])
    ordered = sorted(
        rows,
        key=lambda row: (
            datetime.fromisoformat(row["event"]["occurred_at"].replace("Z", "+00:00")),
            row["seq"],
        ),
    )
    if winners(original) != winners(ordered):
        raise ValueError("Event-time ordering changes a first-accepted event or request")
    return ordered
