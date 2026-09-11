"""Conservative, fully covered client-observed freshness for bounded lab runs."""
import math


def summarize(samples, expected, target_seconds=60):
    if not samples or len(samples) != expected:
        raise ValueError("Incomplete observation coverage; do not drop unobserved events")
    if len({row["event_id"] for row in samples}) != expected:
        raise ValueError("Duplicate observation identity")
    upper = sorted(row["from_send_seconds"] for row in samples)
    ack = sorted(row["from_ack_seconds"] for row in samples)
    if any(not math.isfinite(value) or value < 0 for value in upper + ack):
        raise ValueError("Invalid elapsed duration")
    if any(row["from_send_seconds"] < row["from_ack_seconds"] for row in samples):
        raise ValueError("HTTP acknowledgement must follow request start")
    def quantiles(values):
        return {name: round(values[math.ceil(p * len(values)) - 1], 6)
                for name, p in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("max", 1))}
    return dict(observed=expected, missing=0, quantile_method="nearest-rank",
                send_to_first_query_completion_seconds=quantiles(upper),
                ack_to_first_query_completion_seconds=quantiles(ack),
                target_seconds=target_seconds, passed=upper[math.ceil(0.95 * expected) - 1] <= target_seconds,
                note="Send-to-query is an upper bound on durable acceptance-to-visibility; includes HTTP, query and polling overhead")
