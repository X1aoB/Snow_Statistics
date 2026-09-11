from snow_statistics.model import daily_metrics, deduplicate
from snow_statistics.realtime_fixture import phases


def test_realtime_boundary_hand_calculation():
    batches = phases("hand", "2026-09-11T00:00:00Z")
    assert batches == phases("hand", "2026-09-11T00:00:00Z")
    rows = [r for batch in batches for r in batch]
    valid, quality, quarantine = deduplicate([r for r in rows if r["source"] == "synthetic"])
    assert quality == dict(raw=16, valid=11, duplicates=3, quarantined=2)
    assert {q["reason"] for q in quarantine} == {"event_id_conflict", "invalid_contract"}
    live = daily_metrics([r for r in valid if r["seq"] not in {12, 14}])
    assert [(r["app"], r["date"], r["pv"], r["uv"], r["requests"], r["successes"]) for r in live] == [
        ("mywebsite", "2026-01-01", 2, 1, 0, 0), ("project_snow", "2026-01-01", 0, 1, 1, 1),
        ("mywebsite", "2026-01-02", 4, 2, 0, 0), ("project_snow", "2026-01-02", 0, 0, 1, 0)]
    assert daily_metrics(valid)[2]["pv"] == 6
