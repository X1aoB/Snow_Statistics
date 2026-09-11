from snow_statistics.contracts import Event
from snow_statistics.model import daily_metrics, deduplicate
from snow_statistics.scale_fixture import events, expected


def test_scale_expected_is_independent_and_covers_both_duplicate_types():
    rows = list(events(20))
    assert rows == list(events(20)) and rows != list(events(20, seed=43))
    assert [row["seq"] for row in rows] == list(range(1, 201))
    for row in rows:
        Event.model_validate(row["event"])
    valid, quality, quarantine = deduplicate(rows)
    assert quality == dict(raw=200, valid=180, duplicates=20, quarantined=0) and not quarantine
    assert daily_metrics(valid) == expected(20)["daily"]
    assert rows[9]["event"] == rows[6]["event"]
    assert rows[19]["event"]["event_id"] != rows[16]["event"]["event_id"]
    assert rows[19]["event"]["request_id"] == rows[16]["event"]["request_id"]


def test_scale_golden_counts_at_100k_without_materializing_input():
    result = expected(10_000)
    assert result["quality"] == dict(raw=100_000, valid=90_000, duplicates=10_000, quarantined=0, after_cutoff=0)
    assert sum(row["pv"] for row in result["daily"]) == 30_000
    assert sum(row["requests"] for row in result["daily"]) == 10_000
    assert sum(row["successes"] for row in result["daily"]) == 7_500
    assert len(result["daily"]) == 14
