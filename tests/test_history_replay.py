from copy import deepcopy

import pytest

from snow_statistics.history_replay import order_history
from snow_statistics.model import daily_metrics, deduplicate
from snow_statistics.scale_fixture import events, expected


def test_history_order_preserves_rows_positions_and_metric_winners():
    rows = list(events(20))
    before = deepcopy(rows)
    ordered = order_history(list(reversed(rows)))
    assert rows == before and sorted(ordered, key=lambda row: row["seq"]) == rows
    assert [row["event"]["occurred_at"] for row in ordered] == sorted(
        row["event"]["occurred_at"] for row in rows
    )
    valid, quality, quarantine = deduplicate(ordered)
    assert daily_metrics(valid) == expected(20)["daily"] and not quarantine
    assert quality["duplicates"] == 20


def test_history_rejects_changed_first_request_and_mixed_source():
    rows = list(events(2))
    rows[19]["event"]["occurred_at"] = "2025-12-31T00:00:00Z"
    with pytest.raises(ValueError, match="first-accepted"):
        order_history(rows)
    rows[0]["source"] = "real"
    with pytest.raises(ValueError, match="Synthetic"):
        order_history(rows)
