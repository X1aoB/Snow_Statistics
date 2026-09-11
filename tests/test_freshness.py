import pytest

from snow_statistics.freshness import summarize


def test_nearest_rank_and_conservative_gate():
    samples = [dict(event_id=str(i), from_send_seconds=i, from_ack_seconds=i - .1) for i in range(1, 101)]
    result = summarize(samples, 100)
    assert result["send_to_first_query_completion_seconds"]["p95"] == 95
    assert result["ack_to_first_query_completion_seconds"]["p95"] == 94.9
    assert not result["passed"]
    assert summarize(samples, 100, target_seconds=95)["passed"]


def test_missing_duplicate_or_invalid_observations_cannot_pass():
    sample = dict(event_id="one", from_send_seconds=2, from_ack_seconds=1)
    for samples, expected in (([], 1), ([sample], 2), ([sample, sample], 2),
                              ([sample | dict(from_ack_seconds=-1)], 1),
                              ([sample | dict(from_send_seconds=float("nan"))], 1),
                              ([sample | dict(from_send_seconds=.5)], 1)):
        with pytest.raises(ValueError):
            summarize(samples, expected)
