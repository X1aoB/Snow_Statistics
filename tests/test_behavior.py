import pytest

from snow_statistics.behavior import analyze
from snow_statistics.behavior_fixture import generate_behavior
from snow_statistics.model import deduplicate, funnel
from snow_statistics.simulator import identity


def result():
    fixture = generate_behavior()
    return fixture, analyze(fixture["events"], fixture["date_from"], fixture["date_to"], fixture["cutoff"])


def test_latest_click_no_fallback_default_role_and_exact_timeout():
    fixture, data = result()
    assert {r["request_id"] for r in data["conversions"]} == {"first", "exact"}
    assert {r["jump_id"] for r in data["conversions"]} == {fixture["cases"]["latest"], fixture["cases"]["exact"]}
    assert sum(r["converted"] for r in data["funnel"]) == 2
    assert sum(r["selected"] for r in data["funnel"]) == 1
    # The early demonstration model uses the same main-conversion definition.
    assert {r["request_id"] for r in funnel(deduplicate(fixture["events"])[0])} == {"first", "exact"}


def test_cross_day_session_and_exact_thirty_minute_gap():
    _, data = result()
    sessions = [r for r in data["sessions"] if r["anonymous_id"] == identity(701, "mywebsite/session-boundary")]
    assert [(r["date"], r["events"], r["duration_seconds"]) for r in sessions] == [("2026-01-01", 2, 1799), ("2026-01-02", 1, 0)]
    assert all(r["closed"] for r in sessions)


def test_retention_maturity_and_history_before_report_window():
    fixture, data = result()
    newest = next(r for r in data["retention"] if r["cohort_date"] == "2026-01-09")
    assert newest["eligible_d1"] == 0 and newest["retained_d1"] is None and newest["retained_d7"] is None
    older = next(r for r in data["retention"] if r["cohort_date"] == "2026-01-01" and r["app"] == "mywebsite")
    assert older["retained_d7"] == 1
    narrow = analyze(fixture["events"], "2026-01-02", "2026-01-09", fixture["cutoff"])
    assert not any(r["cohort_date"] == "2026-01-02" for r in narrow["retention"])
    assert not any(r["date"] == "2026-01-01" for r in narrow["sessions"])


def test_replay_cutoff_and_unclosed_date():
    fixture, data = result()
    replay = analyze(fixture["events"] * 2, fixture["date_from"], fixture["date_to"], fixture["cutoff"])
    assert data == replay
    with pytest.raises(ValueError):
        analyze(fixture["events"], "2026-01-01", "2026-01-10", fixture["cutoff"])
