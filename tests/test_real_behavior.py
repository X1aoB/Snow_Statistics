"""Independent synthetic users, clocks and source generations; no real records."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from snow_statistics.real_behavior import (
    AUX_FIELDS,
    auxiliary_manifest,
    merge_tokens,
    observation_status,
    prune_tokens,
    retention_oracle,
    validate_auxiliary_manifest,
    validate_coverage,
)

CUTOFF = "2026-09-13T16:00:00+00:00"
NOW = datetime.fromisoformat(CUTOFF)


def coverage():
    return dict(schema_version=1, source="real", instance_id=str(uuid4()), generation=str(uuid4()),
                continuous_from="2026-08-31T16:00:00+00:00", through=CUTOFF, gaps=[])


def envelope(seq, when, anon):
    return dict(seq=seq, source="real", accepted_at=when, event=dict(event_id=str(uuid4()), app="mywebsite",
                event_type="page_view", occurred_at=when, anonymous_id=anon, path="/not-retained/", elapsed_ms=42))


def test_old_observation_survives_raw_window_without_becoming_a_new_cohort():
    anon = str(uuid4())
    old = envelope(1, "2026-09-04T17:00:00+00:00", anon)
    recent = envelope(2, "2026-09-11T17:00:00+00:00", anon)
    state = merge_tokens([], [old], NOW - timedelta(days=7))
    result = merge_tokens(state, [recent, recent], NOW)
    assert len(result) == 2 and set(result[0]) == set(AUX_FIELDS)
    assert "path" not in result[0] and "elapsed_ms" not in result[0]
    retained = retention_oracle(result, "2026-09-01", "2026-09-13", CUTOFF, coverage(), "2026-08-31T16:00:00+00:00")
    assert len(retained) == 1 and retained[0]["cohort_date"] == "2026-09-05"
    assert retained[0]["retained_d7"] == 1 and retained[0]["eligible_d7"] == 1
    # Looking only at the remaining seven-day raw window would incorrectly
    # assign this known returning anonymous identifier to September 12.
    raw_only = retention_oracle(merge_tokens([], [recent], NOW), "2026-09-01", "2026-09-13", CUTOFF, coverage(), "2026-09-06T16:00:00+00:00")
    assert raw_only[0]["cohort_date"] == "2026-09-12"
    assert raw_only[0]["retained_d7"] is None


def test_coverage_gaps_and_maturity_never_turn_into_zero_return_rate():
    cov = coverage()
    assert observation_status("2026-09-12", 7, CUTOFF, cov, cov["continuous_from"]) == "pending"
    assert observation_status("2026-09-05", 7, CUTOFF, cov, cov["continuous_from"]) == "complete_accepted_prefix"
    cov["gaps"] = [{"from": "2026-09-08T16:00:00Z", "to": "2026-09-09T16:00:00Z", "reason": "retention_gap"}]
    assert observation_status("2026-09-05", 7, CUTOFF, cov, cov["continuous_from"]) == "incomplete"
    assert observation_status("2026-09-05", 1, CUTOFF, cov, "2026-09-06T00:00:00Z") == "incomplete"


def test_copy_replay_and_cleanup_cannot_renew_auxiliary_expiry():
    cov = coverage()
    original = envelope(1, "2026-09-04T17:00:00Z", str(uuid4()))
    tokens = merge_tokens([], [original], NOW)
    copied = merge_tokens(tokens, [original], NOW + timedelta(days=1))
    assert copied == tokens
    descriptor = auxiliary_manifest("hdfs://snow-control:9000/snow/auxiliary/real/test/run1", cov,
                                    original["accepted_at"], 1, cov["continuous_from"])
    assert descriptor["expires_at"] == "2026-10-04T17:00:00+00:00"
    expiry = datetime.fromisoformat(descriptor["expires_at"])
    assert validate_auxiliary_manifest(descriptor, cov, expiry - timedelta(seconds=1)) == descriptor
    with pytest.raises(ValueError, match="Expired"):
        validate_auxiliary_manifest(descriptor, cov, expiry)
    with pytest.raises(ValueError, match="pruned"):
        merge_tokens(tokens, [], expiry)
    assert prune_tokens(tokens, expiry) == []
    copied_descriptor = deepcopy(descriptor)
    copied_descriptor["expires_at"] = (expiry + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="extend"):
        validate_auxiliary_manifest(copied_descriptor, cov, NOW)


def test_generation_fields_and_gap_contract_cannot_be_silently_changed():
    cov = coverage()
    assert validate_coverage(cov, CUTOFF)
    with pytest.raises(ValueError, match="generation"):
        validate_coverage(cov, CUTOFF, {"instance_id": cov["instance_id"], "generation": str(uuid4())})
    cov["gaps"] = [{"from": "2020-01-01T00:00:00Z", "to": CUTOFF, "reason": "gap"}]
    with pytest.raises(ValueError, match="outside"):
        validate_coverage(cov, CUTOFF)
    row = envelope(1, CUTOFF, str(uuid4()))
    row["source"] = "synthetic"
    with pytest.raises(ValueError, match="mix"):
        merge_tokens([], [row], datetime.now(UTC))
