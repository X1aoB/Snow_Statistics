import copy
from datetime import UTC, datetime

import pytest
from test_real_publication import packages

from snow_statistics.io import digest
from snow_statistics.publication import canonical
from snow_statistics.real_lake import prepare_bundle, validate_receipt

NOW = datetime(2026, 9, 14, tzinfo=UTC)
WAREHOUSE = "hdfs://snow-control:9000/snow/warehouse/real/test/iceberg/attempt1"


def test_only_validated_aggregate_pair_can_enter_bounded_real_lake():
    daily, behavior = packages()
    bundle = prepare_bundle(daily, behavior, "attempt1", WAREHOUSE, now=NOW)
    assert set(bundle["aggregates"]) == {"daily", "session_daily", "retention", "funnel"}
    assert bundle["original_at"] == "2026-09-01T00:00:00+08:00"
    assert bundle["expires_at"] == "2026-11-30T00:00:00+08:00"
    for rows in bundle["aggregates"].values():
        for row in rows:
            assert row["source"] == "real"
            assert not {"event_id", "anonymous_id", "request_id", "payload"} & set(row)
    later = prepare_bundle(daily, behavior, "copied", WAREHOUSE + "_copy", now=NOW)
    assert later["expires_at"] == bundle["expires_at"]
    with pytest.raises(ValueError, match="expired"):
        prepare_bundle(daily, behavior, "expired", WAREHOUSE, now=datetime(2026, 12, 1, tzinfo=UTC))
    behavior["aggregates"]["session_daily"][0]["request_id"] = "do-not-copy"
    with pytest.raises(ValueError):
        prepare_bundle(daily, behavior, "leak", WAREHOUSE, now=NOW)


def test_real_lake_requires_actual_readback_and_honest_empty_snapshot():
    daily, behavior = packages()
    bundle = prepare_bundle(daily, behavior, "attempt1", WAREHOUSE, now=NOW)
    receipt = dict(schema_version=1, source="real", run_id="attempt1", warehouse=WAREHOUSE,
                   input_sha256=digest(canonical(bundle)), expires_at=bundle["expires_at"], engine="Spark 3.5.7",
                   master="yarn", application_id="application_1780000000000_0001", hive_registration=False, column_lineage=False,
                   tables={name: dict(name="real_lake.analytics." + name, rows=len(rows), exact_readback_equal=True,
                                      original_snapshot=10 if rows else None, current_snapshot=10 if rows else None,
                                      historical_readback_equal=True if rows else None, location=WAREHOUSE + "/analytics/" + name)
                           for name, rows in bundle["aggregates"].items()})
    assert validate_receipt(receipt, bundle, now=NOW)
    for change in (lambda value: value.update(column_lineage=True), lambda value: value.update(input_sha256="0" * 64),
                   lambda value: value["tables"]["daily"].update(exact_readback_equal=False),
                   lambda value: value["tables"]["daily"].update(original_snapshot=None)):
        altered = copy.deepcopy(receipt)
        change(altered)
        with pytest.raises(ValueError):
            validate_receipt(altered, bundle, now=NOW)
