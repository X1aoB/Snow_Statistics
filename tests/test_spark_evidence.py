from copy import deepcopy

import pytest

from snow_statistics.spark_evidence import job_groups, summarize


def sample():
    metrics = {name: 0 for name in ("Executor Run Time", "Executor CPU Time", "JVM GC Time", "Memory Bytes Spilled", "Disk Bytes Spilled", "Peak Execution Memory")}
    metrics.update({"Executor Run Time": 500, "Disk Bytes Spilled": 100,
                    "Input Metrics": {"Bytes Read": 10, "Records Read": 2},
                    "Output Metrics": {"Bytes Written": 5}, "Shuffle Write Metrics": {"Shuffle Bytes Written": 3}})
    return [{"Event": "SparkListenerLogStart", "Spark Version": "3.5.7"},
            {"Event": "SparkListenerApplicationStart", "Timestamp": 1000, "App ID": "application_test"},
            {"Event": "SparkListenerTaskStart", "Task Info": {"Task ID": 1}},
            {"Event": "SparkListenerTaskEnd", "Task Info": {"Task ID": 1}, "Task End Reason": {"Reason": "Success"}, "Task Metrics": metrics},
            {"Event": "SparkListenerApplicationEnd", "Timestamp": 2500}]


def test_eventlog_timings_and_failure_counts_are_not_silently_dropped():
    events = sample()
    result, _ = summarize(events)
    assert result["application_seconds"] == 1.5
    assert result["executor_run_seconds"] == .5 and result["disk_bytes_spilled"] == 100
    failed = deepcopy(events)
    failed[3]["Task End Reason"]["Reason"] = "ExceptionFailure"
    assert summarize(failed)[0]["task_outcomes"] == {"ExceptionFailure": 1}


def test_partial_or_duplicate_event_logs_cannot_be_accepted():
    events = sample()
    for invalid in (events[:-1], events[:3] + events[4:], events + [events[3]]):
        with pytest.raises(ValueError):
            summarize(invalid)


def test_job_groups_preserve_failed_attempt_cost_and_refuse_ambiguous_stages():
    task = deepcopy(sample()[3])
    task["Stage ID"] = 5
    task["Task Metrics"]["Shuffle Read Metrics"] = {
        "Remote Bytes Read": 4, "Local Bytes Read": 6, "Total Records Read": 8}
    job = dict(Event="SparkListenerJobStart", **{"Job ID": 1, "Stage IDs": [5],
                                                "Properties": {"spark.jobGroup.id": "join-1"}})
    failed = deepcopy(task)
    failed["Task Info"]["Task ID"] = 2
    failed["Task End Reason"]["Reason"] = "ExceptionFailure"
    result = job_groups([job, task, failed])["join-1"]
    assert result["tasks"] == 2 and result["input_bytes"] == 20 and result["shuffle_read_bytes"] == 20
    assert result["outcomes"] == {"Success": 1, "ExceptionFailure": 1}
    with pytest.raises(ValueError, match="Unattributed"):
        job_groups([task])
    conflict = deepcopy(job)
    conflict["Properties"]["spark.jobGroup.id"] = "join-2"
    with pytest.raises(ValueError, match="Shared stage"):
        job_groups([job, task, conflict])
