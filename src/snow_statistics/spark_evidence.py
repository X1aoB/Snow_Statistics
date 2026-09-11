"""Summarize completed Spark event logs without exposing environment properties."""
from collections import Counter


def summarize(events):
    start = end = version = None
    tasks, jobs, executors, plans = [], [], [], []
    started_tasks = set()
    for event in events:
        name = event["Event"]
        if name == "SparkListenerLogStart":
            version = event["Spark Version"]
        elif name == "SparkListenerApplicationStart":
            if start is not None:
                raise ValueError("Multiple applications in one log")
            start = event
        elif name == "SparkListenerApplicationEnd":
            if end is not None:
                raise ValueError("Duplicate application end")
            end = event
        elif name == "SparkListenerExecutorAdded":
            executors.append(dict(id=event["Executor ID"], host=event["Executor Info"]["Host"], cores=event["Executor Info"]["Total Cores"]))
        elif name == "SparkListenerTaskStart":
            started_tasks.add(event["Task Info"]["Task ID"])
        elif name == "SparkListenerTaskEnd":
            tasks.append(event)
        elif name == "SparkListenerJobEnd":
            jobs.append(event["Job Result"]["Result"])
        elif name.endswith(("SparkListenerSQLExecutionStart", "SparkListenerSQLAdaptiveExecutionUpdate")):
            plans.append(dict(event=name, execution_id=event["executionId"], plan=event["physicalPlanDescription"]))
    if not start or not end or not version or not tasks or end["Timestamp"] < start["Timestamp"]:
        raise ValueError("Incomplete application log")
    ended = [task["Task Info"]["Task ID"] for task in tasks]
    if len(set(ended)) != len(ended) or set(ended) != started_tasks:
        raise ValueError("Incomplete or duplicate task observations")
    metrics = [task["Task Metrics"] for task in tasks]
    counts = Counter(task["Task End Reason"]["Reason"] for task in tasks)
    result = dict(engine="Spark " + version, application_id=start["App ID"],
                  application_seconds=(end["Timestamp"] - start["Timestamp"]) / 1000,
                  executors=executors, tasks=len(tasks), task_outcomes=dict(counts), job_outcomes=dict(Counter(jobs)),
                  executor_run_seconds=sum(m["Executor Run Time"] for m in metrics) / 1000,
                  executor_cpu_seconds=sum(m["Executor CPU Time"] for m in metrics) / 1_000_000_000,
                  task_jvm_gc_seconds=sum(m["JVM GC Time"] for m in metrics) / 1000,
                  memory_bytes_spilled=sum(m["Memory Bytes Spilled"] for m in metrics),
                  disk_bytes_spilled=sum(m["Disk Bytes Spilled"] for m in metrics),
                  max_task_peak_execution_memory=max(m["Peak Execution Memory"] for m in metrics),
                  input_bytes_across_tasks=sum(m["Input Metrics"]["Bytes Read"] for m in metrics),
                  input_records_across_tasks=sum(m["Input Metrics"]["Records Read"] for m in metrics),
                  output_bytes_across_tasks=sum(m["Output Metrics"]["Bytes Written"] for m in metrics),
                  shuffle_bytes_written=sum(m["Shuffle Write Metrics"]["Shuffle Bytes Written"] for m in metrics),
                  sql_plan_observations=len(plans),
                  note="Application timing includes executor allocation but excludes input landing and pre-Spark JVM startup. Task counters include rescans/cache work and are not source row counts or process RSS peaks.")
    return result, plans


def job_groups(events):
    """Attribute actual task attempts once, refusing shared-stage ambiguity."""
    stages, jobs, tasks = {}, {}, []
    for event in events:
        if event["Event"] == "SparkListenerJobStart":
            group = event.get("Properties", {}).get("spark.jobGroup.id")
            if not group:
                raise ValueError("Missing explicit benchmark job group")
            jobs[event["Job ID"]] = group
            for stage in event["Stage IDs"]:
                if stage in stages and stages[stage] != group:
                    raise ValueError("Shared stage cannot be charged to two benchmark groups")
                stages[stage] = group
        elif event["Event"] == "SparkListenerTaskEnd":
            tasks.append(event)
    result = {}
    for task in tasks:
        group = stages.get(task["Stage ID"])
        if not group:
            raise ValueError("Unattributed task")
        row = result.setdefault(group, dict(tasks=0, outcomes={}, input_bytes=0, shuffle_write_bytes=0,
                                            shuffle_read_bytes=0, disk_spill_bytes=0, cpu_seconds=0,
                                            executor_seconds=0, stages={}))
        m = task["Task Metrics"]
        row["tasks"] += 1
        outcome = task["Task End Reason"]["Reason"]
        row["outcomes"][outcome] = row["outcomes"].get(outcome, 0) + 1
        row["input_bytes"] += m["Input Metrics"]["Bytes Read"]
        row["shuffle_write_bytes"] += m["Shuffle Write Metrics"]["Shuffle Bytes Written"]
        read = m["Shuffle Read Metrics"]
        row["shuffle_read_bytes"] += read["Remote Bytes Read"] + read["Local Bytes Read"]
        row["disk_spill_bytes"] += m["Disk Bytes Spilled"]
        row["cpu_seconds"] += m["Executor CPU Time"] / 1_000_000_000
        row["executor_seconds"] += m["Executor Run Time"] / 1000
        row["stages"].setdefault(str(task["Stage ID"]), []).append(dict(
            task_id=task["Task Info"]["Task ID"], outcome=outcome,
            shuffle_read_records=read["Total Records Read"], run_ms=m["Executor Run Time"]))
    return result
