"""One bounded YARN scale job with sampled host capacity and scoped stop on risk."""
import argparse
import json
import re
import subprocess
import sys
import time

from vmware_lab import MAX_PROJECT_BYTES, MIN_HOST_AVAILABLE_MIB, ROOT, capacity, run

from snow_statistics.io import write_json


def stop_project(child, stop_script):
    """Attempt every scoped shutdown even if SSH or an earlier stop fails."""
    errors = []

    def attempt(label, command, timeout):
        try:
            subprocess.run(command, cwd=ROOT, check=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError) as error:
            errors.append(dict(step=label, error=type(error).__name__))

    attempt("stop_driver", [sys.executable, "tools/lab_remote.py", "--node", "snow-control",
                            "--script", str(stop_script)], 45)
    try:
        child.wait(timeout=45)
    except subprocess.TimeoutExpired:
        # A local observer exit does not prove the remote job stopped.
        # Continue with every VM stop and retain any failure in the receipt.
        try:
            child.terminate()
            child.wait(timeout=10)
        except (OSError, subprocess.SubprocessError):
            try:
                child.kill()
                child.wait(timeout=10)
            except (OSError, subprocess.SubprocessError) as error:
                errors.append(dict(step="local_observer", error=type(error).__name__))
    for node in ("snow-compute", "snow-analysis", "snow-control"):
        attempt(node + "_services", [sys.executable, "tools/lab_remote.py", "--node", node,
                                    "--script", "tools/stop_scale_node.sh"], 45)
        attempt(node + "_vm", [sys.executable, "tools/vmware_lab.py", "stop", "--node", node], 60)
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, choices=(100_000, 1_000_000), required=True)
    parser.add_argument("--workload", choices=("daily", "optimization", "lake", "lake-verify"), default="daily")
    parser.add_argument("--inject-stop-after-seconds", type=int,
                        help="Explicit synthetic fault exercise; execute scoped stop after 10..120 seconds")
    parser.add_argument("--attempt", required=True)
    args = parser.parse_args()
    if args.workload == "optimization" and args.events != 1_000_000:
        parser.error("Optimization uses the accepted million-row DWD only")
    if args.workload.startswith("lake") and args.events != 100_000:
        parser.error("Lake migration uses accepted 100k source only")
    if args.inject_stop_after_seconds is not None and not 10 <= args.inject_stop_after_seconds <= 120:
        parser.error("Fault injection must be within 10..120 seconds")
    if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", args.attempt):
        parser.error("Invalid attempt")
    area = "lake" if args.workload.startswith("lake") else "optimization" if args.workload == "optimization" else "scale"
    directory = ROOT / "runtime" / area
    label = args.attempt + ("-verify" if args.workload == "lake-verify" else "")
    receipt = directory / (label + "-monitor.json")
    if receipt.exists():
        parser.error("Existing monitor receipt; choose a new attempt")
    try:
        initial = capacity(1024)
    except RuntimeError as error:
        write_json(receipt, dict(attempt=args.attempt, phase="preflight_rejected", job_started=False, reason=str(error)))
        raise SystemExit("Capacity preflight rejected; no job started") from None
    directory.mkdir(parents=True, exist_ok=True)
    script = directory / (label + "-submit.sh")
    workload = (f"tools/run_optimization.sh {args.attempt}" if args.workload == "optimization"
                else f"tools/run_scale_batch.sh {args.events} {args.attempt}")
    if args.workload.startswith("lake"):
        workload = f"tools/run_iceberg_dwd.sh {args.attempt}" + (" --verify" if args.workload == "lake-verify" else "")
    script.write_text(f"set -euo pipefail\ncd /home/snow/Snow_Statistics\nbash {workload}\n", encoding="utf-8", newline="\n")
    stop_script = directory / "stop-scale-job.sh"
    stop_script.write_text("set -euo pipefail\ntest \"$(hostname)\" = snow-control\nsudo docker stop -t 10 snow-spark-yarn </dev/null\n", encoding="utf-8", newline="\n")
    command = [sys.executable, "tools/lab_remote.py", "--node", "snow-control", "--script", str(script), "--reserve-mib", "1024"]
    start = time.monotonic()
    samples, risk, cleanup_errors = [], None, []
    with (directory / (label + "-submit.log")).open("wb") as log:
        child = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
        try:
            while child.poll() is None:
                try:
                    snapshot = capacity()
                    snapshot["host_available_mib"] = int(run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory")) // 1024
                    samples.append(dict(elapsed_seconds=round(time.monotonic()-start, 3), **snapshot))
                    threshold = MAX_PROJECT_BYTES / 1024**3 - .25
                    if snapshot["project_files_gib"] >= threshold:
                        risk = f"Project reached early-stop threshold {threshold:g} GiB"
                    if snapshot["host_available_mib"] < MIN_HOST_AVAILABLE_MIB:
                        risk = f"Host RAM reserve fell below {MIN_HOST_AVAILABLE_MIB} MiB"
                    if time.monotonic() - start > 1000:
                        risk = "Job monitoring deadline"
                    if args.inject_stop_after_seconds is not None and time.monotonic() - start >= args.inject_stop_after_seconds:
                        risk = "Explicit synthetic stop-path fault injection; no actual resource exhaustion"
                except (RuntimeError, OSError, subprocess.SubprocessError, ValueError) as error:
                    risk = str(error)
                if risk:
                    break
                time.sleep(2)
        except KeyboardInterrupt:
            risk = "Operator interrupted monitoring"
        if risk:
            write_json(receipt, dict(attempt=args.attempt, initial=initial, samples=samples,
                                    early_stop_reason=risk, cleanup="starting", job_started=True))
            cleanup_errors = stop_project(child, stop_script)
        result = dict(attempt=args.attempt, initial=initial, samples=samples, early_stop_reason=risk,
                      exit_code=child.returncode, elapsed_seconds=round(time.monotonic()-start, 3),
                      cleanup=("failed" if cleanup_errors else "complete") if risk else "not_required",
                      cleanup_errors=cleanup_errors,
                      note="Sampled process/file usage, not a hard filesystem quota; no original data removed")
        write_json(receipt, result)
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}))
    if risk or child.returncode:
        raise SystemExit("Scale job did not pass; inspect retained monitor and submission log")


if __name__ == "__main__":
    main()
