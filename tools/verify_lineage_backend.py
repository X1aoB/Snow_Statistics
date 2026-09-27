"""Read back Marquez run states and graph identities for the actual journal."""
import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from snow_statistics.io import write_json  # noqa: E402
from snow_statistics.lineage import NAMESPACE as SYNTHETIC_NAMESPACE  # noqa: E402
from snow_statistics.lineage import Journal
from snow_statistics.real_lineage import NAMESPACE as REAL_NAMESPACE  # noqa: E402
from snow_statistics.real_lineage import RealJournal, validate_event

parser = argparse.ArgumentParser()
parser.add_argument("--database", type=Path, default=Path("runtime/lineage/delivery.sqlite"))
parser.add_argument("--url", default="http://127.0.0.1:5000")
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--source", choices=("synthetic", "real"), default="synthetic")
args = parser.parse_args()
endpoint = urlsplit(args.url)
if endpoint.scheme != "http" or endpoint.hostname not in ("127.0.0.1", "localhost") or endpoint.username or endpoint.password or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment:
    raise ValueError("A local API or verified SSH tunnel is required")


def get(path):
    with urlopen(args.url.rstrip("/") + path, timeout=10) as response:
        data = response.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError("Unbounded lineage API response")
        return json.loads(data)


journal = RealJournal(args.database) if args.source == "real" else Journal(args.database)
namespace = REAL_NAMESPACE if args.source == "real" else SYNTHETIC_NAMESPACE
with journal.connection() as db:
    events = [json.loads(row[0]) for row in db.execute("SELECT payload FROM events ORDER BY seq")]
if not events:
    raise ValueError("No recorded executions to verify")
for event in events:
    if event["job"]["namespace"] != namespace:
        raise ValueError("Lineage journal mixes source namespaces")
    if args.source == "real":
        validate_event(event)
expected = {e["run"]["runId"]: e for e in events}
results = []
for run_id, event in expected.items():
    run = get("/api/v1/jobs/runs/" + run_id)
    target = {"START": "RUNNING", "COMPLETE": "COMPLETED", "FAIL": "FAILED"}[event["eventType"]]
    if run["state"] != target:
        raise ValueError("Marquez run state mismatch")
    outputs = {(d["datasetVersionId"]["namespace"], d["datasetVersionId"]["name"]) for d in run["outputDatasetVersions"]}
    inputs = {(d["datasetVersionId"]["namespace"], d["datasetVersionId"]["name"]) for d in run["inputDatasetVersions"]}
    if inputs != {(d["namespace"], d["name"]) for d in event["inputs"]}:
        raise ValueError("Marquez run input identity mismatch")
    if outputs != {(d["namespace"], d["name"]) for d in event["outputs"]}:
        raise ValueError("Marquez run output identity mismatch")
    results.append(dict(run_id=run_id, job=event["job"]["name"], state=run["state"], output_count=len(outputs)))
graph_job = events[-1]["job"]["name"] if args.source == "real" else "snow_models.publish"
graph = get("/api/v1/lineage?" + urlencode(dict(nodeId="job:" + namespace + ":" + graph_job, depth=10)))
nodes = sorted([dict(id=n["id"], type=n["type"]) for n in graph["graph"]], key=lambda n: n["id"])
edges = sorted({(edge["origin"], edge["destination"]) for node in graph["graph"] for edge in node["inEdges"] + node["outEdges"]})
if not nodes or not edges:
    raise ValueError("Missing Marquez lineage graph")
result = dict(engine="Marquez 0.50.0", namespace=namespace, source=args.source, events=len(events), runs=results,
              graph=dict(nodes=nodes, edges=[dict(origin=a, destination=b) for a, b in edges]))
write_json(args.output, result)
print(json.dumps(dict(verified_runs=len(results), verified_nodes=len(nodes), verified_edges=len(edges))))
