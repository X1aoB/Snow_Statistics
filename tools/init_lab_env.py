"""Generate independent local lab credentials; never print their values."""
import argparse
import os
import secrets
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--control", required=True)
parser.add_argument("--compute", default="")
parser.add_argument("--analysis", default="")
args = parser.parse_args()
path = Path("lab/.env")
if path.exists():
    raise SystemExit("lab/.env already exists; refusing to rotate credentials implicitly")
values = {"CONTROL_IP": args.control, "COMPUTE_IP": args.compute, "ANALYSIS_IP": args.analysis}
for key in ("LAB_MYSQL_PASSWORD", "LAB_MYSQL_ROOT_PASSWORD", "LAB_CDC_PASSWORD", "LAB_GOVERNANCE_PASSWORD"):
    values[key] = secrets.token_urlsafe(32)
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
print("Generated independent lab credentials (values hidden)")
