"""Validate the public example's Compose limits without loading private .env files."""
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
directory = Path(tempfile.mkdtemp(prefix="lite-compose-", dir=root / "runtime"))
(directory / "deploy").mkdir()
shutil.copyfile(root / "deploy/compose.lite.yaml", directory / "deploy/compose.lite.yaml")
shutil.copyfile(root / ".env.example", directory / ".env")
command = ["docker", "compose", "--env-file", str(root / "lab/locks/images.env"), "--env-file", str(directory / ".env"),
           "-f", str(directory / "deploy/compose.lite.yaml"), "config", "--format", "json"]
# Compose interpolation is separate from service env_file resolution.
env = os.environ.copy()
for key in tuple(env):
    if key.startswith("SNOW_"):
        del env[key]
env["SNOW_STATE_DIR"] = str(directory / "state-not-mounted")
rendered = subprocess.run(command, env=env, capture_output=True, text=True, check=True)
service = json.loads(rendered.stdout)["services"]["collector"]
assert int(service["mem_limit"]) == 256 * 1024**2 and float(service["cpus"]) == .25
assert service["environment"]["SNOW_MODE"] == "off"
assert int(service["environment"]["SNOW_BUDGET_BYTES"]) == 512 * 1024**2
result = dict(default_mode="off", memory_bytes=int(service["mem_limit"]), cpus=float(service["cpus"]),
              budget_bytes=int(service["environment"]["SNOW_BUDGET_BYTES"]), source="public .env.example only",
              started_containers=False)
(directory / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
