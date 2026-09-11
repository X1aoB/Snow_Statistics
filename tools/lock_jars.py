import hashlib
import json
from pathlib import Path

import httpx

root = Path(__file__).resolve().parents[1]
artifacts = {
    "iceberg-spark-runtime-3.5_2.12-1.10.0.jar": "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0",
}
folder = root / "runtime/jars"
folder.mkdir(parents=True, exist_ok=True)
records = {}
with httpx.Client(timeout=60, follow_redirects=True) as client:
    for name, path in artifacts.items():
        url = "https://repo.maven.apache.org/maven2/" + path + "/" + name
        response = client.get(url)
        response.raise_for_status()
        body = response.content
        expected = client.get(url + ".sha1")
        expected.raise_for_status()
        assert hashlib.sha1(body).hexdigest() == expected.text.strip()
        (folder / name).write_bytes(body)
        records[name] = {"url": url, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
(root / "lab/locks/jars.json").write_text(json.dumps(records, indent=2) + "\n", newline="\n")
print("Verified and locked " + str(len(records)) + " JAR artifact")
