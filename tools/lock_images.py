"""Resolve official image tags to registry digests without Docker or pulling layers."""
import json
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def resolve(ref, client):
    if ref.startswith("quay.io/"):
        registry, name = "quay.io", ref[len("quay.io/"):]
    else:
        registry, name = "registry-1.docker.io", ref
    repository, tag = name.rsplit(":", 1)
    if "/" not in repository:
        repository = "library/" + repository
    headers = {"Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"}
    if registry == "registry-1.docker.io":
        response = client.get("https://auth.docker.io/token", params={"service": "registry.docker.io", "scope": f"repository:{repository}:pull"})
        response.raise_for_status()
        headers["Authorization"] = "Bearer " + response.json()["token"]
    response = client.get(f"https://{registry}/v2/{repository}/manifests/{tag}", headers=headers)
    response.raise_for_status()
    checksum = response.headers["Docker-Content-Digest"]
    base = ref.rsplit(":", 1)[0]
    return base + "@" + checksum


def main():
    inputs = json.loads((ROOT / "lab/images.json").read_text())
    result, errors = {}, {}
    with httpx.Client(timeout=45, follow_redirects=True) as client:
        for key, ref in inputs.items():
            try:
                result[key] = {"tag": ref, "image": resolve(ref, client)}
                print(key + " locked", flush=True)
            except Exception as error:
                errors[key] = type(error).__name__
                print(key + " unavailable: " + type(error).__name__, flush=True)
    folder = ROOT / "lab/locks"
    folder.mkdir(exist_ok=True)
    (folder / "images.json").write_text(json.dumps({"images": result, "unresolved": errors}, indent=2) + "\n", newline="\n")
    (folder / "images.env").write_text("\n".join(f"{key}={value['image']}" for key, value in result.items()) + "\n", newline="\n")
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
