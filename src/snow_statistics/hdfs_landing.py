"""WebHDFS transport for an isolated NAT laboratory, with content readback.

One OS-locked publisher owns a root. A whole directory becomes visible through
one HDFS rename; callers never scan staging directories or mutable globs.
"""
import time
import uuid
from urllib.parse import urlsplit

import httpx

from snow_statistics.io import digest


class HdfsSink:
    def __init__(self, host, lane, datanodes):
        import re
        if not re.fullmatch(r"[a-z0-9-]{1,60}", lane):
            raise ValueError("Unsafe landing lane")
        self.host = host
        self.datanodes = dict(datanodes) | {ip: ip for ip in datanodes.values()}
        self.path = "/snow/ods/synthetic/kafka/" + lane
        self.root = f"hdfs://{host}:9000" + self.path
        self.client = httpx.Client(timeout=30, follow_redirects=False, trust_env=False)

    def redirect(self, location):
        url = urlsplit(location)
        if url.scheme != "http" or url.port != 9864 or url.hostname not in self.datanodes or url.username or url.password:
            raise ValueError("Redirect is outside this lab's DataNodes")
        return url._replace(netloc=self.datanodes[url.hostname] + ":9864").geturl()

    def request(self, method, path, op, **params):
        response = self.client.request(method, f"http://{self.host}:9870/webhdfs/v1" + path,
                                       params={"op": op, "user.name": "root", **params})
        if response.status_code == 404:
            raise FileNotFoundError(path)
        if response.status_code == 307 and op == "OPEN":
            response = self.client.get(self.redirect(response.headers["location"]))
        response.raise_for_status()
        return response

    def read(self, path):
        return self.request("GET", path, "OPEN").content

    def mkdir(self, path):
        if not self.request("PUT", path, "MKDIRS").json()["boolean"]:
            raise RuntimeError("HDFS mkdir failed")

    def create(self, path, body):
        # CREATE is deliberately two-step: data goes to the returned DataNode.
        response = self.client.put(f"http://{self.host}:9870/webhdfs/v1" + path,
                                   params={"op": "CREATE", "user.name": "root", "overwrite": "false", "replication": 2},
                                   follow_redirects=False)
        if response.status_code != 307:
            response.raise_for_status()
            raise RuntimeError("Missing HDFS CREATE redirect")
        destination = self.redirect(response.headers["location"])
        self.client.put(destination, content=body).raise_for_status()

    def put_directory(self, relative, files):
        final = self.path + "/" + relative
        try:
            status = self.request("GET", final, "GETFILESTATUS")
        except FileNotFoundError:
            status = None
        if status is None:
            staging = self.path + "/_staging/" + uuid.uuid4().hex
            self.mkdir(staging)
            self.mkdir(final.rsplit("/", 1)[0])
            for name, body in files.items():
                self.create(staging + "/" + name, body)
                if digest(self.read(staging + "/" + name)) != digest(body):
                    raise ValueError("HDFS staging readback mismatch")
            if not self.request("PUT", staging, "RENAME", destination=final).json()["boolean"]:
                raise RuntimeError("HDFS atomic directory commit failed")
        # Includes recovery after rename succeeded but the client lost its ACK.
        listing = self.request("GET", final, "LISTSTATUS").json()["FileStatuses"]["FileStatus"]
        if {v["pathSuffix"] for v in listing} != set(files):
            raise ValueError("HDFS committed directory has unexpected files")
        for name, body in files.items():
            if digest(self.read(final + "/" + name)) != digest(body):
                raise ValueError("HDFS immutable content conflict")
            if body:
                deadline = time.monotonic() + 30
                while True:
                    blocks = self.request("GET", final + "/" + name, "GETFILEBLOCKLOCATIONS", offset=0,
                                          length=len(body)).json()["BlockLocations"]["BlockLocation"]
                    if (blocks and sum(b["length"] for b in blocks) == len(body) and
                            all(not b["corrupt"] and len(set(b["hosts"])) >= 2 for b in blocks)):
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("HDFS two-replica gate failed; Kafka offsets unchanged")
                    time.sleep(1)
