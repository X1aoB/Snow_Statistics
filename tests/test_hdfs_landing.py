import httpx
import pytest

from snow_statistics.hdfs_landing import HdfsSink


class WebHdfs:
    def __init__(self):
        self.files, self.directories = {}, set()
        self.fail_create, self.lose_rename_ack, self.replicas = False, False, 2

    def __call__(self, request):
        path = request.url.path.removeprefix("/webhdfs/v1")
        op = request.url.params["op"]
        if op == "MKDIRS":
            self.directories.add(path)
            return httpx.Response(200, json={"boolean": True})
        if op == "GETFILESTATUS":
            return httpx.Response(200, json={}) if path in self.directories else httpx.Response(404)
        if op == "CREATE" and request.url.port == 9870 or op == "OPEN" and request.url.port == 9870:
            return httpx.Response(307, headers={"location": "http://snow-compute:9864/webhdfs/v1" + path + "?op=" + op})
        if op == "CREATE":
            assert path not in self.files  # No overwrite, including retries.
            if self.fail_create:
                self.fail_create = False
                self.files[path] = b"partial"
                raise httpx.ReadTimeout("interrupted upload")
            self.files[path] = request.content
            return httpx.Response(201)
        if op == "OPEN":
            return httpx.Response(200, content=self.files[path])
        if op == "RENAME":
            destination = request.url.params["destination"]
            assert destination not in self.directories
            for key in list(self.files):
                if key.startswith(path + "/"):
                    self.files[destination + key[len(path):]] = self.files.pop(key)
            self.directories.remove(path)
            self.directories.add(destination)
            if self.lose_rename_ack:
                self.lose_rename_ack = False
                raise httpx.ReadTimeout("lost rename acknowledgement")
            return httpx.Response(200, json={"boolean": True})
        if op == "LISTSTATUS":
            names = [key.rsplit("/", 1)[1] for key in self.files if key.rsplit("/", 1)[0] == path]
            return httpx.Response(200, json={"FileStatuses": {"FileStatus": [{"pathSuffix": n} for n in names]}})
        if op == "GETFILEBLOCKLOCATIONS":
            return httpx.Response(200, json={"BlockLocations": {"BlockLocation": [
                dict(length=len(self.files[path]), hosts=["node" + str(i) for i in range(self.replicas)], corrupt=False)]}})
        raise AssertionError(op)


def sink():
    fs = WebHdfs()
    result = HdfsSink("192.0.2.1", "test", {"snow-compute": "192.0.2.2"})
    result.client.close()
    result.client = httpx.Client(transport=httpx.MockTransport(fs))
    return result, fs


def test_incomplete_upload_never_exposes_final_directory_and_retry_is_immutable():
    client, fs = sink()
    fs.fail_create = True
    files = {"raw.jsonl": b"raw", "events.jsonl": b"events"}
    with pytest.raises(httpx.ReadTimeout):
        client.put_directory("batches/one", files)
    assert client.path + "/batches/one" not in fs.directories
    client.put_directory("batches/one", files)
    client.put_directory("batches/one", files)
    with pytest.raises(ValueError, match="conflict"):
        client.put_directory("batches/one", files | {"events.jsonl": b"changed"})
    client.client.close()


def test_lost_rename_ack_and_insufficient_replicas(monkeypatch):
    client, fs = sink()
    fs.lose_rename_ack = True
    with pytest.raises(httpx.ReadTimeout):
        client.put_directory("batches/one", {"raw.jsonl": b"raw"})
    client.put_directory("batches/one", {"raw.jsonl": b"raw"})
    fs.replicas = 1
    times = iter((0, 31))
    monkeypatch.setattr("snow_statistics.hdfs_landing.time.monotonic", lambda: next(times))
    with pytest.raises(TimeoutError, match="two-replica"):
        client.put_directory("batches/one", {"raw.jsonl": b"raw"})
    client.client.close()


def test_only_configured_datanodes_receive_payloads():
    client, _ = sink()
    assert client.redirect("http://snow-compute:9864/path?op=OPEN") == "http://192.0.2.2:9864/path?op=OPEN"
    for target in ("http://unrelated:9864/path", "https://snow-compute:9864/path", "http://snow-compute:80/path",
                   "http://user:password@snow-compute:9864/path"):
        with pytest.raises(ValueError):
            client.redirect(target)
    client.client.close()
