import json
import sys
import threading
from types import SimpleNamespace

import httpx
import pytest

from snow_statistics.simulator import generate
from snow_statistics.sync import kafka_sync, sync_once


def test_target_change_cannot_reuse_cursor_or_fetch(tmp_path):
    identity = dict(url="http://localhost:8100", lane="one")
    def fetch(_):
        return dict(events=[], next_cursor=0)
    assert sync_once(tmp_path, fetch, None, identity=identity) == 0
    before = (tmp_path / "target.json").read_bytes()
    with pytest.raises(ValueError, match="changed"):
        sync_once(tmp_path, lambda _: pytest.fail("must not fetch another source"), None,
                  identity=dict(url="http://localhost:8100", lane="two"))
    assert (tmp_path / "target.json").read_bytes() == before
    assert sync_once(tmp_path, fetch, None, identity=identity) == 0


def test_unbound_archive_cannot_be_silently_adopted(tmp_path):
    (tmp_path / "cursor.json").write_text(json.dumps(dict(cursor=5)))
    with pytest.raises(ValueError, match="no target identity"):
        sync_once(tmp_path, lambda _: pytest.fail("must not fetch"), None, identity=dict(lane="new"))
    assert json.loads((tmp_path / "cursor.json").read_bytes())["cursor"] == 5
    assert not (tmp_path / "target.json").exists()


@pytest.mark.parametrize("source", ["synthetic", "real"])
def test_follow_reuses_transport_and_stops_without_skipping(tmp_path, monkeypatch, source):
    row = generate(users=1)["events"][0]
    stop = threading.Event()
    created, published, closed, fetched = [], [], [], []
    class Producer:
        def __init__(self, **kwargs):
            created.append(kwargs)
        def send(self, topic, **kwargs):
            published.append((topic, kwargs["value"]["seq"]))
            return SimpleNamespace(get=lambda timeout: None)
        def close(self, **kwargs):
            closed.append(True)
    monkeypatch.setitem(sys.modules, "kafka", SimpleNamespace(KafkaProducer=Producer))
    def handle(request):
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json=dict(schema_version=1, source="real",
                instance_id="00000000-0000-0000-0000-000000000001",
                generation="00000000-0000-0000-0000-000000000002", earliest_available_seq=1,
                latest_accepted_seq=1, expired_through=0, aggregate_cursor=1))
        fetched.append(int(request.url.params["after"]))
        return httpx.Response(200, json=dict(events=[row] if len(fetched) == 1 else [], next_cursor=1))
    real_client = httpx.Client
    monkeypatch.setattr("snow_statistics.sync.httpx.Client",
                        lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(handle)))
    def on_batch(n):
        if not n:
            stop.set()
    options = dict(lane="followtest", source=source, follow=True, poll_seconds=.1, stop=stop, on_batch=on_batch)
    if source == "synthetic":
        assert kafka_sync("http://localhost:8100", "private-reader-token", "localhost:9092", tmp_path, **options) == 1
        assert fetched == [0, 1] and published == [("snow.synthetic.followtest.events.v1", 1)]
        assert json.loads((tmp_path / "cursor.json").read_bytes())["cursor"] == 1
    else:
        with pytest.raises(ValueError, match="Unexpected source"):
            kafka_sync("http://localhost:8100", "private-reader-token", "localhost:9092", tmp_path, **options)
        assert not published and not (tmp_path / "pending.json").exists()
        assert not (tmp_path / "cursor.json").exists()
    assert len(created) == 1 and closed == [True]
    assert "private-reader-token" not in (tmp_path / "target.json").read_text()
