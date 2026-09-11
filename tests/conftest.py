from datetime import UTC, datetime
from uuid import uuid4

import pytest

from snow_statistics.config import Settings
from snow_statistics.contracts import Event
from snow_statistics.store import Store


@pytest.fixture
def now():
    return datetime(2026, 9, 11, 12, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path):
    return Settings(mode="lite", db=tmp_path / "stats.db", reader_token="reader-test-only",
                    server_token="server-test-only", allowed_characters=frozenset({"sample_character"}))


@pytest.fixture
def store(settings, now):
    instance = Store(settings, clock=lambda: now)
    yield instance
    instance.close()


@pytest.fixture
def page(now):
    return Event(event_id=uuid4(), app="mywebsite", event_type="page_view", occurred_at=now, path="/", anonymous_id=uuid4())
