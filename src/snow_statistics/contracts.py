from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BUSINESS_TZ = ZoneInfo("Asia/Hong_Kong")
Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")]


def utcnow():
    return datetime.now(UTC)


def instant(value: datetime):
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def business_day(value: datetime):
    return value.astimezone(BUSINESS_TZ).date().isoformat()


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=False)
    schema_version: Literal[1] = 1
    event_id: UUID
    app: Literal["mywebsite", "project_snow"]
    event_type: Literal["page_view", "character_select", "entry_click", "entry_arrival", "request_observed", "request_complete"]
    occurred_at: datetime
    anonymous_id: UUID | None = None
    session_id: UUID | None = None
    path: str | None = Field(default=None, max_length=200)
    character_id: Identifier | None = None
    jump_id: UUID | None = None
    channel: Identifier | None = None
    request_id: Identifier | None = None
    success: bool | None = Field(default=None, strict=True)
    elapsed_ms: int | None = Field(default=None, ge=0, le=86_400_000, strict=True)

    @field_validator("occurred_at")
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None:
            raise ValueError("timezone required")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def shape(self):
        base = {"schema_version", "event_id", "app", "event_type", "occurred_at"}
        shapes = {
            "page_view": ({"path"}, {"path", "anonymous_id", "session_id"}),
            "character_select": ({"character_id"}, {"character_id", "anonymous_id", "session_id"}),
            "entry_click": ({"jump_id", "channel"}, {"jump_id", "channel", "anonymous_id", "session_id"}),
            "entry_arrival": ({"jump_id"}, {"jump_id", "anonymous_id", "session_id"}),
            "request_observed": ({"request_id"}, {"request_id", "anonymous_id", "session_id"}),
            "request_complete": ({"request_id", "character_id", "success", "elapsed_ms"},
                                 {"request_id", "character_id", "success", "elapsed_ms"}),
        }
        required, allowed = shapes[self.event_type]
        present = {k for k, v in self.model_dump().items() if v is not None} - base
        if not required <= present or not present <= allowed:
            raise ValueError("fields do not match event type")
        if self.event_type in {"character_select", "request_complete", "request_observed", "entry_arrival"} and self.app != "project_snow":
            raise ValueError("event requires project_snow")
        if self.event_type == "entry_click" and self.app != "mywebsite":
            raise ValueError("entry_click requires mywebsite")
        return self


class Batch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[Event] = Field(min_length=1, max_length=50)


class Daily(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app: Literal["mywebsite", "project_snow"]
    date: str
    pv: int
    uv: int
    requests: int
    successes: int
    success_rate: float | None


class Popularity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app: Literal["mywebsite", "project_snow"]
    date: str
    kind: Literal["page", "character"]
    name: str
    count: int


class Summary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    generated_at: str | None
    date_from: str | None
    date_to: str | None
    status: Literal["ok", "empty", "stale", "unavailable", "archived"]
    completeness: Literal["accepted_events_only"] = "accepted_events_only"
    daily: list[Daily]
    popularity: list[Popularity]


class AccessMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pv: int = Field(ge=0, strict=True)
    uv: int = Field(ge=10, strict=True)


class QualityMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")
    requests: int = Field(ge=10, strict=True)
    successes: int = Field(ge=0, strict=True)
    success_rate: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def protected_counts(self):
        failures = self.requests - self.successes
        if failures < 0 or self.successes not in (0,) and self.successes < 10 or 0 < failures < 10:
            raise ValueError("quality count below public threshold")
        if abs(self.success_rate - self.successes / self.requests) > 1e-12:
            raise ValueError("quality rate inconsistent")
        return self


class HeatMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["page", "character"]
    name: str = Field(max_length=200)
    count: int = Field(ge=10, strict=True)


class ProtectedGroup[T](BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: Literal["published", "suppressed", "pending", "empty"]
    value: T | None

    @model_validator(mode="after")
    def hidden_is_null(self):
        if (self.state == "published") != (self.value is not None):
            raise ValueError("only published groups may contain values")
        return self


class PublicDay(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app: Literal["mywebsite", "project_snow"]
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    cutoff_at: str
    access: ProtectedGroup[AccessMetrics]
    quality: ProtectedGroup[QualityMetrics]
    popularity: ProtectedGroup[list[HeatMetric]]


class PublicPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    timezone: Literal["Asia/Hong_Kong"] = "Asia/Hong_Kong"
    publication_time: Literal["00:15"] = "00:15"
    delay_days: Literal[3] = 3
    threshold: Literal[10] = 10
    frozen_days: Literal[True] = True
    stale_after_seconds: Literal[93600] = 93600


class PublicSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[2] = 2
    generated_at: str | None
    cutoff_at: str | None
    date_from: str | None
    date_to: str | None
    status: Literal["ok", "empty", "stale", "unavailable", "archived"]
    completeness: Literal["accepted_events_only"] = "accepted_events_only"
    policy: PublicPolicy = Field(default_factory=PublicPolicy)
    daily: list[PublicDay] = Field(max_length=180)
