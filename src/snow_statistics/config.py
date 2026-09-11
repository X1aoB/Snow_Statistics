import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    mode: str = "off"
    db: Path = Path("runtime/lite/statistics.db")
    reader_token: str = ""
    server_token: str = ""
    origins: tuple[str, ...] = ("https://xiaob.dev", "https://snow.xiaob.dev")
    allowed_paths: frozenset[str] = field(default_factory=lambda: frozenset({"/", "/statistics/"}))
    allowed_characters: frozenset[str] = field(default_factory=frozenset)
    budget_bytes: int = 2 * 1024**3
    aggregate_interval: float = 60
    max_body_bytes: int = 64 * 1024
    # Optional source-specific instances for local fixtures; never set on the public service.
    source: str = "real"

    def __post_init__(self):
        if self.mode not in {"off", "lite", "full"}:
            raise ValueError("mode must be off, lite or full")
        if self.source not in {"real", "synthetic"}:
            raise ValueError("invalid source")
        if self.reader_token and self.reader_token == self.server_token:
            raise ValueError("reader and server credentials must be separate")
        if self.aggregate_interval <= 0 or self.budget_bytes < 1024 * 1024:
            raise ValueError("invalid resource limits")

    @classmethod
    def from_env(cls):
        def items(key, default=""):
            return tuple(x.strip() for x in os.getenv(key, default).split(",") if x.strip())
        return cls(
            mode=os.getenv("SNOW_MODE", "off"), db=Path(os.getenv("SNOW_DB", "runtime/lite/statistics.db")),
            reader_token=os.getenv("SNOW_READER_TOKEN", ""), server_token=os.getenv("SNOW_SERVER_TOKEN", ""),
            origins=items("SNOW_ORIGINS", "https://xiaob.dev,https://snow.xiaob.dev"),
            allowed_paths=frozenset(items("SNOW_ALLOWED_PATHS", "/,/statistics/")),
            allowed_characters=frozenset(items("SNOW_ALLOWED_CHARACTERS")),
            budget_bytes=int(os.getenv("SNOW_BUDGET_BYTES", str(2 * 1024**3))),
        )
