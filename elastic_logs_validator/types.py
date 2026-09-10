from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class StreamState:
    """In-memory snapshot of a data stream's state fetched from Elastic."""

    name: str
    exists: bool = True
    last_seen_ts: datetime | None = None
    doc_count: int = 0
    health_status: str | None = None  # GREEN, YELLOW, RED


@dataclass(frozen=True)
class StreamSample:
    stream_name: str
    documents: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class StreamConfig:
    """Per datastream config."""

    stream: str
    stale: int | None = None
    min_docs_per_window: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StreamConfig":
        raw_stale = data.get("stale")
        raw_min_docs_per_window = data.get("min_docs_per_window")

        stale = int(raw_stale) if raw_stale is not None else None

        min_docs_per_window = (
            int(raw_min_docs_per_window)
            if raw_min_docs_per_window is not None
            else None
        )

        return cls(
            stream=data["stream"], stale=stale, min_docs_per_window=min_docs_per_window
        )


@dataclass(frozen=True)
class AppConfig:
    """Root configuration object."""

    default_stale: float
    default_min_docs_per_window: int
    streams: list[StreamConfig]
    version: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        return cls(
            default_stale=int(data["default_stale"]),
            default_min_docs_per_window=int(data["default_min_docs_per_window"]),
            streams=[StreamConfig.from_dict(s) for s in data["streams"]],
            version=data["version"],
        )
