import fnmatch
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, TypeVar

T = TypeVar("T")


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
    sample_size: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StreamConfig":
        raw_stale = data.get("stale")
        raw_min_docs_per_window = data.get("min_docs_per_window")
        raw_sample_size = data.get("sample_size")

        stale = int(raw_stale) if raw_stale is not None else None

        min_docs_per_window = (
            int(raw_min_docs_per_window)
            if raw_min_docs_per_window is not None
            else None
        )

        sample_size = int(raw_sample_size) if raw_sample_size is not None else None

        return cls(
            stream=data["stream"],
            stale=stale,
            min_docs_per_window=min_docs_per_window,
            sample_size=sample_size,
        )


@dataclass(frozen=True)
class AppConfig:
    """Root configuration object."""

    default_stale: float
    default_min_docs_per_window: int
    default_sample_size: int
    streams: list[StreamConfig]
    version: int | None = None

    def resolve(
        self,
        stream_name: str,
        override_getter: Callable[[StreamConfig], T | None],
        default_value: T,
    ) -> T:
        """
        Resolves a value for `stream_name`.
        Checks `self.streams` for exact or fnmatch wildcard matches that return
        a non-None override; falls back to `default_value`.
        """
        for stream_cfg in self.streams:
            if fnmatch.fnmatch(stream_name, stream_cfg.stream):
                val = override_getter(stream_cfg)
                if val is not None:
                    return val
        return default_value

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        return cls(
            default_stale=int(data["default_stale"]),
            default_min_docs_per_window=int(data["default_min_docs_per_window"]),
            default_sample_size=data.get("default_sample_size", 3),
            streams=[StreamConfig.from_dict(s) for s in data["streams"]],
            version=data.get("version"),
        )
