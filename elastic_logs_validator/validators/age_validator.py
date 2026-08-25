import fnmatch
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ..types import StreamConfig, StreamState


@dataclass(frozen=True)
class AgeEvaluatedStream:
    name: str
    age: float  # Age in hours
    stale: float  # Applied threshold in hours
    last_seen: str  # ISO 8601 timestamp string


@dataclass
class AgeValidationReport:
    """Report strictly containing freshness rule outcomes."""

    healthy: list[AgeEvaluatedStream] = field(default_factory=list)
    stale: list[AgeEvaluatedStream] = field(default_factory=list)
    empty: list[str] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        """Returns True if any streams are stale or empty."""
        return bool(self.stale or self.empty)


class StreamAgeValidator:
    """Pure in-memory age rule engine. Zero knowledge of collector mechanics."""

    def __init__(self, default_stale_hours: float = 24.0) -> None:
        self.default_stale_hours = float(default_stale_hours)

    def validate(
        self,
        state_map: dict[str, StreamState],
        stream_configs: list[StreamConfig],
    ) -> AgeValidationReport:
        now = datetime.now(timezone.utc)
        config_map = self._build_config_map(stream_configs)

        report = AgeValidationReport()

        for stream_name, state in state_map.items():
            if state.doc_count == 0 or not state.last_seen_ts:
                report.empty.append(stream_name)
                continue

            threshold_hours = self._resolve_threshold(stream_name, config_map)
            latest_ts = datetime.fromisoformat(
                state.last_seen_ts.replace("Z", "+00:00")
            )
            age_seconds = (now - latest_ts).total_seconds()
            age_hours = round(age_seconds / 3600.0, 2)

            entry = AgeEvaluatedStream(
                name=stream_name,
                age=age_hours,
                stale=threshold_hours,
                last_seen=state.last_seen_ts,
            )

            if age_seconds > (threshold_hours * 3600):
                report.stale.append(entry)
            else:
                report.healthy.append(entry)

        return report

    def _build_config_map(self, stream_configs: list[StreamConfig]) -> dict[str, float]:
        config_map: dict[str, float] = {}

        for item in stream_configs:
            name = item.stream
            if not name:
                continue
            stale_val = item.stale
            stale_hours = (
                self.default_stale_hours if stale_val is None else float(stale_val)
            )
            config_map[name.strip()] = stale_hours

        return config_map

    def _resolve_threshold(
        self, stream_name: str, config_map: dict[str, float]
    ) -> float:
        if stream_name in config_map:
            return config_map[stream_name]

        for pattern, threshold in config_map.items():
            if fnmatch.fnmatchcase(stream_name, pattern):
                return threshold

        return self.default_stale_hours
