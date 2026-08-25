from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fnmatch

from elasticsearch8 import Elasticsearch

from elastic_logs_validator.types import AppConfig


@dataclass(frozen=True)
class StreamVolumeResult:
    stream_name: str
    doc_count: int
    expected_min: int
    passed: bool


@dataclass(frozen=True)
class VolumeValidationReport:
    window_days: int
    total_streams: int
    failed_streams: int
    results: dict[str, StreamVolumeResult]


class StreamVolumeValidator:
    """
    Validates that Elasticsearch data streams meet configured minimum
    document thresholds over a time window.
    """

    def __init__(self, client: Elasticsearch) -> None:
        self.client = client

    def _resolve_target_min(
            self, stream_name: str, config: AppConfig
        ) -> int | None:
            """
            Resolves min_weekly_docs for a concrete stream name.
            Matches exact names or fnmatch wildcard patterns defined in AppConfig.
            """
            # 1. Look for explicit or wildcard stream matches in config.streams
            for stream_cfg in config.streams:
                if fnmatch.fnmatch(stream_name, stream_cfg.stream):
                    if stream_cfg.min_docs_per_window is not None:
                        return stream_cfg.min_docs_per_window

            # 2. Fall back to global default if no stream-level override matched
            return config.default_min_docs_per_window

    def validate_streams(
        self,
        stream_names: list[str],
        window_days: int,
        config: AppConfig,
        timestamp_field: str = "@timestamp",
    ) -> VolumeValidationReport:
        """Checks total document count per stream configured in AppConfig."""
        now = datetime.now(timezone.utc)
        gte_time = (now - timedelta(days=window_days)).isoformat()

        results: dict[str, StreamVolumeResult] = {}
        failed_count = 0

        for stream_name in stream_names:
            # Resolve expected minimum threshold for this specific stream
            expected_min = self._resolve_target_min(stream_name, config)

            # Skip volume evaluation if no stream or default threshold exists
            if expected_min is None:
                continue

            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {timestamp_field: {"gte": gte_time}}},
                        ]
                    }
                }
            }

            response = self.client.count(index=stream_name, body=query)
            count = response.get("count", 0)
            passed = count >= expected_min

            if not passed:
                failed_count += 1

            results[stream_name] = StreamVolumeResult(
                stream_name=stream_name,
                doc_count=count,
                expected_min=expected_min,
                passed=passed,
            )

        return VolumeValidationReport(
            window_days=window_days,
            total_streams=len(results),
            failed_streams=failed_count,
            results=results,
        )
