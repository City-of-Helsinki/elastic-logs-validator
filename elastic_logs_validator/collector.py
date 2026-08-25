from dataclasses import dataclass, field
from typing import Any

from elasticsearch8 import Elasticsearch, NotFoundError

from .types import StreamConfig, StreamState


@dataclass(frozen=True)
class CollectorResult:
    """Strongly-typed snapshot of the cluster state."""

    state_map: dict[str, StreamState] = field(default_factory=dict)
    missing_patterns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _MetadataResolution:
    known_streams: set[str] = field(default_factory=set)
    missing_patterns: list[str] = field(default_factory=list)


class StreamMetadataCollector:
    """Fetches a complete, in-memory snapshot of stream state in minimal ES calls."""

    def __init__(self, client: Elasticsearch) -> None:
        self.client = client

    def collect(self, stream_configs: list[StreamConfig]) -> CollectorResult:
        target_patterns = [cfg.stream.strip() for cfg in stream_configs]
        if not target_patterns:
            return CollectorResult()

        # 1. Bulk Metadata Call
        resolution = self._resolve_metadata(target_patterns)
        if not resolution.known_streams:
            return CollectorResult(missing_patterns=resolution.missing_patterns)

        # 2. Bulk Aggregation Call
        state_map = self._query_freshness_and_counts(resolution.known_streams)

        return CollectorResult(
            state_map=state_map, missing_patterns=resolution.missing_patterns
        )

    def _resolve_metadata(self, target_patterns: list[str]) -> _MetadataResolution:
        known_streams: set[str] = set()
        missing_patterns: list[str] = []

        try:
            resp = self.client.indices.get_data_stream(
                name=target_patterns,
                expand_wildcards=["open", "hidden"],
            )
            for s in resp.get("data_streams", []):
                known_streams.add(s["name"])
            return _MetadataResolution(known_streams, missing_patterns)

        except NotFoundError:
            # Fallback isolation on 404
            for pattern in target_patterns:
                try:
                    sub_resp = self.client.indices.get_data_stream(
                        name=pattern,
                        expand_wildcards=["open", "hidden"],
                    )
                    for s in sub_resp.get("data_streams", []):
                        known_streams.add(s["name"])
                except NotFoundError:
                    missing_patterns.append(pattern)

        return _MetadataResolution(known_streams, missing_patterns)

    def _query_freshness_and_counts(
        self, known_streams: set[str]
    ) -> dict[str, StreamState]:
        search_resp = self.client.search(
            index=list(known_streams),
            ignore_unavailable=True,
            expand_wildcards=["open", "hidden"],
            size=0,
            aggs={
                "by_data_stream": {
                    "terms": {
                        "field": "_data_stream",
                        "size": len(known_streams) + 100,
                    },
                    "aggs": {
                        "latest_doc": {
                            "top_metrics": {
                                "metrics": [{"field": "@timestamp"}],
                                "sort": {"@timestamp": "desc"},
                            }
                        }
                    },
                }
            },
        )

        state_map: dict[str, StreamState] = {}
        seen_streams: set[str] = set()

        for bucket in search_resp["aggregations"]["by_data_stream"]["buckets"]:
            stream_name = bucket["key"]
            seen_streams.add(stream_name)

            doc_count = bucket["doc_count"]
            metrics = bucket["latest_doc"]["top_metrics"]
            raw_ts = metrics[0]["metrics"]["@timestamp"] if metrics else None

            state_map[stream_name] = StreamState(
                name=stream_name,
                exists=True,
                last_seen_ts=raw_ts,
                doc_count=doc_count,
            )

        # Handle streams that yielded 0 aggregation buckets
        unseen_streams = known_streams - seen_streams
        if unseen_streams:
            fallback_states = self._fallback_msearch(unseen_streams)
            state_map.update(fallback_states)

        return state_map

    def _fallback_msearch(self, unseen_streams: set[str]) -> dict[str, StreamState]:
        searches: list[dict[str, Any]] = []
        stream_list = list(unseen_streams)

        for stream in stream_list:
            searches.append(
                {
                    "index": stream,
                    "ignore_unavailable": True,
                    "expand_wildcards": ["open", "hidden"],
                }
            )
            searches.append({"size": 1, "sort": [{"@timestamp": "desc"}]})

        msearch_resp = self.client.msearch(searches=searches)
        states: dict[str, StreamState] = {}

        for stream, res in zip(stream_list, msearch_resp["responses"]):
            hits = res.get("hits", {}).get("hits", [])
            raw_ts = hits[0]["_source"].get("@timestamp") if hits else None
            doc_count = res.get("hits", {}).get("total", {}).get("value", 0)

            states[stream] = StreamState(
                name=stream,
                exists=True,
                last_seen_ts=raw_ts,
                doc_count=doc_count,
            )

        return states
