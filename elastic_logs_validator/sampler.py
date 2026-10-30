import random
from typing import Any

from elasticsearch8 import Elasticsearch

from .types import AppConfig, StreamSample, StreamState


class StreamSampler:
    """Fetches random document samples per resolved stream for manual review."""

    def __init__(self, client: Elasticsearch) -> None:
        self.client = client

    def sample(
        self,
        state_map: dict[str, StreamState],
        config: AppConfig,
        seed: int | None = None,
    ) -> dict[str, StreamSample]:
        """
        Fetches random documents for each active stream, guaranteeing at least
        one of each unique operation type, then filling up to sample_size.
        """
        active_streams = [
            name for name, state in state_map.items() if state.doc_count > 0
        ]
        if not active_streams:
            return {}

        run_seed = seed if seed is not None else random.randint(1, 1_000_000)

        searches: list[dict[str, Any]] = []
        stream_sample_sizes: dict[str, int] = {}

        for stream in active_streams:
            sample_size = config.resolve(
                stream, lambda s: s.sample_size, config.default_sample_size
            )
            stream_sample_sizes[stream] = sample_size
            
            # Fetch a larger pool of random candidates to ensure we capture 
            # all rare operation types plus enough filler for the sample size.
            fetch_size = max(sample_size * 5, 100)

            searches.append(
                {
                    "index": stream,
                    "ignore_unavailable": True,
                }
            )
            searches.append(
                {
                    "size": fetch_size,
                    "query": {
                        "function_score": {
                            "query": {"match_all": {}},
                            "random_score": {
                                "seed": run_seed,
                                "field": "_seq_no"
                            }
                        }
                    }
                }
            )

        msearch_resp = self.client.msearch(searches=searches)
        samples: dict[str, StreamSample] = {}

        for stream, res in zip(active_streams, msearch_resp["responses"]):
            hits = res.get("hits", {}).get("hits", [])
            raw_docs = [h["_source"] for h in hits if "_source" in h]
            target_size = stream_sample_sizes[stream]

            # Group documents by operation type
            grouped: dict[str, list[dict[str, Any]]] = {}
            for doc in raw_docs:
                op = doc.get("audit_event", {}).get("operation", "UNKNOWN")
                if op not in grouped:
                    grouped[op] = []
                grouped[op].append(doc)

            selected_docs: list[dict[str, Any]] = []
            remaining_pool: list[dict[str, Any]] = []

            # Phase 1: Guarantee at least 1 document for every unique operation type found
            for _, group_docs in grouped.items():
                selected_docs.append(group_docs[0])
                remaining_pool.extend(group_docs[1:])

            # Phase 2: Fill the remaining slots up to the target sample_size
            needed = target_size - len(selected_docs)
            if needed > 0 and remaining_pool:
                selected_docs.extend(remaining_pool[:needed])
            elif needed < 0:
                # If we found more unique operation types than the total sample size allows,
                # truncate gracefully to match the target size.
                selected_docs = selected_docs[:target_size]

            samples[stream] = StreamSample(stream_name=stream, documents=selected_docs)

        return samples