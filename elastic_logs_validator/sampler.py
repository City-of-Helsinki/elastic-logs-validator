import random
from typing import Any

from elasticsearch8 import Elasticsearch

from .types import StreamSample, StreamState


class StreamSampler:
    """Fetches random document samples per resolved stream for manual review."""

    def __init__(self, client: Elasticsearch) -> None:
        self.client = client

    def sample(
        self,
        state_map: dict[str, StreamState],
        sample_size: int = 3,
        seed: int | None = None,
    ) -> dict[str, StreamSample]:
        """
        Fetches `sample_size` random documents for each active stream in `state_map`.
        Uses a single _msearch network call.
        """
        # Filter out empty streams that have no docs to sample
        active_streams = [
            name for name, state in state_map.items() if state.doc_count > 0
        ]
        if not active_streams:
            return {}

        # Use an explicit seed so reviews are reproducible if needed
        run_seed = seed if seed is not None else random.randint(1, 1_000_000)

        searches: list[dict[str, Any]] = []
        for stream in active_streams:
            # Header
            searches.append(
                {
                    "index": stream,
                    "ignore_unavailable": True,
                }
            )
            # Body using function_score + random_score
            searches.append(
                {
                    "size": sample_size,
                    "query": {
                        "function_score": {
                            "query": {"match_all": {}},
                            "random_score": {
                                "seed": run_seed,
                                "field": "_seq_no",
                            },
                        }
                    },
                }
            )

        msearch_resp = self.client.msearch(searches=searches)
        samples: dict[str, StreamSample] = {}

        for stream, res in zip(active_streams, msearch_resp["responses"]):
            hits = res.get("hits", {}).get("hits", [])
            docs = [h["_source"] for h in hits]
            samples[stream] = StreamSample(stream_name=stream, documents=docs)

        return samples
