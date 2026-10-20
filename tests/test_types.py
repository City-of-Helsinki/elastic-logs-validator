from elastic_logs_validator.types import AppConfig, StreamConfig


def test_app_config_resolve():
    config = AppConfig(
        default_stale=168.0,
        default_min_docs_per_window=24,
        default_sample_size=3,
        streams=[
            StreamConfig(stream="audit-*-api-test-v1", min_docs_per_window=50),
            StreamConfig(stream="logs-*-default", min_docs_per_window=None),
        ],
    )

    # Specific override
    resolved = config.resolve(
        stream_name="audit-yjdh-helsinkilisa-api-test-v1",
        override_getter=lambda cfg: cfg.min_docs_per_window,
        default_value=config.default_min_docs_per_window,
    )
    assert resolved == 50

    # Wildcard fallback to global default
    fallback = config.resolve(
        stream_name="unmatched-stream-v1",
        override_getter=lambda cfg: cfg.min_docs_per_window,
        default_value=config.default_min_docs_per_window,
    )
    assert fallback == 24
