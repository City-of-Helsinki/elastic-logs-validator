from datetime import datetime, timedelta, timezone

import pytest

from elastic_logs_validator.types import AppConfig, StreamConfig, StreamState
from elastic_logs_validator.validators.age_validator import (
    AgeEvaluatedStream,
    AgeValidationReport,
    StreamAgeValidator,
)

# --- Fixtures ---


@pytest.fixture
def now_utc():
    return datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_config():
    return AppConfig(
        default_stale=24.0,
        default_min_docs_per_window=10,
        default_sample_size=3,
        streams=[
            StreamConfig(stream="audit-*-v1", stale=72.0),
            StreamConfig(stream="audit-fast-v1", stale=12.0),
            StreamConfig(stream="unmonitored-v1", stale=None),
        ],
    )


# --- Unit Tests ---


def test_age_evaluated_stream_last_seen_str(now_utc):
    """Tests timestamp formatting for output string rendering."""
    stream_never = AgeEvaluatedStream(
        name="empty-stream", age=0.0, stale=24.0, last_seen_ts=None
    )
    assert stream_never.last_seen_str == "NEVER"

    dt = datetime(2026, 8, 25, 12, 34, 56, 789123, tzinfo=timezone.utc)
    stream_active = AgeEvaluatedStream(
        name="active-stream", age=1.5, stale=24.0, last_seen_ts=dt
    )
    assert stream_active.last_seen_str == "2026-08-25T12:34:56.789Z"


def test_validate_healthy_and_stale_streams(sample_config, now_utc, monkeypatch):
    """
    Tests calculation of stream age, threshold
    evaluation, and state classification.
    """
    # Freeze time in validator logic
    monkeypatch.setattr(
        "elastic_logs_validator.validators.age_validator.datetime",
        type("FrozenDateTime", (), {"now": classmethod(lambda cls, tz=None: now_utc)}),
    )

    state_map = {
        # 10 hours old, threshold = 72.0h (wildcard audit-*-v1) -> Healthy
        "audit-normal-v1": StreamState(
            name="audit-normal-v1",
            doc_count=100,
            last_seen_ts=now_utc - timedelta(hours=10),
        ),
        # 15 hours old, threshold = 12.0h (exact match audit-fast-v1) -> Stale
        "audit-fast-v1": StreamState(
            name="audit-fast-v1",
            doc_count=50,
            last_seen_ts=now_utc - timedelta(hours=15),
        ),
        # 30 hours old, threshold = 24.0h (fallback default) -> Stale
        "general-logs-v1": StreamState(
            name="general-logs-v1",
            doc_count=200,
            last_seen_ts=now_utc - timedelta(hours=30),
        ),
    }

    validator = StreamAgeValidator(default_stale_hours=24.0)
    report = validator.validate(state_map=state_map, config=sample_config)

    assert isinstance(report, AgeValidationReport)
    assert report.has_failures is True

    # Healthy streams
    assert len(report.healthy) == 1
    assert report.healthy[0].name == "audit-normal-v1"
    assert report.healthy[0].age == 10.0
    assert report.healthy[0].stale == 72.0

    # Stale streams
    assert len(report.stale) == 2
    stale_names = {s.name for s in report.stale}
    assert stale_names == {"audit-fast-v1", "general-logs-v1"}


def test_validate_empty_and_unseen_streams(sample_config, now_utc):
    """
    Tests that streams with zero docs or missing
    last_seen_ts are classified as empty.
    """
    state_map = {
        "zero-doc-stream": StreamState(
            name="zero-doc-stream",
            doc_count=0,
            last_seen_ts=now_utc - timedelta(hours=1),
        ),
        "never-seen-stream": StreamState(
            name="never-seen-stream",
            doc_count=100,
            last_seen_ts=None,
        ),
    }

    validator = StreamAgeValidator(default_stale_hours=24.0)
    report = validator.validate(state_map=state_map, config=sample_config)

    assert report.has_failures is True
    assert len(report.empty) == 2
    assert set(report.empty) == {"zero-doc-stream", "never-seen-stream"}
    assert len(report.healthy) == 0
    assert len(report.stale) == 0


def test_resolve_threshold_precedence(sample_config):
    """
    Tests threshold resolution order:
    exact match -> wildcard pattern -> default fallback.
    """
    validator = StreamAgeValidator(default_stale_hours=24.0)
    config_map = validator._build_config_map(sample_config.streams)

    # Exact match
    assert validator._resolve_threshold("audit-fast-v1", config_map) == 12.0

    # Wildcard pattern match
    assert validator._resolve_threshold("audit-custom-v1", config_map) == 72.0

    # Explicit None stale_val falls back to validator default
    assert validator._resolve_threshold("unmonitored-v1", config_map) == 24.0

    # Unmatched stream falls back to validator default
    assert validator._resolve_threshold("random-stream", config_map) == 24.0
