from unittest.mock import MagicMock

import pytest
from elasticsearch8 import Elasticsearch

from elastic_logs_validator.types import AppConfig, StreamConfig
from elastic_logs_validator.validators.volume_validator import (
    StreamVolumeValidator,
    VolumeValidationReport,
)

# --- Fixtures ---


@pytest.fixture
def mock_es_client():
    return MagicMock(spec=Elasticsearch)


@pytest.fixture
def validator(mock_es_client):
    return StreamVolumeValidator(client=mock_es_client)


@pytest.fixture
def sample_config():
    return AppConfig(
        default_stale=168.0,
        default_min_docs_per_window=24,
        default_sample_size=3,
        streams=[
            StreamConfig(stream="audit-*-api-test-v1", min_docs_per_window=50),
            StreamConfig(stream="logs-*-default", min_docs_per_window=None),
        ],
    )


@pytest.fixture
def unmonitored_config():
    return AppConfig(
        default_stale=168.0,
        default_min_docs_per_window=None,  # No global fallback threshold
        default_sample_size=3,
        streams=[
            StreamConfig(stream="audit-*-v1", min_docs_per_window=100),
            StreamConfig(stream="unmonitored-*-v1", min_docs_per_window=None),
        ],
    )


# --- Volume Validator Unit Tests ---


def test_validate_streams_evaluation_and_report(
    validator, mock_es_client, sample_config
):
    """
    Verifies that the validator:
    1. Evaluates fetched document counts against config-resolved thresholds.
    2. Correctly flags passed vs failed streams.
    3. Returns a complete VolumeValidationReport instance.
    """
    mock_es_client.count.side_effect = [
        {"count": 60},  # Passes (60 >= 50)
        {"count": 10},  # Fails  (10 < 24 default)
    ]

    discovered_streams = [
        "audit-yjdh-helsinkilisa-api-test-v1",
        "logs-debug-app-default",
    ]

    report = validator.validate_streams(
        stream_names=discovered_streams,
        config=sample_config,
        window_days=7,
    )

    assert isinstance(report, VolumeValidationReport)
    assert report.total_streams == 2
    assert report.failed_streams == 1
    assert report.window_days == 7

    res_audit = report.results["audit-yjdh-helsinkilisa-api-test-v1"]
    assert res_audit.expected_min == 50
    assert res_audit.doc_count == 60
    assert res_audit.passed is True

    res_debug = report.results["logs-debug-app-default"]
    assert res_debug.expected_min == 24
    assert res_debug.doc_count == 10
    assert res_debug.passed is False


def test_validate_streams_skips_unmonitored_streams(
    validator, mock_es_client, unmonitored_config
):
    """Tests that streams with no resolved threshold (None) are bypassed entirely."""
    mock_es_client.count.return_value = {"count": 150}

    discovered_streams = [
        "audit-app-v1",
        "unmonitored-app-v1",
    ]

    report = validator.validate_streams(
        stream_names=discovered_streams,
        config=unmonitored_config,
        window_days=7,
    )

    assert report.total_streams == 1
    assert "audit-app-v1" in report.results
    assert "unmonitored-app-v1" not in report.results
    assert mock_es_client.count.call_count == 1


def test_query_structure_and_window_days(validator, mock_es_client, sample_config):
    """
    Tests that ES count API calls are framed with
    the correct range filter and index.
    """
    mock_es_client.count.return_value = {"count": 50}

    validator.validate_streams(
        stream_names=["test-stream-v1"],
        config=sample_config,
        window_days=14,
        timestamp_field="@custom_timestamp",
    )

    mock_es_client.count.assert_called_once()
    _, kwargs = mock_es_client.count.call_args

    assert kwargs["index"] == "test-stream-v1"
    query = kwargs["body"]["query"]["bool"]["filter"][0]
    assert "@custom_timestamp" in query["range"]
