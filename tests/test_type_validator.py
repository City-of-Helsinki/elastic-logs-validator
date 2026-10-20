from unittest.mock import MagicMock

import pytest

from elastic_logs_validator.types import (
    StreamSample,
)
from elastic_logs_validator.validators.type_validator import (
    RequiredField,
    StreamMapping,
    StreamTypeValidationReport,
    StreamTypeValidator,
)

# --- Fixtures ---


@pytest.fixture
def mock_es_client():
    return MagicMock()


@pytest.fixture
def validator(mock_es_client):
    return StreamTypeValidator(client=mock_es_client)


@pytest.fixture
def sample_mapping_raw():
    return {
        "properties": {
            "@timestamp": {"type": "date"},
            "service": {
                "properties": {
                    "name": {"type": "keyword"},
                    "version": {"type": "keyword"},
                }
            },
            "http": {
                "properties": {
                    "response": {"properties": {"status_code": {"type": "long"}}}
                }
            },
            "tags": {"type": "keyword"},
        }
    }


# --- StreamMapping Unit Tests ---


def test_stream_mapping_flatten_properties(sample_mapping_raw):
    mapping = StreamMapping(stream_name="test-stream", raw_mappings=sample_mapping_raw)

    assert mapping.flat_properties["@timestamp"] == "date"
    assert mapping.flat_properties["service"] == "object"
    assert mapping.flat_properties["service.name"] == "keyword"
    assert mapping.flat_properties["http.response.status_code"] == "long"
    assert mapping.flat_properties["tags"] == "keyword"


def test_stream_mapping_validate_schema_success(sample_mapping_raw):
    mapping = StreamMapping(stream_name="test-stream", raw_mappings=sample_mapping_raw)
    required = [
        RequiredField(path="@timestamp", expected_type="date"),
        RequiredField(path="service.name", expected_type=["keyword", "text"]),
    ]

    violations = mapping.validate_schema(required)
    assert violations == []


def test_stream_mapping_validate_schema_violations(sample_mapping_raw):
    mapping = StreamMapping(stream_name="test-stream", raw_mappings=sample_mapping_raw)
    required = [
        RequiredField(path="missing.field", expected_type="keyword"),
        RequiredField(path="http.response.status_code", expected_type="keyword"),
    ]

    violations = mapping.validate_schema(required)
    assert len(violations) == 2

    assert violations[0].reason == "MISSING_MAPPING"
    assert violations[0].field_path == "missing.field"

    assert violations[1].reason == "MAPPING_TYPE_MISMATCH"
    assert violations[1].field_path == "http.response.status_code"
    assert violations[1].actual_type == "long"
    assert violations[1].expected_type == "keyword"


# --- StreamTypeValidator Unit Tests ---


def test_validate_skips_empty_samples(validator, mock_es_client):
    samples = {
        "active-stream": StreamSample(
            stream_name="active-stream", documents=[{"@timestamp": "2026-01-01"}]
        ),
        "empty-stream": StreamSample(stream_name="empty-stream", documents=[]),
    }

    mock_es_client.indices.get_mapping.return_value = {
        "active-stream": {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
    }

    report = validator.validate(samples)

    assert isinstance(report, StreamTypeValidationReport)
    assert report.skipped_streams == ["empty-stream"]
    assert report.valid_streams == ["active-stream"]
    assert not report.has_violations


def test_validate_unmapped_streams(validator, mock_es_client):
    samples = {
        "unmapped-stream": StreamSample(
            stream_name="unmapped-stream", documents=[{"some": "data"}]
        )
    }

    # ES returns empty response or no key for stream
    mock_es_client.indices.get_mapping.return_value = {}

    report = validator.validate(samples)

    assert report.unmapped_streams == ["unmapped-stream"]
    assert report.valid_streams == []


def test_validate_document_type_mismatches(validator, mock_es_client):
    samples = {
        "audit-stream": StreamSample(
            stream_name="audit-stream",
            documents=[
                {
                    # string instead of long
                    "http": {"response": {"status_code": "500"}},
                    # bool instead of keyword
                    "service": {"name": True},
                }
            ],
        )
    }

    mock_es_client.indices.get_mapping.return_value = {
        "audit-stream": {
            "mappings": {
                "properties": {
                    "http": {
                        "properties": {
                            "response": {
                                "properties": {"status_code": {"type": "long"}}
                            }
                        }
                    },
                    "service": {"properties": {"name": {"type": "keyword"}}},
                }
            }
        }
    }

    report = validator.validate(samples)

    assert report.has_violations
    assert len(report.violations) == 2

    reasons = [v.reason for v in report.violations]
    paths = [v.field_path for v in report.violations]

    assert set(reasons) == {"DOC_TYPE_MISMATCH"}
    assert "http.response.status_code" in paths
    assert "service.name" in paths


def test_validate_array_and_nested_objects(validator, mock_es_client):
    samples = {
        "logs-stream": StreamSample(
            stream_name="logs-stream",
            documents=[
                {
                    "tags": ["prod", 123, "api"],  # Int allowed for keyword by checker
                    "items": [{"id": "abc"}, {"id": 999}],
                }
            ],
        )
    }

    mock_es_client.indices.get_mapping.return_value = {
        "logs-stream": {
            "mappings": {
                "properties": {
                    "tags": {"type": "keyword"},
                    "items": {"properties": {"id": {"type": "keyword"}}},
                }
            }
        }
    }

    report = validator.validate(samples)

    assert not report.has_violations
    assert report.valid_streams == ["logs-stream"]


def test_concrete_backing_index_fallback(validator, mock_es_client):
    """
    Tests backing index resolution (.ds-<stream_name>-*) when
    ES doesn't alias the top key.
    """
    samples = {
        "audit-logs": StreamSample(
            stream_name="audit-logs", documents=[{"action": "LOGIN"}]
        )
    }

    # ES returns the concrete backing index name instead of alias key
    mock_es_client.indices.get_mapping.return_value = {
        ".ds-audit-logs-2026.08.01-000001": {
            "mappings": {"properties": {"action": {"type": "keyword"}}}
        }
    }

    report = validator.validate(samples)

    assert report.valid_streams == ["audit-logs"]
    assert not report.unmapped_streams
