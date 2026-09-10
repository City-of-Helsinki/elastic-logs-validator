import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from elasticsearch8 import Elasticsearch

from elastic_logs_validator.validators.volume_validator import (
    StreamVolumeValidator,
    VolumeValidationReport,
)

from .collector import StreamMetadataCollector
from .report_generator import ReportGenerator
from .sampler import StreamSampler
from .utils import get_env, load_config, load_schema
from .validators.age_validator import AgeValidationReport, StreamAgeValidator
from .validators.type_validator import (
    RequiredField,
    StreamTypeValidationReport,
    StreamTypeValidator,
)

load_dotenv()

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

required_fields = list(
    [
        RequiredField(path="@timestamp", expected_type="date"),
        RequiredField(path="audit_event.actor"),
        RequiredField(path="audit_event.date_time", expected_type="date"),
        RequiredField(path="audit_event.operation", expected_type=["keyword", "text"]),
        RequiredField(path="audit_event.origin", expected_type="constant_keyword"),
        RequiredField(path="audit_event.target"),
        RequiredField(
            path="audit_event.environment",
            expected_type=["constant_keyword", "keyword"],
        ),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample Elasticsearch data streams and generate inspection reports."
    )

    # Optional flag with choices: defaults to 'none' for head-less/CI runs
    parser.add_argument(
        "--generate-report",
        choices=["html", "json", "none"],
        default="none",
        nargs="?",
        const="html",
        help=(
            "Generate a review report file on disk. "
            "If passed without a format, defaults to 'html'. "
            "Options: html, json, none."
        ),
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports"),
        help="Directory where generated report files will be written.",
    )

    return parser.parse_args()


logger = logging.getLogger(__name__)


def print_age_report(report: AgeValidationReport) -> None:
    """
    Prints stream recency / freshness validation results in aligned tabular format.
    """
    total_streams = len(report.healthy) + len(report.stale) + len(report.empty)

    logger.info("--- Age Validation Summary ---")
    logger.info(
        "Total: %d | Healthy: %d | Stale: %d | Empty: %d",
        total_streams,
        len(report.healthy),
        len(report.stale),
        len(report.empty),
    )

    for s in report.healthy:
        age_str = f"{s.age:.1f}h" if isinstance(s.age, (int, float)) else f"{s.age}h"
        thresh_str = (
            f"{s.stale:.1f}h" if isinstance(s.stale, (int, float)) else f"{s.stale}h"
        )
        logger.info(
            "  └─ HEALTHY: %-45s | Age: %-8s | Threshold: %-7s | Last Seen: %s",
            s.name,
            age_str,
            thresh_str,
            s.last_seen_str,
        )

    for s in report.stale:
        age_str = f"{s.age:.1f}h" if isinstance(s.age, (int, float)) else f"{s.age}h"
        thresh_str = (
            f"{s.stale:.1f}h" if isinstance(s.stale, (int, float)) else f"{s.stale}h"
        )
        logger.warning(
            "  └─ STALE:   %-45s | Age: %-8s | Threshold: %-7s | Last Seen: %s",
            s.name,
            age_str,
            thresh_str,
            s.last_seen_str,
        )

    for name in report.empty:
        logger.error(
            "  └─ EMPTY:   %-45s | Age: %-8s | Threshold: %-7s | Last Seen: %s",
            name,
            "N/A",
            "N/A",
            "NEVER (0 docs)",
        )


def print_type_report(report: StreamTypeValidationReport) -> None:
    """Prints schema and field mapping validation results in aligned tabular format."""
    invalid_streams = sorted({v.stream_name for v in report.violations})
    total_streams = (
        len(report.valid_streams)
        + len(invalid_streams)
        + len(report.unmapped_streams)
        + len(report.skipped_streams)
    )

    logger.info("--- Type Mapping Validation Summary ---")
    logger.info(
        "Total: %d | Valid: %d | Invalid: %d | "
        "Unmapped: %d | Skipped: %d | Violations: %d",
        total_streams,
        len(report.valid_streams),
        len(invalid_streams),
        len(report.unmapped_streams),
        len(report.skipped_streams),
        len(report.violations),
    )

    for stream_name in report.valid_streams:
        logger.info(
            "  └─ VALID:    %-45s | Field: %-20s | Expected: %-10s | Actual: %-10s",
            stream_name,
            "ALL_MATCH",
            "OK",
            "OK",
        )

    for v in report.violations:
        stream_name = getattr(v, "stream_name", "unknown")
        field_name = getattr(v, "field", "unknown")
        exp_type = getattr(v, "expected_type", "unknown")
        act_type = getattr(v, "actual_type", "unknown")
        logger.error(
            "  └─ MISMATCH: %-45s | Field: %-20s | Expected: %-10s | Actual: %-10s",
            stream_name,
            field_name,
            exp_type,
            act_type,
        )

    for stream_name in report.unmapped_streams:
        logger.warning(
            "  └─ UNMAPPED: %-45s | Status: No target mapping defined",
            stream_name,
        )

    for stream_name in report.skipped_streams:
        logger.info(
            "  └─ SKIPPED:  %-45s | Status: Skipped by filter/config",
            stream_name,
        )


def print_volume_report(report: VolumeValidationReport) -> None:
    """Prints volume / throughput validation results in aligned tabular format."""
    logger.info("--- Volume Validation Summary ---")
    logger.info(
        "Total: %d | Passed: %d | Failed: %d",
        report.total_streams,
        report.total_streams - report.failed_streams,
        report.failed_streams,
    )

    sorted_results = sorted(
        report.results.items(),
        key=lambda item: (not item[1].passed, item[0]),
    )

    for stream_name, res in sorted_results:
        if res.passed:
            logger.info(
                "  └─ PASS: %-45s | Count: %-6d | Target: %-6d | Surplus: +%d",
                stream_name,
                res.doc_count,
                res.expected_min,
                res.doc_count - res.expected_min,
            )
        else:
            deficit = res.expected_min - res.doc_count
            logger.warning(
                "  └─ FAIL: %-45s | Count: %-6d | Target: %-6d | Deficit: -%d",
                stream_name,
                res.doc_count,
                res.expected_min,
                deficit,
            )


def is_validation_successful(
    age_report: AgeValidationReport,
    type_report: StreamTypeValidationReport,
    volume_report: VolumeValidationReport | None = None,
) -> bool:
    if age_report.has_failures:
        return False
    if type_report.has_violations:
        return False
    if volume_report and volume_report.failed_streams > 0:
        return False
    return True


def main() -> None:
    args = parse_args()

    host = get_env("ES_HOST")
    api_key = get_env("ES_API_KEY")

    assets_path = Path(__file__).parent / "assets"
    schema = load_schema(assets_path / "app_config_schema.json")
    config = load_config("./config.json", schema=schema)

    client = Elasticsearch(host, api_key=api_key)
    collector = StreamMetadataCollector(client=client)
    snapshot = collector.collect(config.streams)

    # Handle missing targets
    if snapshot.missing_patterns:
        for pattern in snapshot.missing_patterns:
            logger.error("MISSING: pattern '%s' not found", pattern)

    sampler = StreamSampler(client=client)
    samples = sampler.sample(snapshot.state_map, sample_size=3)

    age_validator = StreamAgeValidator(default_stale_hours=config.default_stale)
    age_report = age_validator.validate(
        state_map=snapshot.state_map, stream_configs=config.streams
    )

    # 3. Validate types against live stream mapping
    type_validator = StreamTypeValidator(client=client, required_fields=required_fields)
    type_report = type_validator.validate(samples)

    volume_validator = StreamVolumeValidator(client=client)
    volume_report = volume_validator.validate_streams(
        stream_names=list(samples.keys()),
        window_days=7,
        config=config
    )

    print_age_report(age_report)
    print_type_report(type_report)
    print_volume_report(volume_report)

    reporter = ReportGenerator(output_dir=args.output_dir, assets_dir=assets_path)
    summary = reporter.generate_report(
        samples=samples, fmt=args.generate_report, volume_report=volume_report
    )

    logger.info(
        f"Covered {summary.total_streams} streams and "
        f"{summary.total_documents} total documents."
    )

    if summary.output_path:
        logger.info(f"Report successfully written to: {summary.output_path}")

    # Exit code strategy
    if not is_validation_successful(age_report, type_report, volume_report):
        logger.error("Validation failed.")
        sys.exit(1)

    logger.info("Validation completed successfully.")


if __name__ == "__main__":
    main()
