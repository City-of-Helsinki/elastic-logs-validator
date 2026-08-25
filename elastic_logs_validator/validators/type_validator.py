from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class RequiredField:
    path: str
    # Accepts string ("keyword"), list/tuple (["keyword", "text"]) or None (existence)
    expected_type: str | Sequence[str] | None = None

    @property
    def allowed_types(self) -> set[str]:
        if self.expected_type is None:
            return set()
        if isinstance(self.expected_type, str):
            return {self.expected_type}
        return set(self.expected_type)

    @property
    def expected_type_display(self) -> str:
        if not self.expected_type:
            return ""
        if isinstance(self.expected_type, str):
            return self.expected_type
        return " | ".join(self.expected_type)


@dataclass(frozen=True)
class MappingViolation:
    stream_name: str
    field_path: str
    reason: str  # "MISSING_MAPPING" | "MAPPING_TYPE_MISMATCH" | "DOC_TYPE_MISMATCH"
    expected_type: str | None = None
    actual_type: str | None = None

    def __str__(self) -> str:
        if self.reason == "MISSING_MAPPING":
            req = (
                f" (expected type '{self.expected_type}')" if self.expected_type else ""
            )
            return (
                f"[{self.stream_name}] Mapping missing required field "
                f"'{self.field_path}'{req}"
            )
        if self.reason == "MAPPING_TYPE_MISMATCH":
            return (
                f"[{self.stream_name}] Mapping field '{self.field_path}' is "
                f"'{self.actual_type}', expected '{self.expected_type}'"
            )
        return (
            f"[{self.stream_name}] Document field '{self.field_path}' value has type "
            f"'{self.actual_type}', mapping expects '{self.expected_type}'"
        )


@dataclass(frozen=True)
class StreamTypeValidationReport:
    valid_streams: list[str] = field(default_factory=list)
    skipped_streams: list[str] = field(default_factory=list)
    unmapped_streams: list[str] = field(default_factory=list)
    violations: list[MappingViolation] = field(default_factory=list)

    @property
    def invalid_streams(self) -> list[str]:
        """Returns unique stream names that contain type violations."""
        return sorted({v.stream_name for v in self.violations})

    @property
    def has_violations(self) -> bool:
        return bool(self.violations)

    def summary(self) -> str:
        if self.has_violations:
            return (
                f"FAIL: {len(self.violations)} violation(s) found across "
                f"{len(self.valid_streams)} valid stream(s)."
            )
        if not self.valid_streams:
            return (
                f"WARNING: No active documents available to validate "
                f"({len(self.skipped_streams)} stream(s) skipped)."
            )
        return (
            f"OK: {len(self.valid_streams)} stream(s) passed validation "
            f"({len(self.skipped_streams)} skipped due to empty samples)."
        )


class StreamMapping:
    """Parses raw ES mapping data and provides a flattened dot-notation lookup."""

    def __init__(self, stream_name: str, raw_mappings: dict[str, Any]) -> None:
        self.stream_name = stream_name
        self.raw_properties = raw_mappings.get("properties", {})
        self.flat_properties = self._flatten_properties(self.raw_properties)

    @classmethod
    def _flatten_properties(
        cls, node: dict[str, Any], prefix: str = ""
    ) -> dict[str, str]:
        flat: dict[str, str] = {}
        for key, val in node.items():
            path = f"{prefix}{key}"
            if "properties" in val:
                flat[path] = "object"
                flat.update(
                    cls._flatten_properties(val["properties"], prefix=f"{path}.")
                )
            else:
                flat[path] = val.get("type", "object")
        return flat

    def validate_schema(
        self, required_fields: list[RequiredField]
    ) -> list[MappingViolation]:
        violations: list[MappingViolation] = []
        for req in required_fields:
            if req.path not in self.flat_properties:
                violations.append(
                    MappingViolation(
                        stream_name=self.stream_name,
                        field_path=req.path,
                        reason="MISSING_MAPPING",
                        expected_type=req.expected_type_display,
                    )
                )
                continue

            actual_type = self.flat_properties[req.path]
            allowed = req.allowed_types

            # If allowed_types is non-empty, ensure actual_type is one of them
            if allowed and actual_type not in allowed:
                violations.append(
                    MappingViolation(
                        stream_name=self.stream_name,
                        field_path=req.path,
                        reason="MAPPING_TYPE_MISMATCH",
                        expected_type=req.expected_type_display,
                        actual_type=actual_type,
                    )
                )
        return violations


class StreamTypeValidator:
    """
    Validates ES mappings against required dot-notated paths and
    checks document samples against mapping types.
    """

    TYPE_CHECKERS = {
        # Allow ints alongside strings for keyword if numeric IDs are present
        "keyword": lambda v: isinstance(v, (str, int)),
        "text": lambda v: isinstance(v, str),
        "long": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "short": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "byte": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "double": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "date": lambda v: isinstance(v, str),
        "object": lambda v: isinstance(v, dict),
    }

    def __init__(
        self,
        client: Any,
        required_fields: list[RequiredField] | None = None,
    ) -> None:
        self.client = client
        self.required_fields = required_fields or []

    def validate(self, samples: dict[str, Any]) -> StreamTypeValidationReport:
        skipped_streams = [
            s for s, sample in samples.items() if not getattr(sample, "documents", None)
        ]
        active_streams = [
            s for s, sample in samples.items() if getattr(sample, "documents", None)
        ]

        if not active_streams:
            return StreamTypeValidationReport(skipped_streams=skipped_streams)

        mappings = self._fetch_stream_mappings(active_streams)

        valid_streams: list[str] = []
        unmapped_streams: list[str] = []
        all_violations: list[MappingViolation] = []

        for stream_name in active_streams:
            sample = samples[stream_name]
            mapping = mappings.get(stream_name)

            if not mapping:
                unmapped_streams.append(stream_name)
                continue

            stream_violations: list[MappingViolation] = []

            # 1. Validate required field schema on the ES mapping
            if self.required_fields:
                stream_violations.extend(mapping.validate_schema(self.required_fields))

            # 2. Validate sample document values against ES mapping definitions
            for doc in sample.documents:
                stream_violations.extend(
                    self._check_node(stream_name, doc, mapping.raw_properties)
                )

            if stream_violations:
                all_violations.extend(stream_violations)
            else:
                valid_streams.append(stream_name)

        return StreamTypeValidationReport(
            valid_streams=valid_streams,
            skipped_streams=skipped_streams,
            unmapped_streams=unmapped_streams,
            violations=all_violations,
        )

    def _fetch_stream_mappings(self, streams: list[str]) -> dict[str, StreamMapping]:
        """Fetches mappings and maps ES response keys back to data stream names."""
        res = self.client.indices.get_mapping(
            index=streams,
            ignore_unavailable=True,
            expand_wildcards=["open", "hidden"],
        )
        stream_mappings: dict[str, StreamMapping] = {}

        for requested_stream in streams:
            # Direct hit
            if requested_stream in res:
                raw = res[requested_stream].get("mappings", {})
                stream_mappings[requested_stream] = StreamMapping(requested_stream, raw)
                continue

            # Concrete backing index fallback (.ds-<stream_name>-*)
            for concrete_index, details in res.items():
                if (
                    concrete_index.startswith(f".ds-{requested_stream}-")
                    or concrete_index == requested_stream
                ):
                    raw = details.get("mappings", {})
                    stream_mappings[requested_stream] = StreamMapping(
                        requested_stream, raw
                    )
                    break

        return stream_mappings

    def _check_node(
        self,
        stream_name: str,
        doc_node: dict[str, Any],
        mapping_node: dict[str, Any],
        prefix: str = "",
    ) -> list[MappingViolation]:
        violations: list[MappingViolation] = []

        if not isinstance(doc_node, dict):
            return violations

        for key, value in doc_node.items():
            path = f"{prefix}{key}"
            field_mapping = mapping_node.get(key)

            if not field_mapping or value is None:
                continue

            # Handle object hierarchies
            if "properties" in field_mapping and isinstance(value, dict):
                violations.extend(
                    self._check_node(
                        stream_name,
                        value,
                        field_mapping["properties"],
                        prefix=f"{path}.",
                    )
                )
                continue

            # Handle array items by checking element types
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and "properties" in field_mapping:
                        violations.extend(
                            self._check_node(
                                stream_name,
                                item,
                                field_mapping["properties"],
                                prefix=f"{path}.",
                            )
                        )
                    else:
                        v = self._check_value_type(
                            stream_name, path, item, field_mapping
                        )
                        if v:
                            violations.append(v)
                continue

            # Check scalar field value type
            v = self._check_value_type(stream_name, path, value, field_mapping)
            if v:
                violations.append(v)

        return violations

    def _check_value_type(
        self,
        stream_name: str,
        path: str,
        value: Any,
        field_mapping: dict[str, Any],
    ) -> MappingViolation | None:
        expected_type = field_mapping.get("type")
        if not expected_type:
            return None

        checker = self.TYPE_CHECKERS.get(expected_type)
        if checker and not checker(value):
            return MappingViolation(
                stream_name=stream_name,
                field_path=path,
                reason="DOC_TYPE_MISMATCH",
                expected_type=expected_type,
                actual_type=type(value).__name__,
            )

        return None
