import json
import os
from pathlib import Path
from typing import Any

import jsonschema
from jsonschema import SchemaError, ValidationError

from .types import AppConfig


def get_env(key: str, default: str | None = None) -> str:
    """
    Loads environment value. If not found, use default value.

    :return: Environment value
    :raises RuntimeError: If the key does not exist and default is not provided.
    """
    try:
        return os.environ[key]
    except KeyError:
        if default is not None:
            return default

    raise RuntimeError(f"Missing required ENV var: {key}") from None


def load_schema(schema_path: Path | str) -> dict[str, Any]:
    """Loads and returns the JSON schema dict from disk."""

    schema_path = Path(schema_path)
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"Schema definition file missing at path: {schema_path.resolve()}"
        )
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Syntax error in JSON schema file '{schema_path}': "
            f"line {e.lineno}, col {e.colno}: {e.msg}"
        ) from e


def load_config(config_path: Path | str, schema: dict[str, Any]) -> AppConfig:
    """
    Loads JSON configuration from disk and validates it against DataStreamConfigSchema.

    :param config_path: Explicit path to the configuration file.
    :return: An immutable AppConfig instance.
    :raises FileNotFoundError: If the configuration target path does not exist.
    :raises ValueError: If the JSON is malformed or violates the JSON Schema contract.
    :raises RuntimeError: If the schema itself is invalid.
    """
    config_path = Path(config_path)

    if not config_path.is_file():
        raise FileNotFoundError(
            f"Configuration file missing at path: {config_path.resolve()}"
        )

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Syntax error parsing JSON in '{config_path}': "
            f"line {e.lineno}, col {e.colno}: {e.msg}"
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"Failed reading configuration file '{config_path}': {e}"
        ) from e

    try:
        jsonschema.validate(instance=raw_data, schema=schema)
    except ValidationError as e:
        field_path = " -> ".join(str(p) for p in e.absolute_path) or "root"
        raise ValueError(
            f"Configuration validation failed at [{field_path}]: {e.message}"
        ) from e
    except SchemaError as e:
        raise RuntimeError(
            f"Meta-schema violation in internal schema definition: {e.message}"
        ) from e

    return AppConfig.from_dict(raw_data)
