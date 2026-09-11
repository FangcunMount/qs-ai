"""Verify the original QS server-only input schema before using it for validation."""

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.infrastructure.qs_server.output import schema_directory


def load_input_schema(*, directory: Path | None = None) -> dict[str, Any]:
    directory = directory if directory is not None else schema_directory()
    try:
        raw = (directory / "ai-explanation-input-v1.schema.json").read_bytes()
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if hashlib.sha256(raw).hexdigest() != manifest["input"]["sha256"]:
            raise ValueError("Input schema checksum mismatch")
        schema = json.loads(raw)
        if not isinstance(schema, dict) or schema["properties"]["schema_version"]["const"] != (
            "ai-explanation-input/v1"
        ):
            raise ValueError("Input schema identity mismatch")
        Draft202012Validator.check_schema(schema)
        return schema
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError("Cannot load verified input schema") from None
