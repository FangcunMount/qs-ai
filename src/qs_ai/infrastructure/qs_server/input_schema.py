"""Verify the original QS server-only input schema before using it for validation."""

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.infrastructure.qs_server.output import schema_directory


def load_input_schema(
    *, directory: Path | None = None, version: str = "ai-explanation-input/v1"
) -> dict[str, Any]:
    directory = directory if directory is not None else schema_directory()
    try:
        sources = {
            "ai-explanation-input/v1": ("ai-explanation-input-v1.schema.json", "manifest.json"),
            "ai-explanation-input/v2": (
                "ai-explanation-input-v2.schema.json",
                "mbti-input-manifest.json",
            ),
        }
        schema_file, manifest_file = sources[version]
        raw = (directory / schema_file).read_bytes()
        manifest = json.loads((directory / manifest_file).read_bytes())
        if hashlib.sha256(raw).hexdigest() != manifest["input"]["sha256"]:
            raise ValueError("Input schema checksum mismatch")
        schema = json.loads(raw)
        if (
            not isinstance(schema, dict)
            or schema["properties"]["schema_version"]["const"] != version
        ):
            raise ValueError("Input schema identity mismatch")
        Draft202012Validator.check_schema(schema)
        return schema
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError("Cannot load verified input schema") from None
