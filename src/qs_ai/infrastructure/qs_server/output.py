import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.output import InvalidOutput


def schema_directory() -> Path:
    bundled = Path(__file__).resolve().parent / "schema_assets"
    if bundled.is_dir():
        return bundled
    return Path(__file__).resolve().parents[4] / "integrations" / "qs_server" / "schemas"


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidOutput("duplicate_json_field")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise InvalidOutput("invalid_json_constant")


class QSOutputParser:
    def __init__(self, directory: Path | None = None) -> None:
        directory = directory if directory is not None else schema_directory()
        raw = (directory / "ai-explanation-output-v1.schema.json").read_bytes()
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
            raise InvalidOutput("schema_checksum_mismatch")
        schema = json.loads(raw)
        if not isinstance(schema, dict):
            raise InvalidOutput("schema_document_invalid")
        self._schema = schema
        Draft202012Validator.check_schema(schema)
        self._validator = Draft202012Validator(schema)

    def schema(self) -> dict[str, Any]:
        return deepcopy(self._schema)

    def parse(self, raw: str) -> dict[str, Any]:
        try:
            raw.encode("utf-8")
            value = json.loads(raw, object_pairs_hook=_object, parse_constant=_reject_constant)
        except (TypeError, ValueError):
            raise InvalidOutput("invalid_json") from None
        if not isinstance(value, dict) or not self._validator.is_valid(value):
            raise InvalidOutput("output_schema_invalid")
        return value
