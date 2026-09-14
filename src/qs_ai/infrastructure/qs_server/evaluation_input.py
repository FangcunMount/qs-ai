"""Validate synthetic provider projections against their explicit input contract.

Suite payloads have no QS report provenance. Check only context/facts, retaining
the exact definitions of the frozen schema rather than fabricating source IDs.
"""

import hashlib
import json
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.qs_server.evaluation_suite import PUBLISHED_INPUT_VERSION, FrozenSuite
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import schema_directory


def validate_suite_input(
    suite: FrozenSuite, reference: FrozenContractRef, payload: dict[str, Any]
) -> None:
    if (
        suite.input_construction_version != PUBLISHED_INPUT_VERSION
        or suite.input_schema != reference
    ):
        raise ValueError("Evaluation input contract differs from frozen release")
    raw = (schema_directory() / "ai-explanation-input-v1.schema.json").read_bytes()
    if (
        reference.id != "ai-explanation-input"
        or reference.version != "ai-explanation-input/v1"
        or reference.fingerprint != "sha256:" + hashlib.sha256(raw).hexdigest()
    ):
        raise ValueError("Evaluation input schema differs from registered asset")
    schema = load_input_schema()
    projection = {
        **schema,
        "required": ["context", "facts"],
        "properties": {name: schema["properties"][name] for name in ("context", "facts")},
    }
    # Do not turn a malformed payload into a valid one at dispatch time. Its
    # original bytes and contract must have been frozen before Run creation.
    if not Draft202012Validator(projection).is_valid(payload):
        raise ValueError("Evaluation payload violates frozen input projection")


def validate_suite_inputs(suite: FrozenSuite, reference: FrozenContractRef) -> None:
    for case in json.loads(suite.definition_json)["cases"]:
        # The single preflight case intentionally violates dimension bounds
        # and must be rejected before dispatch by run_preflight.
        if case["stage"] == "generation":
            validate_suite_input(suite, reference, case["provider_payload"])
