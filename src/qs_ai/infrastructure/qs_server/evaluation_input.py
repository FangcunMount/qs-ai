"""Validate synthetic provider projections against their explicit input contract.

Suite payloads have no QS report provenance. Validate the frozen facts and, for
the explicit thematic contract, its references, without fabricating source IDs.
"""

import hashlib
import json
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    MBTI_EXPLORATION_INPUT_VERSION,
    MBTI_INPUT_VERSION,
    MBTI_THEMATIC_INPUT_VERSION,
    PUBLISHED_INPUT_VERSION,
    FrozenSuite,
)
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import schema_directory


def validate_suite_input(
    suite: FrozenSuite, reference: FrozenContractRef, payload: dict[str, Any]
) -> None:
    versions = {
        PUBLISHED_INPUT_VERSION: "v1",
        MBTI_INPUT_VERSION: "v2",
        MBTI_THEMATIC_INPUT_VERSION: "v3",
        MBTI_EXPLORATION_INPUT_VERSION: "v4",
    }
    version = versions.get(suite.input_construction_version or "")
    if version is None or suite.input_schema != reference:
        raise ValueError("Evaluation input contract differs from frozen release")
    raw = (schema_directory() / f"ai-explanation-input-{version}.schema.json").read_bytes()
    if (
        reference.id != "ai-explanation-input"
        or reference.version != f"ai-explanation-input/{version}"
        or reference.fingerprint != "sha256:" + hashlib.sha256(raw).hexdigest()
    ):
        raise ValueError("Evaluation input schema differs from registered asset")
    schema = load_input_schema(version=reference.version)
    projection_fields: tuple[str, ...] = ("context", "facts")
    if version in ("v3", "v4"):
        projection_fields += ("reference_material",)
    projection = {
        **schema,
        "required": list(projection_fields),
        "properties": {name: schema["properties"][name] for name in projection_fields},
    }
    # Do not turn a malformed payload into a valid one at dispatch time. Its
    # original bytes and contract must have been frozen before Run creation.
    if not Draft202012Validator(projection).is_valid(payload):
        raise ValueError("Evaluation payload violates frozen input projection")
    if version == "v2":
        from qs_ai.application.interpretation.mbti_input import validate_mbti_projection

        validate_mbti_projection(payload)
    elif version in ("v3", "v4"):
        from qs_ai.application.interpretation.input_values import MBTIThematicInputPolicy
        from qs_ai.application.interpretation.mbti_themes_input import (
            validate_mbti_themes_projection,
        )
        from qs_ai.infrastructure.qs_server.profiles import decode_published_profile

        fixture = json.loads(suite.definition_json)["profile_fixture"]
        profile = decode_published_profile(
            {
                "definition": {
                    k: v for k, v in fixture.items() if k not in {"status", "fingerprint"}
                },
                "status": "published",  # decoder envelope, not a publication or approval
                "fingerprint": fixture["fingerprint"],
            }
        )
        if not isinstance(profile.input_policy, MBTIThematicInputPolicy):
            raise ValueError("Thematic evaluation requires its original frozen Profile")
        validate_mbti_themes_projection(payload, profile.input_policy)


def validate_suite_inputs(suite: FrozenSuite, reference: FrozenContractRef) -> None:
    for case in json.loads(suite.definition_json)["cases"]:
        # The single preflight case intentionally violates dimension bounds
        # and must be rejected before dispatch by run_preflight.
        if case["stage"] == "generation":
            validate_suite_input(suite, reference, case["provider_payload"])
