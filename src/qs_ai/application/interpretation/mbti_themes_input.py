"""Add frozen thematic references to the unchanged authoritative MBTI facts."""

import hashlib
import json
from dataclasses import replace
from typing import Any

from qs_ai.application.interpretation.input_values import (
    AssembledInput,
    InvalidInput,
    MBTIThematicInputPolicy,
    _json,
)
from qs_ai.application.interpretation.mbti_input import assemble_mbti, validate_mbti_projection
from qs_ai.domain.governance.scenes import MBTI_CONTRACT, MBTI_THEMATIC_CONTRACT


def assemble_mbti_themes(
    snapshot: Any, policy: MBTIThematicInputPolicy, locale: str, focus: tuple[str, ...]
) -> AssembledInput:
    if policy.scene_contract_version != MBTI_THEMATIC_CONTRACT:
        raise InvalidInput("Unsupported MBTI thematic contract")
    # Reuse exact report validation/projection, without rescaling or enriching facts.
    baseline = assemble_mbti(
        snapshot, replace(policy, scene_contract_version=MBTI_CONTRACT), locale, focus
    )
    document = json.loads(baseline.canonical_json)
    material = policy.reference_material
    if (material.model_code, material.model_version) != (policy.model_code, policy.model_version):
        raise InvalidInput("MBTI references differ from report model")
    selected = material.select(document["facts"]["model_result"]["type_code"])
    document.update(
        schema_version="ai-explanation-input/v3",
        scene_contract_version=MBTI_THEMATIC_CONTRACT,
        reference_material=selected.projection(),
    )
    canonical = _json(document)
    return AssembledInput(
        canonical,
        "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
        _json({name: document[name] for name in ("context", "facts", "reference_material")}),
    )


def validate_mbti_themes_projection(
    payload: dict[str, Any], policy: MBTIThematicInputPolicy
) -> None:
    """Check original frozen references, not current assets or model-written metadata."""
    try:
        if set(payload) != {"context", "facts", "reference_material"}:
            raise ValueError
        validate_mbti_projection({name: payload[name] for name in ("context", "facts")})
        expected = policy.reference_material.select(payload["facts"]["model_result"]["type_code"])
        if payload["reference_material"] != expected.projection():
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise InvalidInput("Frozen MBTI reference projection mismatch") from None
