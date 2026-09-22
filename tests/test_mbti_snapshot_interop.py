"""Consume bytes emitted by QS's actual reportSnapshot in the pinned Go checkout."""

import hashlib
import json
import os
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.input import InvalidInput
from qs_ai.application.interpretation.selection import snapshot_selector
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from tests.test_mbti_contract import assemble


@pytest.mark.interop
def test_go_projection_is_accepted_without_reinterpreting_facts():
    path = os.environ.get("QS_AI_SNAPSHOT_VECTOR_OUT")
    if not path:
        pytest.skip("QS-produced snapshot vector is required for interop")
    vectors = json.loads(Path(path).read_text())
    for key in ("mbti", "mbti_zero"):
        snapshot = vectors[key]
        result = assemble(snapshot)
        value = json.loads(result.canonical_json)
        Draft202012Validator(load_input_schema(version="ai-explanation-input/v2")).validate(value)
        assert snapshot_selector(snapshot).model_code == "MBTI_OEJTS"
        assert value["source"] == snapshot["source"]
        assert value["facts"]["model_result"] == snapshot["model_extra"]
        for source, actual in zip(
            snapshot["dimensions"], value["facts"]["dimensions"], strict=True
        ):
            assert actual["pole_facts"] == source["pole_facts"]
            assert actual["raw_score"]["value"] == source["raw_score"]
        assert value["facts"]["model_result"]["type_code"] == "ISFJ"
        if key == "mbti_zero":
            assert value["facts"]["model_result"]["match_percent"] == 0
            assert all(d["pole_facts"]["strength"] == 0 for d in value["facts"]["dimensions"])
        assert (
            result.fingerprint
            == "sha256:" + hashlib.sha256(result.canonical_json.encode()).hexdigest()
        )
        provider = json.loads(result.provider_payload)
        assert set(provider) == {"context", "facts"}
        assert "image_url" not in provider["facts"]["model_result"]
        with pytest.raises(InvalidInput):
            assemble(vectors["scale"])
