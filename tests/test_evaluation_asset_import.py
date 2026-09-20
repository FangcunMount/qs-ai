"""Initialization keeps original bytes and fingerprints and grants no approval."""

from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_evaluation_assets import baseline_assets
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    FrozenPolicyDocument,
    execution_policy,
    load_execution_policy,
    load_gate_policy,
    quality_thresholds,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets, semantic_assets


def test_baseline_import_preserves_exact_source_and_bytes():
    source, policies, prompt, schema = baseline_assets()
    assert source == "qs-server:87f9dbea6db8c5a832d788bbb91ee8c257d47bd9"
    execution, gate = policies
    assert (
        execution_policy(FrozenPolicyDocument(execution.reference, execution.definition_json))
        == load_execution_policy()
    )
    assert quality_thresholds(
        FrozenPolicyDocument(gate.reference, gate.definition_json)
    ) == quality_thresholds(load_gate_policy())
    original = load_semantic_assets()
    assert prompt.markdown == original.prompt_markdown
    assert schema.definition_json == original.output_schema_json
    assert (
        semantic_assets(
            prompt.markdown, schema.definition_json, prompt.reference, original.output_schema
        )
        == original
    )


def test_unverified_bytes_are_rejected():
    _, policies, prompt, _ = baseline_assets()
    with pytest.raises(ValueError, match="content mismatch"):
        replace(policies[0], definition_json=policies[0].definition_json + "\n")
    with pytest.raises(ValueError, match="content mismatch"):
        replace(prompt, markdown=prompt.markdown + "\n")
