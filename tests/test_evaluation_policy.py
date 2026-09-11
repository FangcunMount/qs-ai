import hashlib
import json
import shutil

import pytest

from qs_ai.infrastructure.qs_server.evaluation_policies import (
    evaluation_directory,
    load_execution_policy,
)


def test_original_v2_policy_preserves_sample_targets_and_unknown_boundaries():
    policy = load_execution_policy()
    assert policy.generation_cases * policy.candidates_per_case == 35
    assert policy.preflight_cases == 1
    assert policy.generation_per_slot == policy.semantic_per_candidate == 2
    assert policy.generation_per_run == policy.semantic_per_run == 70
    assert policy.unknown_requires_acknowledgement
    assert not policy.quality_replacement_allowed
    assert not policy.semantic_failure_regenerates_candidate
    for stage, code in policy.automatic_selectors:
        assert policy.selects_automatic_retry(stage, code, retryable=True)
        assert not policy.selects_automatic_retry(stage, code, retryable=False)
    for stage, code in policy.manual_selectors:
        assert not policy.selects_automatic_retry(stage, code, retryable=True)
    assert not policy.selects_automatic_retry(
        "generation_execution", "semantic_provider_no_message", retryable=True
    )


@pytest.mark.parametrize("stage", ["generation_execution", "semantic_evaluation"])
def test_both_target_and_run_budget_must_have_capacity(stage):
    policy = load_execution_policy()
    assert policy.within_budget(stage, 0, 0)
    assert policy.within_budget(stage, 1, 69)
    assert not policy.within_budget(stage, 2, 69)
    assert not policy.within_budget(stage, 1, 70)
    for target, total in ((-1, 0), (2, 1), (True, 1)):
        with pytest.raises(ValueError):
            policy.within_budget(stage, target, total)


def test_unknown_stage_and_changed_policy_are_not_silently_accepted(tmp_path):
    with pytest.raises(ValueError):
        load_execution_policy().within_budget("other", 0, 0)
    shutil.copytree(evaluation_directory(), tmp_path, dirs_exist_ok=True)
    path = tmp_path / "policies.json"
    policies = json.loads(path.read_bytes())
    policies["execution_policy"]["definition_json"] += " "
    path.write_text(json.dumps(policies))
    with pytest.raises(ValueError, match="checksum"):
        load_execution_policy(directory=tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    manifest["files"]["policies.json"] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="fingerprint"):
        load_execution_policy(directory=tmp_path)
