from dataclasses import replace
from pathlib import Path

import pytest

from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from qs_ai.infrastructure.qs_server.evaluation_suite import V6, load_suite


def test_original_suite_freezes_ordered_slots_and_separate_preflight():
    suite = load_suite(V6)
    policy = load_execution_policy()
    assert len(suite.generation_case_ids) == policy.generation_cases == 7
    assert suite.repetitions == policy.candidates_per_case == 5
    assert suite.preflight_case_id == "PROMPT-EVAL-008"
    assert suite.slots() == tuple(
        (f"PROMPT-EVAL-{case:03}", ordinal) for case in range(1, 8) for ordinal in range(1, 6)
    )
    assert len(set(suite.slots())) == 35
    assert (
        suite.definition_json.encode()
        == Path(
            "integrations/qs_server/evaluation/ai-explanation-prompt-evaluation-cases-v6.json"
        ).read_bytes()
    )


@pytest.mark.parametrize(
    "change", [{"id": "other"}, {"version": "v7"}, {"fingerprint": "sha256:" + "0" * 64}]
)
def test_suite_requires_exact_frozen_identity(change):
    with pytest.raises(ValueError, match="identity"):
        load_suite(replace(V6, **change))


def test_changed_suite_bytes_rejected_before_planning(tmp_path):
    source = Path(
        "integrations/qs_server/evaluation/ai-explanation-prompt-evaluation-cases-v6.json"
    )
    (tmp_path / source.name).write_bytes(source.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="fingerprint"):
        load_suite(V6, directory=tmp_path)
