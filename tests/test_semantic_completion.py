from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import ProviderReceipt
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion


def completion():
    at = datetime(2026, 9, 12, tzinfo=UTC)
    return SemanticCompletion(
        "semantic:1",
        "candidate:1",
        "sha256:" + "a" * 64,
        1,
        "invocation:1",
        "succeeded",
        at,
        at + timedelta(seconds=1),
        1,
        ProviderReceipt("invocation:1", "request:1", "deepseek", "model", 1, 2, 100),
        b'{ "judge": true }',
        b'{ "judge": true }',
    )


@pytest.mark.parametrize(
    "change",
    [
        {"receipt": None},
        {"raw_output": b""},
        {"normalized_output": b""},
        {"normalized_output": b"NaN"},
        {"normalized_output": b"\xff"},
        {"provider_call_count": False},
        {"execution_ordinal": 3},
        {"candidate_output_fingerprint": ""},
        {"status": "dispatching"},
        {"raw_output": b"x" * (256 * 1024 + 1)},
    ],
)
def test_incomplete_semantic_evidence_rejected(change):
    with pytest.raises(ValueError):
        replace(completion(), **change)


def test_checkpoint_matches_exact_candidate_and_generation_fingerprint():
    value = completion()
    cp = ExecutionCheckpoint(
        value.execution_id,
        "semantic",
        "case:1",
        1,
        value.candidate_id,
        1,
        "worker:1",
        value.invocation_id,
        "dispatching",
        value.started_at,
        value.started_at + timedelta(seconds=30),
        value.started_at,
    )
    assert value.matches_checkpoint(cp, "worker:1", value.candidate_output_fingerprint)
    for change in (
        {"candidate_id": "candidate:2"},
        {"execution_id": "semantic:2"},
        {"invocation_id": "invocation:2"},
        {"execution_ordinal": 2},
    ):
        assert not value.matches_checkpoint(
            replace(cp, **change), "worker:1", value.candidate_output_fingerprint
        )
    assert not value.matches_checkpoint(cp, "worker:2", value.candidate_output_fingerprint)
    assert not value.matches_checkpoint(cp, "worker:1", "sha256:" + "b" * 64)
    # An elapsed lease alone does not invalidate an otherwise-current terminal write.
    assert replace(
        value, finished_at=cp.lease_expires_at + timedelta(seconds=1)
    ).matches_checkpoint(cp, "worker:1", value.candidate_output_fingerprint)


def test_failed_judge_preserves_invalid_bytes_but_quality_is_not_execution_failure():
    failure = ClassifiedFailure(
        "semantic_evaluation",
        "semantic_execution",
        "semantic_output_schema_invalid",
        True,
        False,
        "retry_semantic",
        "Invalid judge response",
        ("semantic:1",),
    )
    failed = replace(completion(), status="failed", normalized_output=b"not-json", failure=failure)
    assert failed.normalized_output == b"not-json"
    quality = ClassifiedFailure(
        "semantic_evaluation",
        "quality_failure",
        "quality_failed",
        False,
        False,
        "retain_candidate",
        "Quality failed",
        ("semantic:1",),
    )
    with pytest.raises(ValueError):
        replace(failed, failure=quality)
    unknown = ClassifiedFailure(
        "semantic_evaluation",
        "result_unknown",
        "response_unknown",
        False,
        True,
        "manual_acknowledgement",
        "Response unknown",
        ("semantic:1",),
    )
    with pytest.raises(ValueError):
        replace(failed, failure=unknown)
    assert replace(failed, status="result_unknown", failure=unknown).status == "result_unknown"
