"""Characterize original semantic terminal progress before readonly planning extraction."""

import json
from dataclasses import asdict, dataclass, replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql import evaluation_semantic as original
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from tests.test_evaluation_identity import identity
from tests.test_generation_completion import completion as generation
from tests.test_semantic_completion import completion as semantic


@dataclass(frozen=True)
class Decisions:
    decisions: tuple = ()


@pytest.mark.parametrize("candidate_claim", [False, True])
@pytest.mark.parametrize(
    "outcome",
    [
        "unknown",
        "retry_allowed",
        "retry_refused",
        "budget_exhausted",
        "success_continue",
        "success_review",
        "cancelled_success",
    ],
)
async def test_original_semantic_budget_candidate_owner_cancel_and_review_progress(
    monkeypatch, candidate_claim, outcome
):
    run_id, generated, first = uuid4(), generation(), semantic()
    success, cancel = (
        outcome.startswith("success_") or outcome == "cancelled_success",
        outcome == "cancelled_success",
    )
    version = 6 if cancel else 5
    failure = (
        None
        if success
        else ClassifiedFailure(
            "semantic_evaluation",
            "result_unknown" if outcome == "unknown" else "semantic_execution",
            "provider_result_unknown" if outcome == "unknown" else "semantic_invalid",
            outcome != "unknown",
            outcome == "unknown",
            "manual_acknowledgement" if outcome == "unknown" else "retry_semantic",
            "Original semantic terminal evidence",
            (first.execution_id,),
        )
    )
    completed = replace(
        first,
        invocation_id="semantic:invocation",
        receipt=replace(first.receipt, invocation_id="semantic:invocation"),
        status="succeeded" if success else "result_unknown" if outcome == "unknown" else "failed",
        failure=failure,
        candidate_output_fingerprint=generated.normalized_fingerprint,
        execution_ordinal=2 if outcome == "budget_exhausted" else 1,
    )
    cp = ExecutionCheckpoint(
        completed.execution_id,
        "semantic",
        generated.case_id,
        generated.slot_ordinal,
        completed.candidate_id,
        completed.execution_ordinal,
        "worker:first",
        completed.invocation_id,
        "dispatching",
        completed.started_at,
        completed.started_at + timedelta(seconds=30),
        completed.started_at,
    )
    progress = dict(
        status="collecting",
        transitions=[],
        unresolved_result_unknown_count=2,
        preflight=dict(status="passed", case_id=""),
    )
    if cancel:
        progress["cancel_requested"] = dict(source_version=5, version=6)
    run = dict(
        progress_json=progress,
        definition_json=json.dumps(
            dict(
                release=asdict(identity()),
                execution_policy_json="first-policy",
                slots=[dict(case_id=generated.case_id, ordinal=1)],
            )
        ),
    )
    candidate = dict(
        id=completed.candidate_id,
        generation_execution_id=generated.execution_id,
        normalized_output_fingerprint=generated.normalized_fingerprint,
        review_ready=False,
        assertions=[],
    )
    policy = SimpleNamespace(
        definition_json="first-policy",
        semantic_per_candidate=2,
        allows_automatic_semantic_recovery=Mock(return_value=outcome == "retry_allowed"),
    )
    monkeypatch.setattr(original, "frozen_policies", lambda _: (policy, None))
    monkeypatch.setattr(original, "decode_completion", lambda _: generated)
    monkeypatch.setattr(
        original,
        "resolve_semantic_route",
        AsyncMock(
            return_value=SimpleNamespace(
                definition_json=json.dumps(dict(provider="deepseek", model="model"))
            )
        ),
    )
    monkeypatch.setattr(original, "assertion_inventory", Mock(return_value=()))
    monkeypatch.setattr(original, "semantic_obligations", Mock(return_value=()))
    monkeypatch.setattr(original, "stored_run_suite", AsyncMock(return_value=object()))
    monkeypatch.setattr(original, "semantic_contract", AsyncMock(return_value=object()))
    monkeypatch.setattr(original, "parse_semantic_output", AsyncMock(return_value=Decisions()))
    monkeypatch.setattr(original, "project_slots", Mock(return_value=()))
    next_action = Mock(
        return_value=SimpleNamespace(
            kind="await_review" if outcome == "success_review" else "continue"
        )
    )
    monkeypatch.setattr(original, "next_action", next_action)
    expected = CheckpointState(run_id, version + 1, None)
    save, accepted = AsyncMock(), AsyncMock(return_value=expected)
    monkeypatch.setattr(original, "save_checkpoint", save)
    monkeypatch.setattr(original, "complete_claim", accepted)
    claim = SimpleNamespace(checkpoint=cp) if candidate_claim else None
    monkeypatch.setattr(original, "completion_owner", AsyncMock(return_value=(run, version, claim)))

    def one(value):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = value
        return result

    def many(value):
        result = Mock()
        result.mappings.return_value.all.return_value = value
        return result

    ledger = Mock()
    ledger.scalar_one_or_none.return_value = encode(cp)
    prior = [
        dict(execution_ordinal=i, result_json=None) for i in range(1, completed.execution_ordinal)
    ]
    responses = (
        []
        if candidate_claim
        else [one(dict(version=version, checkpoint_json=encode(cp))), one(run)]
    ) + [one(dict(candidate_json=candidate)), ledger, many(prior), Mock()]
    if success:
        responses.append(Mock())
    if not candidate_claim:
        if success and not cancel:
            responses.extend([many([]), many([]), many([])])
        responses.append(Mock())
    db = SimpleNamespace(execute=AsyncMock(side_effect=responses), commit=AsyncMock())
    result = await original.complete_semantic(
        db, run_id, 5, 1, " worker:first ", completed, Mock(), claim=claim
    )
    assert result == expected and result.checkpoint is None
    if candidate_claim:
        actual = accepted.await_args.args[4]
        save.assert_not_called()
    else:
        actual = db.execute.await_args.args[0].compile().params["progress_json"]
        save.assert_awaited_once_with(db, expected, version)
        accepted.assert_not_called()
    cause = {
        "unknown": "result_unknown_requires_review",
        "retry_refused": "semantic_recovery_not_allowed",
        "budget_exhausted": "semantic_budget_exhausted",
    }.get(outcome, "")
    status = (
        "blocked"
        if cause and not candidate_claim
        else "awaiting_review"
        if outcome == "success_review" and not candidate_claim
        else "collecting"
    )
    assert actual["status"] == status
    assert actual["unresolved_result_unknown_count"] == (3 if outcome == "unknown" else 2)
    if actual["transitions"]:
        assert actual["transitions"] == [
            dict(
                **{"from": "collecting", "to": status},
                actor="worker:first",
                cause_code=cause or "candidate_evidence_complete",
                at=completed.finished_at.isoformat(),
                evidence_refs=[completed.execution_id],
            )
        ]
    assert len(actual["transitions"]) == (status != "collecting")
    assert progress["status"] == "collecting" and progress["transitions"] == []
    assert progress["unresolved_result_unknown_count"] == 2
    assert bool(next_action.call_count) == (success and not cancel and not candidate_claim)
    assert bool(policy.allows_automatic_semantic_recovery.call_count) == (
        outcome in ("retry_allowed", "retry_refused")
    )
    db.commit.assert_not_called()
