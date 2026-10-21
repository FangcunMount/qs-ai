"""Characterize original terminal progress through its persistence entrypoint."""

import json
from dataclasses import asdict, replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.persistence.mysql import evaluation_completions as original
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from tests.test_evaluation_identity import identity
from tests.test_generation_completion import completion


@pytest.mark.parametrize("candidate_claim", [False, True])
@pytest.mark.parametrize(
    "outcome", ["success", "unknown", "retry_allowed", "retry_refused", "budget_exhausted"]
)
async def test_original_terminal_budget_unknown_counter_and_serial_only_blocking(
    monkeypatch,
    candidate_claim,
    outcome,
):
    run_id, value = uuid4(), completion()
    cause = {
        "success": "",
        "unknown": "result_unknown_requires_review",
        "retry_allowed": "",
        "retry_refused": "generation_recovery_not_allowed",
        "budget_exhausted": "generation_budget_exhausted",
    }[outcome]
    if outcome != "success":
        failure = ClassifiedFailure(
            "generation_execution",
            "result_unknown" if outcome == "unknown" else "provider_protocol",
            "provider_result_unknown" if outcome == "unknown" else "provider_unavailable",
            False,
            outcome == "unknown",
            "manual_acknowledgement" if outcome == "unknown" else "retry_generation",
            "Original terminal failure",
            (value.execution_id,),
        )
        value = replace(
            value,
            status="result_unknown" if outcome == "unknown" else "failed",
            failure=failure,
            execution_ordinal=2 if outcome == "budget_exhausted" else 1,
        )
    cp = ExecutionCheckpoint(
        value.execution_id,
        "generation",
        value.case_id,
        value.slot_ordinal,
        "",
        value.execution_ordinal,
        "worker:first",
        value.invocation_id,
        "dispatching",
        value.started_at,
        value.started_at + timedelta(seconds=30),
        value.started_at,
    )
    progress = {"status": "collecting", "transitions": [], "unresolved_result_unknown_count": 2}
    run = dict(
        progress_json=progress,
        definition_json=json.dumps(
            dict(
                release=asdict(identity()),
                execution_policy_json="first-frozen-policy",
                slots=[dict(case_id=value.case_id, ordinal=value.slot_ordinal)],
            )
        ),
    )
    policy = SimpleNamespace(
        definition_json="first-frozen-policy",
        generation_per_slot=2,
        allows_automatic_generation_recovery=Mock(return_value=outcome == "retry_allowed"),
    )
    monkeypatch.setattr(original, "frozen_policies", lambda _: (policy, None))
    monkeypatch.setattr(original, "validate_generation_completion", AsyncMock())
    save = AsyncMock()
    monkeypatch.setattr(original, "save_checkpoint", save)
    claim = SimpleNamespace(checkpoint=cp) if candidate_claim else None
    monkeypatch.setattr(original, "completion_owner", AsyncMock(return_value=(run, 5, claim)))
    accepted = AsyncMock(return_value=CheckpointState(run_id, 6, None))
    monkeypatch.setattr(original, "complete_claim", accepted)
    cp_row = Mock()
    cp_row.mappings.return_value.one_or_none.return_value = dict(
        version=5, checkpoint_json=encode(cp)
    )
    run_row = Mock()
    run_row.mappings.return_value.one_or_none.return_value = run
    ledger = Mock()
    ledger.scalar_one_or_none.return_value = encode(cp)
    prior = Mock()
    prior.mappings.return_value.all.return_value = [
        dict(candidate_id=None, execution_ordinal=i) for i in range(1, value.execution_ordinal)
    ]
    responses = (
        ([] if candidate_claim else [cp_row, run_row])
        + [ledger, prior, Mock()]
        + ([] if candidate_claim else [Mock()])
    )
    db = SimpleNamespace(execute=AsyncMock(side_effect=responses), commit=AsyncMock())
    state = await original.complete_generation(
        db,
        run_id,
        5,
        1,
        " worker:first ",
        value,
        Mock(),
        Mock(),
        candidate_id="candidate:first" if outcome == "success" else "",
        assertions=(
            AssertionReceipt(
                "schema", "default", 1, True, "deterministic", "passed", "Schema valid"
            ),
        )
        if outcome == "success"
        else (),
        claim=claim,
    )
    assert state.version == 6 and state.checkpoint is None
    if candidate_claim:
        actual = accepted.await_args.args[4]
        save.assert_not_called()
    else:
        actual = db.execute.await_args.args[0].compile().params["progress_json"]
        save.assert_awaited_once_with(db, state, 5)
        accepted.assert_not_called()
    assert actual["unresolved_result_unknown_count"] == (3 if outcome == "unknown" else 2)
    assert actual["status"] == ("blocked" if cause and not candidate_claim else "collecting")
    assert len(actual["transitions"]) == (1 if cause and not candidate_claim else 0)
    if actual["transitions"]:
        assert actual["transitions"][-1] == {
            "from": "collecting",
            "to": "blocked",
            "cause_code": cause,
            "actor": "worker:first",
            "at": value.finished_at.isoformat(),
            "evidence_refs": [value.execution_id],
        }
    assert progress == {
        "status": "collecting",
        "transitions": [],
        "unresolved_result_unknown_count": 2,
    }
    db.commit.assert_not_called()  # Original host still owns the root commit.
