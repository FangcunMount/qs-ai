"""Expired candidate observation cannot synthesize provider responses or live ownership."""

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
    CandidateRecoverySource,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.workflow_transport import candidate_expiry_state as module
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from tests.test_evaluation_serial_response_receipts import original as original


def fixture(original, monkeypatch, *, mode="generation", other=False):
    p = original
    cp = replace(
        p.cp,
        kind="semantic" if mode == "semantic" else "generation",
        candidate_id="candidate:first" if mode == "semantic" else "",
    )
    if mode == "prepared":
        cp = replace(cp, phase="prepared", dispatch_started_at=None)
    claim = SlotClaim(p.ref.run_id, p.ref.version, cp)
    run = dict(
        run_id=str(claim.run_id),
        organization_id=1,
        execution_mode="candidate_v2",
        progress_json=dict(status="collecting", transitions=[]),
        definition_json='{"release_fingerprint":"sha256:' + "a" * 64 + '"}',
        frozen_execution_policy_fingerprint="original policy hash",
        frozen_execution_policy_json={"policy": "first"},
    )
    observed = CandidateRecoverySource(7, run, claim, None)
    reader = AsyncMock(return_value=observed)
    monkeypatch.setattr(module, "read_candidate_recovery_source", reader)
    active = (claim,)
    if other:
        active += (
            replace(
                claim,
                version=6,
                checkpoint=replace(
                    cp,
                    execution_id="execution:other",
                    invocation_id="invocation:other",
                    case_id="case:other",
                    slot_ordinal=2,
                ),
            ),
        )
    active_reader = AsyncMock(return_value=active)
    monkeypatch.setattr(module, "active_claims", active_reader)
    target = {**run["progress_json"], "status": "collecting" if other else "blocked"}
    terminal = AsyncMock(return_value=target)
    monkeypatch.setattr(module, "project_candidate_terminal", terminal)
    generated = Mock()
    generated.mappings.return_value.one.return_value = {"original": "accepted candidate"}
    db = SimpleNamespace(execute=AsyncMock(return_value=generated), commit=AsyncMock())
    monkeypatch.setattr(
        module,
        "decode_completion",
        lambda _: SimpleNamespace(normalized_fingerprint="sha256:" + "f" * 64),
    )
    request = module.CandidateExpiryRequest(1, claim)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
@pytest.mark.parametrize("other", [False, True])
@pytest.mark.parametrize("lock", [False, True])
async def test_expiry_uses_original_interrupted_outcome_and_all_current_claims_without_writes(
    original, monkeypatch, mode, other, lock
):
    p = fixture(original, monkeypatch, mode=mode, other=other)
    before = copy.deepcopy(p.run)
    projected = await module.project_candidate_expiry(
        p.db, p.request, p.cp.lease_expires_at, lock=lock
    )
    assert projected.version == 7 and projected.claim == p.claim and p.run == before
    assert len(projected.digest) == 32
    p.reader.assert_awaited_once_with(p.db, 1, p.claim, p.cp.lease_expires_at, lock=lock)
    if mode == "prepared":
        assert projected.completion is None and projected.progress == before["progress_json"]
        p.terminal.assert_not_called()
    else:
        assert projected.completion.status == "result_unknown"
        assert projected.completion.receipt is None and not projected.completion.raw_output
        assert projected.completion.failure.result_unknown
        p.terminal.assert_awaited_once_with(
            p.db,
            p.claim.run_id,
            1,
            p.run,
            p.claim,
            p.active,
            projected.completion,
            lock=lock,
        )
    p.db.commit.assert_not_called()
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)


@pytest.mark.parametrize("evidence", ["completed", "response"])
async def test_first_response_or_already_completed_claim_cannot_be_overwritten(
    original, monkeypatch, evidence
):
    p = fixture(original, monkeypatch)
    p.reader.return_value = (
        None if evidence == "completed" else replace(p.observed, evidence=original.result)
    )
    with pytest.raises(CheckpointConflict):
        await module.project_candidate_expiry(p.db, p.request, p.cp.lease_expires_at, lock=False)
    p.terminal.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("mode", ["prepared", "generation", "semantic"])
@pytest.mark.parametrize("changed", [False, True])
async def test_original_expiry_write_keeps_exact_claim_guard_and_caller_root(
    original, monkeypatch, mode, changed
):
    p = fixture(original, monkeypatch, mode=mode)
    source = await module.project_candidate_expiry(
        p.db, p.request, p.cp.lease_expires_at, lock=False
    )
    facts = EvaluationStateFacts(
        p.request.intent,
        p.cp.lease_expires_at,
        0,
        0,
        p.request.encode(source.digest),
        source_version=7,
    )
    projection = replace(source, digest=bytes(32)) if changed else source
    monkeypatch.setattr(module, "project_candidate_expiry", AsyncMock(return_value=projection))
    release, save, generation, semantic = [AsyncMock() for _ in range(4)]
    for name, value in [
        ("release_claim", release),
        ("save_checkpoint", save),
        ("complete_generation", generation),
        ("complete_semantic", semantic),
    ]:
        monkeypatch.setattr(module, name, value)
    if changed:
        with pytest.raises(MessageConflict):
            await module.apply_candidate_expiry(p.db, facts)
        assert all(not value.called for value in (release, save, generation, semantic))
    else:
        await module.apply_candidate_expiry(p.db, facts)
        if mode == "prepared":
            release.assert_awaited_once_with(p.db, p.claim)
            assert save.await_args.args[1].version == 8
            generation.assert_not_called()
            semantic.assert_not_called()
        else:
            target = generation if mode == "generation" else semantic
            assert target.await_args.kwargs["claim"] == p.claim
            assert target.await_args.args[1:4] == (p.claim.run_id, 7, 1)
            release.assert_not_called()
    p.db.commit.assert_not_called()


async def test_renewal_does_not_create_new_request_but_frozen_expiry_stays_exact(
    original, monkeypatch
):
    p = fixture(original, monkeypatch)
    from datetime import timedelta

    renewed = replace(
        p.claim,
        checkpoint=replace(p.cp, lease_expires_at=p.cp.lease_expires_at + timedelta(seconds=30)),
    )
    request = module.CandidateExpiryRequest(1, renewed)
    assert request.intent == p.request.intent
    assert request.encode(b"x" * 32) != p.request.encode(b"x" * 32)
    facts = EvaluationStateFacts(
        p.request.intent, p.cp.lease_expires_at, 0, 0, p.request.encode(b"x" * 32), source_version=7
    )
    assert module.CandidateExpiryRequest.from_facts(facts)[0].claim == p.claim
    with pytest.raises(ValueError):
        replace(p.request, claim=replace(p.claim, version=True))
