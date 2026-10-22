"""Original expiry rules and readonly projection, not real database/transport acceptance."""

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.workflow_transport import expired_recovery_state as state
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from tests.test_evaluation_expired_recovery_source import source as source


def setup(source, monkeypatch, *, kind="generation", phase="prepared"):
    p = source
    cp = replace(p.cp, kind=kind, candidate_id="candidate:first" if kind == "semantic" else "")
    if phase == "dispatching":
        cp = cp.mark_dispatching(cp.owner, p.at)
    p.checkpoint["checkpoint_json"] = encode(cp)
    p.run.update(
        run_id=str(p.run_id),
        organization_id=1,
        definition_json='{"release_fingerprint":"sha256:' + "b" * 64 + '"}',
    )
    request = state.ExpiredRecoveryRequest(p.run_id, 1, 7, cp.invocation_id, cp.lease_expires_at)
    receipt = Mock()
    receipt.first.return_value = None
    generated = Mock()
    generated.mappings.return_value.one.return_value = {"original": "generation"}
    p.db.execute.side_effect = [p.selected(p.checkpoint), p.selected(p.run), receipt, generated]
    accepted = SimpleNamespace(policy=object())
    generation = AsyncMock(return_value=accepted)
    semantic = AsyncMock(return_value=accepted)
    target = {**p.progress, "status": "blocked", "unresolved_result_unknown_count": 1}
    generation_progress, semantic_progress = (
        Mock(return_value=target),
        AsyncMock(return_value=target),
    )
    monkeypatch.setattr(state, "read_generation_acceptance", generation)
    monkeypatch.setattr(state, "read_semantic_acceptance", semantic)
    monkeypatch.setattr(state, "generation_completion_progress", generation_progress)
    monkeypatch.setattr(state, "project_semantic_progress", semantic_progress)
    monkeypatch.setattr(
        state,
        "decode_completion",
        lambda _: SimpleNamespace(normalized_fingerprint="sha256:" + "f" * 64),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["generation", "semantic"])
@pytest.mark.parametrize("phase", ["prepared", "dispatching"])
@pytest.mark.parametrize("lock", [False, True])
async def test_expiry_projection_reuses_original_unknown_or_prepared_release_without_writes(
    source, monkeypatch, kind, phase, lock
):
    f = setup(source, monkeypatch, kind=kind, phase=phase)
    p = source
    before = copy.deepcopy((p.run, p.checkpoint))
    result = await state.project_expired_recovery(
        p.db, f.request, "system:recovery", f.cp.lease_expires_at, lock=lock
    )
    assert len(result.digest) == 32 and (p.run, p.checkpoint) == before
    audit = result.progress["recoveries"][0]
    assert audit["cause_code"] == (
        "expired_preparation_released" if phase == "prepared" else "expired_dispatch_result_unknown"
    )
    assert audit["invocation_id"] == p.cp.invocation_id and audit["from_version"] == 7
    if phase == "dispatching":
        assert result.progress["status"] == "blocked"
        assert result.progress["unresolved_result_unknown_count"] == 1
        acceptance = f.generation if kind == "generation" else f.semantic
        completion = acceptance.await_args.args[5 if kind == "generation" else 4]
        assert completion.status == "result_unknown" and completion.receipt is None
        assert not completion.raw_output and completion.failure.kind == "result_unknown"
    else:
        f.generation.assert_not_called()
        f.semantic.assert_not_called()
    assert all(call.args[0].is_select for call in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("phase", ["prepared", "dispatching"])
async def test_dispatch_or_first_durable_response_prevents_unknown_replacement(
    source, monkeypatch, phase
):
    f = setup(source, monkeypatch, phase=phase)
    f.receipt.first.return_value = object()
    error = ValueError if phase == "prepared" else state.DurableSerialResponseAvailable
    with pytest.raises(error):
        await state.project_expired_recovery(
            source.db, f.request, "system:recovery", f.cp.lease_expires_at, lock=False
        )
    f.generation.assert_not_called()
    f.semantic.assert_not_called()
    source.db.commit.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
async def test_original_expiry_writer_rechecks_exact_frozen_projection_without_commit(
    source, monkeypatch, changed
):
    f = setup(source, monkeypatch)
    planned = await state.project_expired_recovery(
        source.db, f.request, "system:recovery", f.cp.lease_expires_at, lock=False
    )
    facts = EvaluationStateFacts(
        f.request.intent,
        f.cp.lease_expires_at,
        0,
        0,
        f.request.encode("system:recovery", planned.digest),
    )
    current = replace(planned, digest=bytes(32)) if changed else planned
    project = AsyncMock(return_value=current)
    writer = AsyncMock()
    monkeypatch.setattr(state, "project_expired_recovery", project)
    monkeypatch.setattr(state, "recover_expired", writer)
    if changed:
        with pytest.raises(MessageConflict):
            await state.apply_expired_recovery(source.db, facts)
        writer.assert_not_called()
    else:
        await state.apply_expired_recovery(source.db, facts)
        writer.assert_awaited_once()
        assert writer.await_args.args[:8] == (
            source.db,
            source.run_id,
            7,
            1,
            source.cp.invocation_id,
            source.cp.lease_expires_at,
            facts.at,
            "system:recovery",
        )
    project.assert_awaited_once_with(source.db, f.request, "system:recovery", facts.at, lock=True)
    source.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["owner", "version", "expiry", "context", "candidate"])
async def test_expiry_codec_rejects_foreign_or_inconsistent_frozen_evidence(
    source, monkeypatch, damage
):
    f = setup(source, monkeypatch)
    at = source.cp.lease_expires_at
    facts = EvaluationStateFacts(
        f.request.intent, at, 0, 0, f.request.encode("system:recovery", b"x" * 32)
    )
    assert state.ExpiredRecoveryRequest.from_facts(facts)[0] == f.request
    with pytest.raises((ValueError, CheckpointConflict)):
        if damage == "owner":
            state.ExpiredRecoveryRequest.from_facts(
                replace(facts, intent=replace(facts.intent, organization_id="2"))
            )
        elif damage == "version":
            replace(f.request, version=True)
        elif damage == "expiry":
            replace(f.request, observed_expiry=at.replace(tzinfo=None))
        elif damage == "context":
            state.ExpiredRecoveryRequest.from_facts(
                replace(facts, business_context=facts.business_context + b" ")
            )
        else:
            source.run["execution_mode"] = "candidate_v2"
            await state.project_expired_recovery(
                source.db, f.request, "system:recovery", at, lock=False
            )
