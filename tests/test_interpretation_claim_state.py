"""One frozen original claim, exact source and no phantom running event on deferral."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.persistence.mysql.execution import ClaimStage
from qs_ai.infrastructure.workflow_transport import interpretation_claim_state as module
from tests.test_evaluation_state_plan import fixture as state_fixture


@pytest.fixture(params=[False, True], ids=["running", "exhausted"])
async def frozen(request, monkeypatch):
    keys = state_fixture().keys
    session = Session(str(uuid4()), Actor("7", "original:actor"), "8", ("9",), "original goal")
    session.queue(str(uuid4()))
    source = module.InterpretationClaimSource(
        session,
        str(uuid4()),
        str(uuid4()),
        3 if request.param else 0,
        7,
        hashlib.sha256(b"precise original claim source").digest(),
    )
    opened = []
    db = SimpleNamespace(connection=AsyncMock())

    @asynccontextmanager
    async def open():
        opened.append(True)
        try:
            yield db
        finally:
            opened.pop()

    store = SimpleNamespace(transactions=SimpleNamespace(open=open), claim_in=AsyncMock())
    read = AsyncMock(return_value=source)
    monkeypatch.setattr(module, "read_claim_source", read)
    seal = module.prepare

    def prepare(*args, **kwargs):
        assert not opened, "Claim source must close before first Seal"
        return seal(*args, **kwargs)

    monkeypatch.setattr(module, "prepare", prepare)
    planner = module.InterpretationClaimPlanner(store, **keys)
    plan = await planner.candidate(source, 30)
    return SimpleNamespace(**locals())


async def test_claim_keeps_first_wire_clock_identity_and_original_version_transitions(frozen):
    p = frozen
    restored = decode_plan(encode_plan(p.plan))
    facts, archive = module.read_claim_plan(p.source, 30, restored)
    assert facts.version_delta == (2 if p.source.exhausted else 1)
    assert archive.sequence == p.source.session.version + facts.version_delta
    state = pb.MessagingBody.FromString(archive.body).interpretation_state
    assert state.status == ("blocked" if p.source.exhausted else "running")
    assert state.failure_code == ("attempts_exhausted" if p.source.exhausted else "")
    candidate = await p.planner.candidate(p.source, 30)
    p.source.intent(30).request.match(candidate)
    assert candidate.digest != restored.digest
    assert candidate.branches[0].wire != restored.branches[0].wire
    assert module.read_claim_plan(p.source, 30, restored) == (facts, archive)
    assert p.session.status == Status.QUEUED
    assert "original goal" not in repr(p.source)


@pytest.mark.parametrize("damage", ["ttl", "fence", "attempt", "source", "scope"])
async def test_changed_original_claim_input_cannot_adopt_first_execution(frozen, damage):
    p = frozen
    source, ttl = p.source, 30
    if damage == "ttl":
        ttl = 31
    elif damage == "scope":
        source = replace(source, session=replace(source.session, actor=Actor("8", "other")))
    else:
        field = {"source": "source_hash"}.get(damage, damage)
        value = {"fence": 8, "attempt": p.source.attempt + 1, "source": b"x" * 32}[damage]
        source = replace(source, **{field: value})
    with pytest.raises(ValueError):
        module.read_claim_plan(source, ttl, p.plan)


async def test_source_change_before_freeze_never_seals_or_writes(frozen, monkeypatch):
    p = frozen
    p.read.return_value = replace(p.source, fence=8)
    seal = AsyncMock(side_effect=AssertionError("Must not Seal changed source"))
    monkeypatch.setattr(module, "prepare", seal)
    with pytest.raises(LeaseLost):
        await p.planner.candidate(p.source, 30)
    seal.assert_not_called()
    p.store.claim_in.assert_not_called()


async def test_write_rechecks_source_then_borrows_original_root_with_first_id_and_clock(frozen):
    p = frozen
    facts, archive = module.read_claim_plan(p.source, 30, p.plan)
    projected = replace(p.source.session)
    projected.running()
    if p.source.exhausted:
        projected.block("attempts_exhausted")
    claim = (
        None
        if p.source.exhausted
        else Claim(p.source.job_id, projected.active_run_id, projected, 8, None, False, None)
    )
    stage = ClaimStage("exhausted" if p.source.exhausted else "claimed", claim)
    p.store.claim_in.return_value = stage
    write = module.InterpretationClaimWrite(p.store, p.source, 30, p.plan)
    assert write.staged is None
    await write(p.db, facts)
    assert write.staged == stage
    p.store.claim_in.assert_awaited_once_with(
        p.db,
        p.source.job_id,
        p.source.session.id,
        30,
        state_event_id=archive.envelope().message_id,
        at=facts.at,
    )
    assert p.read.await_args.kwargs == {"lock": True}


@pytest.mark.parametrize("status", ["miss", "deferred"])
async def test_no_event_admission_cannot_commit_a_frozen_running_or_exhausted_event(frozen, status):
    p = frozen
    facts, _ = module.read_claim_plan(p.source, 30, p.plan)
    p.store.claim_in.return_value = ClaimStage(status)
    write = module.InterpretationClaimWrite(p.store, p.source, 30, p.plan)
    with pytest.raises(LeaseLost):
        await write(p.db, facts)
    assert write.staged is None


async def test_locked_source_conflict_refuses_original_business_write(frozen):
    p = frozen
    facts, _ = module.read_claim_plan(p.source, 30, p.plan)
    p.read.return_value = replace(p.source, source_hash=b"x" * 32)
    with pytest.raises(LeaseLost):
        await module.InterpretationClaimWrite(p.store, p.source, 30, p.plan)(p.db, facts)
    p.store.claim_in.assert_not_called()


async def test_wrong_staged_business_outcome_never_becomes_frozen_commit_proof(frozen):
    p = frozen
    facts, _ = module.read_claim_plan(p.source, 30, p.plan)
    p.store.claim_in.return_value = ClaimStage("claimed" if p.source.exhausted else "exhausted")
    with pytest.raises(LeaseLost):
        await module.InterpretationClaimWrite(p.store, p.source, 30, p.plan)(p.db, facts)


@pytest.mark.parametrize("damage", ["event_id", "ttl_seconds", "source_hash", "kind", "extra"])
async def test_keyless_plan_reader_rejects_changed_claim_context(frozen, damage):
    p = frozen
    facts, archive = module.read_claim_plan(p.source, 30, p.plan)
    context = json.loads(facts.business_context)
    context[damage] = dict(
        event_id=str(uuid4()),
        ttl_seconds=31,
        source_hash="b" * 64,
        kind="another/v1",
        extra="unexpected",
    )[damage]
    changed = replace(facts, business_context=module._json(context))
    # Keep original body and wire while replacing the explicit frozen context.
    branch = p.plan.branches[0]
    changed_archive = replace(archive, context=changed.encode())
    with pytest.raises(ValueError):
        changed_plan = replace(
            p.plan, branches=(replace(branch, message=changed_archive.message()),)
        )
        module.read_claim_plan(p.source, 30, changed_plan)


@pytest.mark.parametrize("ttl", [True, 2, 86401])
def test_claim_plan_requires_bounded_explicit_ttl(ttl):
    session = Session(str(uuid4()), Actor("7", "actor"), "8", ("9",), "goal")
    session.queue(str(uuid4()))
    source = module.InterpretationClaimSource(session, str(uuid4()), str(uuid4()), 0, 0, b"x" * 32)
    with pytest.raises(ValueError):
        source.intent(ttl)


@pytest.mark.parametrize("state", ["prepared", "aborted"])
async def test_unresolved_existing_claim_does_not_plan_execute_or_handoff(frozen, state):
    from reliable_messaging.transaction import State

    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimPending,
        InterpretationClaimSender,
    )

    p = frozen
    runtime = SimpleNamespace(
        store=SimpleNamespace(
            lookup=AsyncMock(
                return_value=SimpleNamespace(
                    acquired=False,
                    state=State(state),
                )
            )
        ),
        reserve=AsyncMock(),
        execute=AsyncMock(),
    )
    sender = InterpretationClaimSender(p.store, SimpleNamespace(), runtime, **p.keys)
    sender._planner.candidate = AsyncMock()
    sender._handoff = AsyncMock()
    with pytest.raises(InterpretationClaimPending):
        await sender(p.source, 30)
    sender._planner.candidate.assert_not_called()
    sender._handoff.assert_not_called()
    runtime.reserve.assert_not_called()
    runtime.execute.assert_not_called()


@pytest.mark.parametrize("proof", ["committed", "missing", "archive_missing", "wrong_reference"])
async def test_duplicate_claim_requires_exact_proof_without_second_model_handoff(frozen, proof):
    from datetime import UTC, datetime, timedelta

    from reliable_messaging.transaction import Evidence, Execution, Reservation, State

    from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
        InterpretationClaimPending,
        InterpretationClaimSender,
    )

    p = frozen
    execution = Execution(p.plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    facts, first = module.read_claim_plan(p.source, 30, p.plan)
    reference = execution.references[0]
    if proof == "wrong_reference":
        reference = replace(reference, fence=2)
    evidence = Evidence(reference, State.COMMITTED, State.COMMITTED) if proof != "missing" else None
    runtime = SimpleNamespace(
        store=SimpleNamespace(
            lookup=AsyncMock(return_value=Reservation(execution, False, State.COMMITTED)),
            evidence=AsyncMock(return_value=evidence),
        ),
        reserve=AsyncMock(),
        execute=AsyncMock(),
    )
    archives = SimpleNamespace(
        read_committed=AsyncMock(
            return_value=None if proof == "archive_missing" else first,
        )
    )
    sender = InterpretationClaimSender(p.store, archives, runtime, **p.keys)
    sender._planner.candidate, sender._handoff = AsyncMock(), AsyncMock()
    if proof == "committed":
        assert await sender(p.source, 30) is None
    else:
        with pytest.raises(InterpretationClaimPending):
            await sender(p.source, 30)
    sender._planner.candidate.assert_not_called()
    sender._handoff.assert_not_called()
    runtime.execute.assert_not_called()
    runtime.reserve.assert_not_called()
