"""Original finish inputs, first identities and exact replay without model authority."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import State, decode_plan, encode_plan

from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.workflow_transport import interpretation_finish_state as module
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishPending,
    InterpretationFinishSender,
)
from tests.test_evaluation_state_plan import fixture as state_fixture


@pytest.fixture
async def frozen(monkeypatch):
    f = state_fixture()
    session = Session(str(uuid4()), Actor("7", "original:actor"), "8", ("9",), "original goal")
    session.queue(str(uuid4()))
    session.running()
    claim = Claim(str(uuid4()), session.active_run_id, session, 7, "original answer", False, None)
    incoming = module.InterpretationFinishInput(
        claim, WorkflowResult("checkpoint:first", question="original question", can_skip=False)
    )
    intent = incoming.intent(str(uuid4()))
    opened = []
    db = SimpleNamespace(connection=AsyncMock(), scalar=AsyncMock(return_value=intent.request_id))

    @asynccontextmanager
    async def open():
        opened.append(True)
        try:
            yield db
        finally:
            opened.pop()

    store = SimpleNamespace(transactions=SimpleNamespace(open=open))
    source = AsyncMock(return_value=(incoming.claim.session, "a" * 64, 3))
    monkeypatch.setattr(module, "_source", source)
    real_prepare = module.prepare

    def prepare(*args, **kwargs):
        assert not opened, "Source connection must close before signing/encryption"
        return real_prepare(*args, **kwargs)

    monkeypatch.setattr(module, "prepare", prepare)
    planner = module.InterpretationFinishPlanner(store, **f.keys)
    plan = await planner.candidate(incoming, intent)
    return SimpleNamespace(**locals())


async def test_finish_reuses_first_ids_wire_and_clock_with_stable_input_and_closed_source(frozen):
    p = frozen
    winner = decode_plan(encode_plan(p.plan))
    first, archive, question_id = module.read_finish_plan(p.incoming, p.intent, winner)
    other = await p.planner.candidate(p.incoming, p.intent)
    p.intent.request.match(other)
    assert winner.digest != other.digest
    assert winner.branches[0].wire != other.branches[0].wire
    assert module.read_finish_plan(p.incoming, p.intent, winner)[0] == first
    assert (
        question_id
        and question_id
        != json.loads(module.read_finish_plan(p.incoming, p.intent, other)[0].business_context)[
            "question_id"
        ]
    )
    assert json.loads(first.business_context)["question_sequence"] == 3
    assert archive.envelope().original_occurred_at == first.at.isoformat(timespec="microseconds")
    assert p.source.await_args.kwargs == {"lock": False}


async def test_input_owns_original_session_snapshot_and_does_not_log_business_body(frozen):
    p = frozen
    before = p.incoming.input_hash
    p.claim.session.goal = "later mutation outside the operation"
    assert p.incoming.input_hash == before
    assert "original question" not in repr(p.incoming)
    assert "original answer" not in repr(p.incoming)


@pytest.mark.parametrize("field", ["question", "checkpoint_ref", "can_skip", "failure_code"])
async def test_different_business_input_cannot_reuse_first_finish(frozen, field):
    p = frozen
    values = dict(
        question="other question",
        checkpoint_ref="other checkpoint",
        can_skip=True,
        failure_code="other failure",
    )
    result = replace(p.incoming.result, **{field: values[field]})
    if field == "failure_code":
        result = replace(result, question=None)
    other = module.InterpretationFinishInput(p.incoming.claim, result)
    assert other.intent(p.intent.request_id).request.input_hash != p.intent.request.input_hash
    with pytest.raises(ValueError):
        module.read_finish_plan(other, p.intent, p.plan)


@pytest.mark.parametrize(
    "damage", ["extra", "duplicate", "clock_free_id", "first_question", "hash"]
)
async def test_inexact_finish_context_is_refused_before_execution(frozen, damage):
    p = frozen
    facts, _, _ = module.read_finish_plan(p.incoming, p.intent, p.plan)
    context = json.loads(facts.business_context)
    if damage == "extra":
        context["extra"] = True
    elif damage == "clock_free_id":
        context["event_id"] = str(uuid4())
    elif damage == "first_question":
        context["question_id"] = str(uuid4())
    elif damage == "hash":
        context["source_hash"] = "x" * 64
    raw = module._json(context)
    if damage == "duplicate":
        raw = raw[:-1] + b',"kind":"interpretation-finish/v1"}'
    changed = replace(facts, business_context=raw)
    # Feed a structurally decoded plan into the business-specific decoder. The
    # generic codec and original witness have their own separate tests.
    monkeypatch = pytest.MonkeyPatch()
    with monkeypatch.context() as patch:
        _, archive, _ = module.read_finish_plan(p.incoming, p.intent, p.plan)
        patch.setattr(module, "read_interpretation_state_plan", lambda *args: (changed, archive))
        with pytest.raises(ValueError):
            module.read_finish_plan(p.incoming, p.intent, p.plan)


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_existing_unresolved_finish_never_plans_executes_or_repeats_model(
    frozen, monkeypatch, state
):
    p = frozen
    runtime = SimpleNamespace(
        store=SimpleNamespace(
            lookup=AsyncMock(return_value=SimpleNamespace(acquired=False, state=state))
        ),
        reserve=AsyncMock(),
        execute=AsyncMock(),
    )
    sender = InterpretationFinishSender(p.store, SimpleNamespace(), runtime, **p.f.keys)
    sender._planner.candidate = AsyncMock()
    from qs_ai.infrastructure.workflow_transport import interpretation_finish_sender as send_module

    monkeypatch.setattr(
        send_module, "finish_request_id", AsyncMock(return_value=p.intent.request_id)
    )
    with pytest.raises(InterpretationFinishPending):
        await sender(p.incoming.claim, p.incoming.result)
    sender._planner.candidate.assert_not_called()
    runtime.reserve.assert_not_called()
    runtime.execute.assert_not_called()


async def test_lease_heartbeat_does_not_change_business_source_hash(frozen):
    p = frozen
    claim = p.incoming.claim
    job = dict(
        id=claim.job_id,
        session_id=claim.session.id,
        run_id=claim.run_id,
        status="leased",
        fence_token=claim.fence,
        attempt=1,
        answer=claim.answer,
        skipped=claim.skipped,
        question_id=claim.question_id,
        lease_until="before",
    )
    first = module._source_hash(claim.session, job)
    job["lease_until"] = "extended by original heartbeat"
    assert module._source_hash(claim.session, job) == first
    job["fence_token"] += 1
    assert module._source_hash(claim.session, job) != first
