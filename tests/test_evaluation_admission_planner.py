"""Complete evaluation plans freeze original receipt/state and sequence without business writes."""

import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4, uuid5

import pytest
from jwcrypto import jwk

from qs_ai.application.evaluation.capacity import CapacityExceeded
from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport import evaluation_planner as planning
from qs_ai.infrastructure.workflow_transport.evaluation_plan import EvaluationPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.test_evaluation_admission_projection import fixture as read_fixture
from tests.test_transaction_command import command_fixture


def fixture(monkeypatch, kind=pb.EVALUATION_START, previous=None):
    read = read_fixture(monkeypatch)
    envelope, raw, wire = command_fixture(kind)
    body = pb.MessagingBody.FromString(raw)
    request = getattr(body, planning.FIELDS[kind])
    request.scope.run_id = str(read.scope.run_id)
    request.expected_version, request.reason, request.confirm = 1, "explicit authority", True
    envelope.aggregate_key = str(read.scope.run_id)
    raw = body.SerializeToString(deterministic=True)
    envelope.body_sha256, envelope.body_length, envelope.inline_body = (
        hashlib.sha256(raw).hexdigest(),
        len(raw),
        raw,
    )
    incoming = FrozenCommandInput.from_authenticated(envelope, raw, wire)
    original = read.db.execute

    async def execute(statement):
        if "messaging_evaluation_sequences" in str(statement):
            result = Mock()
            result.mappings.return_value.one_or_none.return_value = previous
            return result
        return await original(statement)

    read.db.execute = AsyncMock(side_effect=execute)
    root = SimpleNamespace(is_active=True)
    read.db.get_transaction = lambda: root
    read.db.connection = AsyncMock()
    active = False

    @asynccontextmanager
    async def open_read():
        nonlocal active
        active = True
        try:
            yield read.db
        finally:
            active = False

    identities = []

    def identity():
        assert not active
        value = str(uuid4())
        identities.append(value)
        return value

    real_prepare = planning.prepare

    def prepare(*args, **kwargs):
        assert not active, "no signing/encryption inside original read transaction"
        return real_prepare(*args, **kwargs)

    spy = Mock(side_effect=prepare)
    monkeypatch.setattr(planning, "prepare", spy)
    planner = planning.EvaluationAdmissionPlanner(
        SimpleNamespace(open=open_read),
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        clock=lambda: read.f.at,
        new_id=identity,
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("previous", [None, {"sequence": 4, "version": 1}])
async def test_actual_start_plan_reuses_original_transition_and_freezes_first_sequence(
    monkeypatch, previous
):
    f = fixture(monkeypatch, previous=previous)
    plan = await f.planner.candidate(f.incoming)
    facts = EvaluationPlanFacts.from_plan(f.incoming, plan)
    values = read_admission_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(values[0].body).command_receipt
    state = pb.MessagingBody.FromString(values[1].body).evaluation_state
    assert len(f.identities) == 3 and len(values) == 2
    assert facts.source_digest is not None and facts.at == f.read.f.at
    assert (
        receipt.evaluation_receipt.status == "collecting"
        and receipt.evaluation_receipt.version == 2
    )
    assert state.event_sequence == (5 if previous else 1) and state.version == 2
    assert values[1].envelope().message_id == str(uuid5(UUID(state.run_id), "state:2"))
    assert EvaluationPlanFacts.decode(facts.encode()) == facts
    f.read.db.commit.assert_not_called()
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.read.db.execute.await_args_list
    )


async def test_cancel_freezes_original_explicit_false_discard_and_state(monkeypatch):
    f = fixture(monkeypatch, pb.EVALUATION_CANCEL)
    from qs_ai.infrastructure.persistence.mysql import evaluation_admission as projection

    planned = AsyncMock(
        return_value=(
            CheckpointState(f.read.scope.run_id, 2, None),
            {"status": "canceled", "transitions": []},
        )
    )
    monkeypatch.setattr(projection, "plan_cancellation", planned)
    plan = await f.planner.candidate(f.incoming)
    EvaluationPlanFacts.from_plan(f.incoming, plan)
    assert planned.await_args.kwargs == {"discard": False, "confirm": True}
    assert (
        pb.MessagingBody.FromString(
            read_admission_plan(f.incoming, plan)[1].body
        ).evaluation_state.status
        == "canceled"
    )


async def test_capacity_refusal_freezes_receipt_only_and_no_business_state(monkeypatch):
    f = fixture(monkeypatch)
    f.read.budget.side_effect = CapacityExceeded("daily capacity")
    plan = await f.planner.candidate(f.incoming)
    assert len(plan.branches) == 1
    receipt = pb.MessagingBody.FromString(
        read_admission_plan(f.incoming, plan)[0].body
    ).command_receipt
    assert receipt.decision == pb.REJECTED and receipt.code == "evaluation_capacity_exceeded"


async def test_unknown_database_does_not_freeze_false_refusal_or_sign_any_message(monkeypatch):
    f = fixture(monkeypatch)
    f.read.db.execute.side_effect = ConnectionError("unavailable")
    with pytest.raises(ConnectionError):
        await f.planner.candidate(f.incoming)
    f.spy.assert_not_called()


@pytest.mark.parametrize("damage", ["input", "kind", "target", "source", "sequence", "clock"])
async def test_durable_winner_context_is_exact_not_reconstructed_from_candidate(
    monkeypatch, damage
):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    facts = EvaluationPlanFacts.from_plan(f.incoming, plan)
    changes = {
        "input": {"input_hash": b"x" * 32},
        "kind": {"kind": pb.EVALUATION_CANCEL},
        "target": {"run_id": str(uuid4())},
        "source": {"source_digest": None},
        "sequence": {"previous_sequence": 3, "previous_version": 1},
        "clock": {"at": facts.at + timedelta(seconds=1)},
    }[damage]
    # Rebuild only host frames; original first bodies/wires remain untouched.
    from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, PreparedMessage
    from qs_ai.infrastructure.workflow_transport.transaction_archive import (
        decode_branch,
        freeze_branch,
    )

    branches = []
    for branch in plan.branches:
        a = decode_branch(branch)
        branches.append(
            freeze_branch(
                PreparedMessage(a.envelope(), a.body, a.wire, EVENTS),
                str(a.organization_id),
                a.sequence,
                a.ordered,
                a.requires_receipt,
                branch.branch_id,
                branch.physical_topic,
                "m3-1",
                context=replace(facts, **changes).encode(),
            )
        )
    with pytest.raises(ValueError):
        EvaluationPlanFacts.from_plan(f.incoming, replace(plan, branches=tuple(branches)))


@pytest.mark.parametrize(
    "damage", ["duplicate", "unknown", "noncanonical", "bounds", "bool-sequence"]
)
async def test_context_codec_rejects_ambiguous_versions_and_identity_ranges(monkeypatch, damage):
    f = fixture(monkeypatch)
    facts = EvaluationPlanFacts.from_plan(f.incoming, await f.planner.candidate(f.incoming))
    raw = facts.encode()
    if damage == "duplicate":
        raw = raw.replace(b"{", b'{"kind":8,', 1)
    elif damage == "unknown":
        raw = raw.replace(b"/v1", b"/v9")
    elif damage == "noncanonical":
        raw += b" "
    elif damage == "bounds":
        raw = b"x" * 16385
    else:
        raw = raw.replace(b'"previous_sequence":0', b'"previous_sequence":false')
    with pytest.raises(ValueError):
        EvaluationPlanFacts.decode(raw)
