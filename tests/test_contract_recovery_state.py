"""A policy exception remains an explicit trusted-host authorization, not model work."""

import hashlib
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4, uuid5

import pytest
from reliable_messaging.transaction import State

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.contract_recovery_sender import (
    ContractRecoveryPending,
    ContractRecoverySender,
)
from qs_ai.infrastructure.workflow_transport.contract_recovery_state import (
    ContractRecoveryInput,
    read_contract_recovery_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from tests.test_evaluation_state_plan import fixture as state_fixture
from tests.test_semantic_contract_recovery import recovered_inventory


@pytest.fixture
def frozen():
    f = state_fixture()
    value = recovered_inventory()["contract_recoveries"][0]
    incoming = ContractRecoveryInput(UUID(f.intent.run_id), 7, 7, "sha256:" + "a" * 64, value, True)
    digest = hashlib.sha256(b"original failed evidence and original projected exception").digest()
    facts = EvaluationStateFacts(incoming.intent, value.resolved_at, 3, 7, incoming.encode(digest))
    state = pb.EvaluationRuntimeState(
        run_id=str(incoming.run_id),
        organization_id="7",
        version=8,
        event_sequence=4,
        status="collecting",
        release_fingerprint=incoming.expected_release_fingerprint,
    )
    message = prepare(
        pb.EVALUATION_STATE,
        str(uuid5(incoming.run_id, "state:8")),
        str(incoming.run_id),
        pb.MessagingBody(evaluation_state=state),
        organization_id="7",
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        **f.keys,
    )
    plan = freeze_evaluation_state_plan(facts, str(uuid4()), message)
    return SimpleNamespace(**locals())


def test_first_operator_authorization_audit_and_frozen_response_are_durable_without_keys(frozen):
    p = frozen
    later = replace(
        p.incoming, value=replace(p.value, resolved_at=p.value.resolved_at + timedelta(seconds=10))
    )
    assert later.intent == p.incoming.intent
    facts, first, source, archive = read_contract_recovery_plan(later, p.plan)
    assert first == p.incoming and source == p.digest and archive.wire == p.message.wire
    assert facts.at == p.value.resolved_at
    changed = first.response()
    changed["reason"] = "changed"
    assert first.response()["reason"] == p.value.reason
    assert first.value.actor not in repr(first)


@pytest.mark.parametrize(
    "field", ["expected_release_fingerprint", "actor", "reason", "execution", "organization"]
)
def test_different_business_input_cannot_reuse_first_exception(frozen, field):
    p = frozen
    if field == "expected_release_fingerprint":
        changed = replace(p.incoming, expected_release_fingerprint="sha256:" + "b" * 64)
    elif field == "organization":
        changed = replace(p.incoming, organization_id=8)
    else:
        values = {
            "actor": {"actor": "operator:other"},
            "reason": {"reason": "other explicit exception"},
            "execution": {"execution_id": "execution:other"},
        }
        changed = replace(p.incoming, value=replace(p.value, **values[field]))
    with pytest.raises(ValueError):
        read_contract_recovery_plan(changed, p.plan)


@pytest.mark.parametrize(
    "damage", ["duplicate", "extra", "whitespace", "trailing", "magic", "oversize", "clock"]
)
def test_invalid_facts_do_not_authorize_an_exception(frozen, damage):
    p = frozen
    raw = p.facts.business_context
    if damage == "duplicate":
        raw = raw[:-1] + b',"source_digest":"' + b"a" * 64 + b'"}'
    elif damage == "extra":
        raw = raw[:-1] + b',"extra":true}'
    elif damage == "whitespace":
        raw = raw.replace(b'"operation":', b'"operation": ')
    elif damage == "trailing":
        raw += b"\n"
    elif damage == "magic":
        raw = raw.replace(b"/v1", b"/v2")
    elif damage == "oversize":
        raw += b"a" * 8192
    else:
        with pytest.raises(ValueError):
            ContractRecoveryInput.from_facts(replace(p.facts, at=p.facts.at + timedelta(seconds=1)))
        return
    with pytest.raises(ValueError):
        ContractRecoveryInput.from_facts(replace(p.facts, business_context=raw))


@pytest.mark.parametrize(
    "field,value",
    [
        ("confirm", False),
        ("confirm", 1),
        ("organization_id", True),
        ("organization_id", 2**64),
        ("expected_version", True),
        ("expected_version", 2**63 - 1),
    ],
)
def test_explicit_confirmation_and_bounded_identity_before_network(frozen, field, value):
    with pytest.raises(ValueError):
        replace(frozen.incoming, **{field: value})


def test_ordinary_management_actor_is_not_a_trusted_host_operator(frozen):
    with pytest.raises(ValueError):
        replace(frozen.incoming, value=replace(frozen.value, actor="user:42"))


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_unknown_or_aborted_execution_never_replans_or_reexecutes_original_authorization(
    frozen, state
):
    runtime = SimpleNamespace(
        store=SimpleNamespace(
            lookup=AsyncMock(return_value=SimpleNamespace(acquired=False, state=state))
        ),
        reserve=AsyncMock(),
        execute=AsyncMock(),
    )
    tx = SimpleNamespace(
        open=AsyncMock(side_effect=AssertionError("No business transaction from unresolved lookup"))
    )
    dispatch = ContractRecoverySender(
        tx, object(), runtime, signing_key=object(), recipient_key=object()
    )
    with pytest.raises(ContractRecoveryPending):
        await dispatch(frozen.incoming)
    runtime.reserve.assert_not_called()
    runtime.execute.assert_not_called()
    tx.open.assert_not_called()
