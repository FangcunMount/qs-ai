"""Complete first-wire plans; opaque unit wire never claims crypto authentication."""

from dataclasses import replace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan
from reliable_messaging.wire import Envelope, encode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    METADATA,
    SCHEMA,
    PreparedMessage,
)
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
    read_admission_plan,
)
from tests.test_transaction_command import command_fixture


def publication(kind, body, aggregate, correlation=""):
    import hashlib

    raw = body.SerializeToString(deterministic=True)
    identity = str(uuid4())
    if kind == pb.INTERPRETATION_STATE:
        body.interpretation_state.event_id = identity
        raw = body.SerializeToString(deterministic=True)
    header = pb.MessagingEnvelope(
        schema_version=SCHEMA,
        producer="qs-ai",
        destination="qs-server",
        message_id=identity,
        kind=kind,
        aggregate_key=aggregate,
        correlation_command_id=correlation,
        original_occurred_at="2026-10-21T06:10:00+08:00",
        body_sha256=hashlib.sha256(raw).hexdigest(),
        body_length=len(raw),
        inline_body=raw,
    )
    wire = encode(
        Envelope(identity, b"opaque-first-publication-" + identity.encode(), dict(METADATA))
    )
    return PreparedMessage(header, raw, wire, EVENTS)


def fixture(decision=pb.ACCEPTED):
    command, raw, wire = command_fixture()
    incoming = FrozenCommandInput.from_authenticated(command, raw, wire)
    session, run = str(uuid4()), str(uuid4())
    receipt = publication(
        pb.COMMAND_RECEIPT,
        pb.MessagingBody(
            command_receipt=pb.MessagingCommandReceipt(
                command_id=command.message_id,
                command_body_sha256=command.body_sha256,
                decision=decision,
                workflow_receipt=workflow.Receipt(
                    session_id=session, run_id=run, version=2, status="queued"
                ),
            )
        ),
        command.aggregate_key,
        command.message_id,
    )
    state = publication(
        pb.INTERPRETATION_STATE,
        pb.MessagingBody(
            interpretation_state=workflow.StateEvent(
                request_id=command.aggregate_key,
                session_id=session,
                actor=workflow.Actor(org_id="7", subject_id="9"),
                testee_id="19",
                version=2,
                status="queued",
            )
        ),
        command.aggregate_key,
    )
    return incoming, receipt, state


def test_plan_round_trip_retains_every_first_body_wire_and_route_without_signing_keys():
    incoming, receipt, state = fixture()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, 2)))
    restored = decode_plan(encode_plan(plan))
    archives = read_admission_plan(incoming, restored)
    assert len(archives) == 2
    assert [(a.body, a.wire) for a in archives] == [
        (receipt.body, receipt.wire),
        (state.body, state.wire),
    ]
    assert all(a.requires_receipt and not a.ordered for a in archives)
    receipt.envelope.aggregate_key = "caller mutation"
    assert read_admission_plan(incoming, restored)[0].envelope().aggregate_key != "caller mutation"


def test_candidate_receipt_identity_and_cipher_do_not_change_stable_incoming_request():
    incoming, receipt, state = fixture()
    first = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, 2)))
    replacement = publication(
        pb.COMMAND_RECEIPT,
        pb.MessagingBody(
            command_receipt=pb.MessagingCommandReceipt(
                command_id=incoming.envelope().message_id,
                command_body_sha256=incoming.envelope().body_sha256,
                decision=pb.REJECTED,
                code="state_conflict",
                grpc_status_code=10,
            )
        ),
        incoming.envelope().aggregate_key,
        incoming.envelope().message_id,
    )
    other = freeze_admission_plan(incoming, str(uuid4()), ((replacement, 1),))
    assert first.digest != other.digest
    incoming.request.match(first)
    incoming.request.match(other)
    # This only compares candidates; it never authorizes automatic Replan/Retry.


def test_evaluation_plan_preserves_business_version_and_independent_event_sequence():
    command, body, wire = command_fixture(pb.EVALUATION_START)
    incoming = FrozenCommandInput.from_authenticated(command, body, wire)
    receipt = publication(
        pb.COMMAND_RECEIPT,
        pb.MessagingBody(
            command_receipt=pb.MessagingCommandReceipt(
                command_id=command.message_id,
                command_body_sha256=command.body_sha256,
                decision=pb.ACCEPTED,
                evaluation_receipt=workflow.EvaluationState(
                    run_id=command.aggregate_key,
                    version=2,
                    status="collecting",
                ),
            )
        ),
        command.aggregate_key,
        command.message_id,
    )
    state = publication(
        pb.EVALUATION_STATE,
        pb.MessagingBody(
            evaluation_state=pb.EvaluationRuntimeState(
                run_id=command.aggregate_key,
                organization_id="7",
                version=2,
                event_sequence=9,
                status="collecting",
                release_fingerprint="a" * 64,
            )
        ),
        command.aggregate_key,
    )
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, 9)))
    restored = read_admission_plan(incoming, decode_plan(encode_plan(plan)))
    assert restored[1].sequence == 9
    assert pb.MessagingBody.FromString(restored[1].body).evaluation_state.version == 2


@pytest.mark.parametrize("wrong", ["owner", "scope", "physical", "revision", "branch-id", "wire"])
def test_durable_winner_rejects_wrong_identity_route_or_first_wire(wrong):
    incoming, receipt, _ = fixture()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2),))
    branch = plan.branches[0]
    with pytest.raises(ValueError):
        if wrong == "owner":
            plan = replace(plan, owner="qs.mysql")
        elif wrong == "scope":
            plan = replace(plan, scope="org:8")
        else:
            changes = {
                "physical": {"physical_topic": "wrong"},
                "revision": {"route_revision": "future"},
                "branch-id": {"branch_id": str(uuid4())},
                "wire": {"wire": b"wrong"},
            }
            plan = replace(plan, branches=(replace(branch, **changes[wrong]),))
        read_admission_plan(incoming, plan)


@pytest.mark.parametrize("wrong", ["request", "session", "version", "sequence"])
def test_frozen_state_must_describe_the_same_business_receipt(wrong):
    incoming, receipt, state = fixture()
    body = pb.MessagingBody.FromString(state.body)
    sequence = 2
    if wrong == "request":
        body.interpretation_state.request_id = str(uuid4())
    elif wrong == "session":
        body.interpretation_state.session_id = str(uuid4())
    elif wrong == "version":
        body.interpretation_state.version = 3
    else:
        sequence = 3
    state = publication(pb.INTERPRETATION_STATE, body, incoming.envelope().aggregate_key)
    with pytest.raises(ValueError):
        freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, sequence)))


@pytest.mark.parametrize("wrong", ["missing", "duplicate", "held-state", "foreign-receipt"])
def test_plan_requires_one_exact_receipt_and_never_applies_business_for_held(wrong):
    incoming, receipt, state = fixture(pb.HELD if wrong == "held-state" else pb.ACCEPTED)
    if wrong == "missing":
        publications = ((state, 2),)
    elif wrong == "duplicate":
        other = publication(
            pb.COMMAND_RECEIPT,
            pb.MessagingBody.FromString(receipt.body),
            incoming.envelope().aggregate_key,
            incoming.envelope().message_id,
        )
        publications = ((receipt, 2), (other, 2))
    elif wrong == "foreign-receipt":
        other = pb.MessagingBody.FromString(receipt.body)
        other.command_receipt.command_body_sha256 = "b" * 64
        receipt = publication(
            pb.COMMAND_RECEIPT,
            other,
            incoming.envelope().aggregate_key,
            incoming.envelope().message_id,
        )
        publications = ((receipt, 2),)
    else:
        publications = ((receipt, 2), (state, 2))
    with pytest.raises(ValueError):
        freeze_admission_plan(incoming, str(uuid4()), publications)
