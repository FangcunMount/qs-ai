"""Frozen terminal context is small, private and distinct from provider retry authority."""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport import serial_generation_state as module
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from tests.test_evaluation_serial_response_receipts import original as original
from tests.test_evaluation_state_plan import fixture as state_fixture
from tests.test_generation_completion import completion


@pytest.fixture
def frozen(original):
    p, f = original, state_fixture()
    digest = hashlib.sha256(b"first hidden source").digest()
    operation = module.SerialGenerationOperation(
        p.ref, module.response_digest(p.result), digest, digest, digest, "candidate:first"
    )
    intent = operation.intent
    facts = EvaluationStateFacts(intent, p.at, 8, 5, operation.encode(), 6)
    state = pb.EvaluationRuntimeState(
        run_id=intent.run_id,
        organization_id="1",
        version=7,
        event_sequence=9,
        status="collecting",
        release_fingerprint=f.state.release_fingerprint,
    )
    first = prepare(
        pb.EVALUATION_STATE,
        str(uuid5(p.ref.run_id, "state:7")),
        intent.run_id,
        pb.MessagingBody(evaluation_state=state),
        organization_id="1",
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        **f.keys,
    )
    winner = freeze_evaluation_state_plan(facts, str(uuid4()), first)
    return SimpleNamespace(**locals())


def test_source_and_candidate_freeze_separately_from_stable_original_response_request(frozen):
    p = frozen
    assert module.SerialGenerationOperation.decode(p.operation.encode()) == p.operation
    assert module.read_serial_generation_plan(p.intent, p.winner) == (p.facts, p.operation)
    assert (
        replace(p.operation, source_hash=b"s" * 32, candidate_id="candidate:other").intent
        == p.intent
    )
    assert (
        replace(p.operation, response_hash=b"r" * 32).intent.request.input_hash
        != p.intent.request.input_hash
    )
    assert len(p.intent.operation_key) < 128
    assert "candidate:first" not in repr(p.operation) and "reference" not in repr(p.operation)
    assert p.p.response.raw_output.encode() not in p.operation.encode()


@pytest.mark.parametrize(
    "damage",
    [
        "duplicate",
        "extra",
        "whitespace",
        "trailing",
        "magic",
        "oversize",
        "boolean",
        "semantic",
        "upper_hash",
        "candidate",
        "negative_version",
    ],
)
def test_wrong_or_ambiguous_first_terminal_context_is_rejected(frozen, damage):
    p = frozen
    raw = p.operation.encode()
    value = json.loads(raw[len(module._MAGIC) :])
    if damage == "extra":
        value["extra"] = True
    if damage == "boolean":
        value["reference"]["organization_id"] = True
    if damage == "semantic":
        value["reference"]["checkpoint"].update(kind="semantic", candidate_id="candidate:original")
    if damage == "candidate":
        value["candidate_id"] = " wrong"
    if damage == "negative_version":
        value["reference"]["version"] = -1
    raw = module._MAGIC + module._json(value)
    if damage == "duplicate":
        raw = raw[:-1] + b',"candidate_id":"candidate:first"}'
    if damage == "whitespace":
        raw += b" "
    if damage == "trailing":
        raw += b"\n"
    if damage == "magic":
        raw = raw.replace(b"/v1", b"/v2", 1)
    if damage == "oversize":
        raw += b"x" * 8192
    if damage == "upper_hash":
        raw = raw.replace(
            p.operation.source_hash.hex().encode(), p.operation.source_hash.hex().upper().encode()
        )
    with pytest.raises(ValueError):
        module.SerialGenerationOperation.decode(raw)


@pytest.mark.parametrize("damage", ["reference", "expected_version", "target", "source_missing"])
def test_body_target_and_stable_reference_are_both_checked(frozen, damage):
    p = frozen
    facts = p.facts
    if damage == "reference":
        facts = replace(
            facts,
            business_context=replace(
                p.operation, reference=replace(p.p.ref, organization_id=2)
            ).encode(),
        )
    elif damage == "expected_version":
        facts = replace(facts, intent=replace(p.intent, expected_version=4))
    elif damage == "source_missing":
        facts = replace(facts, source_version=None)
    else:
        facts = replace(facts, source_version=5)
    winner = freeze_evaluation_state_plan(facts, str(uuid4()), p.first)
    with pytest.raises((ValueError, MessageConflict)):
        module.read_serial_generation_plan(facts.intent, winner)


def test_completion_digest_binds_exact_outputs_receipt_and_time_without_persisting_them():
    first = completion()
    digest = module.completion_digest(first)
    assert digest == module.completion_digest(first)
    assert module.completion_digest(replace(first, raw_output=b"different first bytes")) != digest
    assert (
        module.completion_digest(
            replace(first, receipt=replace(first.receipt, request_id="provider:other"))
        )
        != digest
    )
