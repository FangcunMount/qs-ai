"""One correction fits frozen facts; later RPC clocks never change its stable input."""

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from tests.test_evaluation_review_correction import correction


def fixture():
    entry = correction()
    operation = CorrectionStateOperation(
        ManagementScope(uuid4(), 7, 42),
        entry.source_version,
        entry.command_id,
        entry.previous_review_fingerprint,
        entry.candidate_output_fingerprint,
        entry.review,
    )
    snapshot = b"s" * 32
    return operation, snapshot


@pytest.mark.parametrize("reason", ["原审核更正", "核" * 333, "\x00" * 1000])
async def test_complete_valid_original_correction_roundtrips_in_bounded_facts(reason):
    original, snapshot = fixture()
    operation = replace(original, value=replace(original.value, reason=reason))
    raw = operation.encode(snapshot)
    assert len(raw) <= 8192 and reason not in repr(operation)
    facts = EvaluationStateFacts(operation.intent, operation.value.reviewed_at, 8, 17, raw)
    assert len(facts.encode()) <= 16384
    restored, digest = CorrectionStateOperation.from_facts(facts)
    assert restored == operation and digest == snapshot
    later = replace(
        operation,
        value=replace(
            operation.value, reviewed_at=operation.value.reviewed_at + timedelta(seconds=5)
        ),
    )
    assert later.intent == operation.intent
    assert CorrectionStateOperation.from_facts(replace(facts, intent=later.intent))[0] == operation


@pytest.mark.parametrize("damage", ["reason", "actor", "command", "version", "fingerprint"])
def test_business_identity_or_input_changes_do_not_reuse_an_operation(damage):
    operation, _ = fixture()
    if damage == "reason":
        other = replace(operation, value=replace(operation.value, reason="另一授权输入"))
    elif damage == "actor":
        other = replace(
            operation,
            scope=replace(operation.scope, operator_user_id=43),
            value=replace(operation.value, reviewer="user:43"),
        )
    elif damage == "command":
        other = replace(operation, command_id=str(uuid4()))
    elif damage == "version":
        other = replace(operation, expected_version=operation.expected_version + 1)
    else:
        other = replace(operation, candidate_output_fingerprint="sha256:" + "c" * 64)
    assert other.intent.request.input_hash != operation.intent.request.input_hash


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "noncanonical", "codec", "intent"])
def test_correction_facts_do_not_accept_ambiguous_or_mismatched_identity(damage):
    operation, snapshot = fixture()
    raw = operation.encode(snapshot)
    facts = EvaluationStateFacts(operation.intent, operation.value.reviewed_at, 8, 17, raw)
    if damage == "duplicate":
        raw = raw[:-1] + b',"snapshot_digest":"' + b"a" * 64 + b'"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"extra":true}'
    elif damage == "noncanonical":
        raw = raw.replace(b'"operation":', b'"operation": ')
    elif damage == "codec":
        raw = raw.replace(b"/v1", b"/v2", 1)
    else:
        facts = replace(facts, intent=replace(facts.intent, operation_key="other/operation"))
    with pytest.raises(ValueError):
        CorrectionStateOperation.from_facts(replace(facts, business_context=raw))
