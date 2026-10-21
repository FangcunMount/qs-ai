"""Original audit, trusted actor and stable resolution input survive frozen round trips."""

import json
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from qs_ai.infrastructure.workflow_transport.resolution_state_plan import ResolutionStateOperation
from tests.test_evaluation_resolution import AT, decision


def original():
    scope = ManagementScope(uuid4(), 1, 42)
    operation = ResolutionStateOperation(scope, uuid4(), 7, decision(actor=scope.actor), True)
    facts = EvaluationStateFacts(operation.intent, AT, 4, 7, operation.encode(b"s" * 32))
    return operation, facts


def test_first_resolution_audit_and_source_roundtrip_exactly():
    operation, facts = original()
    restored, digest = ResolutionStateOperation.from_facts(facts)
    assert restored == operation and digest == b"s" * 32
    assert restored.encode(digest) == facts.business_context
    assert len(facts.business_context) < 8192
    assert operation.value.reason not in repr(operation)


def test_clock_change_does_not_change_stable_original_resolution_input():
    operation, facts = original()
    later = replace(operation, value=replace(operation.value, resolved_at=AT + timedelta(hours=1)))
    assert later.intent == operation.intent
    assert later.encode(b"s" * 32) != operation.encode(b"s" * 32)
    assert ResolutionStateOperation.from_facts(facts)[0] == operation


@pytest.mark.parametrize("field", ["reason", "decision", "target", "operation", "version", "scope"])
def test_business_input_and_identity_changes_do_not_reuse_the_first_intent(field):
    operation, _ = original()
    if field == "reason":
        changed = replace(operation, value=replace(operation.value, reason="另一项授权"))
    elif field == "decision":
        changed = replace(operation, value=replace(operation.value, decision="cancel_run"))
    elif field == "target":
        changed = replace(
            operation, value=replace(operation.value, execution_id="another:execution")
        )
    elif field == "operation":
        changed = replace(operation, operation_id=uuid4())
    elif field == "version":
        changed = replace(operation, expected_version=8)
    else:
        changed = replace(operation, scope=replace(operation.scope, organization_id=2))
    assert changed.intent != operation.intent


@pytest.mark.parametrize("fault", ["actor", "confirm", "version_bool", "version_max", "zero_id"])
def test_invalid_trusted_input_is_refused_before_io(fault):
    operation, _ = original()
    changes = {
        "actor": {"value": replace(operation.value, actor="user:43")},
        "confirm": {"confirm": False},
        "version_bool": {"expected_version": True},
        "version_max": {"expected_version": 2**63 - 1},
        "zero_id": {"operation_id": UUID(int=0)},
    }
    with pytest.raises(ValueError):
        replace(operation, **changes[fault])


@pytest.mark.parametrize("fault", ["unknown", "duplicate", "noncanonical", "naive", "bad_digest"])
def test_malformed_frozen_protocol_cannot_become_an_original_resolution(fault):
    operation, facts = original()
    marker, raw = facts.business_context.split(b"\0", 1)
    value = json.loads(raw)
    if fault == "unknown":
        value["extra"] = True
    elif fault == "naive":
        value["operation"]["value"]["resolved_at"] = AT.replace(tzinfo=None).isoformat()
    elif fault == "bad_digest":
        value["snapshot_digest"] = "ff"
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if fault == "duplicate":
        encoded = encoded[:-1] + b',"snapshot_digest":"' + b"73" * 32 + b'"}'
    elif fault == "noncanonical":
        encoded += b" "
    with pytest.raises(ValueError):
        ResolutionStateOperation.from_facts(
            replace(facts, business_context=marker + b"\0" + encoded)
        )


def test_wrong_reference_or_fact_clock_does_not_confirm_first_resolution():
    operation, facts = original()
    with pytest.raises(ValueError):
        ResolutionStateOperation.from_facts(replace(facts, at=AT + timedelta(seconds=1)))
    changed = replace(operation, value=replace(operation.value, reason="另一项授权"))
    with pytest.raises(ValueError):
        ResolutionStateOperation.from_facts(replace(facts, intent=changed.intent))
