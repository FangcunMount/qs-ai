"""Freeze the first preflight evidence separately from stable operation identity."""

import hashlib
from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    PreflightOperation,
    preflight_intent,
    source_snapshot_digest,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from tests.test_evaluation_preflight_plan import fixture


def facts():
    f = fixture()
    original = PreflightOperation(source_snapshot_digest(f.checkpoint, f.run), f.evidence)
    intent = preflight_intent(f.run_id, 2, 7)
    value = EvaluationStateFacts(intent, f.at, 0, 0, original.encode())
    return f, original, value


def test_first_evidence_time_and_source_round_trip_without_changing_stable_request():
    f, original, value = facts()
    assert PreflightOperation.from_facts(value) == original
    assert PreflightOperation.decode(original.encode()) == original
    later = replace(
        original,
        snapshot_digest=hashlib.sha256(b"changed mutable snapshot").digest(),
        evidence=replace(f.evidence, evaluated_at=f.at + timedelta(seconds=1)),
    )
    other = replace(value, at=later.evidence.evaluated_at, business_context=later.encode())
    assert PreflightOperation.from_facts(other) == later
    assert value.intent.request == other.intent.request
    assert hashlib.sha256(value.encode()).digest() != hashlib.sha256(other.encode()).digest()
    assert value.intent.request == preflight_intent(f.run_id, 2, 7).request


@pytest.mark.parametrize(
    "damage", ["version", "progress", "checkpoint", "definition", "organization"]
)
def test_source_snapshot_detects_original_business_change(damage):
    f, original, _ = facts()
    checkpoint, run = dict(f.checkpoint), dict(f.run)
    if damage == "version":
        checkpoint["version"] += 1
    elif damage == "progress":
        run["progress_json"] = {"status": "blocked"}
    elif damage == "checkpoint":
        checkpoint["checkpoint_json"] = {"dispatched": True}
    elif damage == "definition":
        run["definition_json"] += " "
    else:
        run["organization_id"] = 8
    assert source_snapshot_digest(checkpoint, run) != original.snapshot_digest


def test_context_cannot_authorize_a_different_operation_or_replace_frozen_clock():
    _, _, value = facts()
    with pytest.raises(ValueError):
        PreflightOperation.from_facts(replace(value, at=value.at + timedelta(seconds=1)))
    with pytest.raises(ValueError):
        PreflightOperation.from_facts(
            replace(value, intent=replace(value.intent, operation_key="dispatch/2"))
        )
    with pytest.raises(ValueError):
        PreflightOperation.from_facts(replace(value, business_context=b""))


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "noncanonical", "version"])
def test_ambiguous_or_unrecognized_preflight_evidence_is_not_normalized(damage):
    _, original, _ = facts()
    raw = original.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"snapshot_digest":"' + original.snapshot_digest.hex().encode() + b'"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"unexpected":true}'
    elif damage == "noncanonical":
        raw = raw.replace(b'"snapshot_digest":', b'"snapshot_digest": ')
    else:
        raw = raw.replace(b"/v1", b"/v2", 1)
    with pytest.raises(ValueError):
        PreflightOperation.decode(raw)
