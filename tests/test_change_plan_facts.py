"""Freeze target authorization with bounded explicit CHANGE context, no raw business body."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from qs_ai.infrastructure.workflow_transport.frozen_authorization import EvidenceAuthorization
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts


def facts():
    at = datetime(2026, 10, 21, tzinfo=UTC)
    return InterpretationMutationFacts(
        b"x" * 32,
        at,
        str(uuid4()),
        *(str(uuid4()) for _ in range(4)),
        authorization=EvidenceAuthorization(b"t" * 32, True, at),
    )


def test_change_frozen_authorization_context_roundtrips_without_new_resources():
    value = facts()
    assert InterpretationMutationFacts.decode(value.encode()) == value
    assert value.identity("run") == value.run_id
    with pytest.raises(RuntimeError):
        value.identity("session")
    assert "xxxxxxxx" not in repr(value) and "tttttttt" not in repr(value)
    assert (
        InterpretationMutationFacts.decode(
            replace(value, authorization=None).encode()
        ).authorization
        is None
    )


@pytest.mark.parametrize(
    "changed", ["unknown", "duplicate", "noncanonical", "oversized", "wrong-version", "bad-proof"]
)
def test_change_context_rejects_corrupt_or_ambiguous_persisted_bytes(changed):
    raw = facts().encode()
    if changed == "unknown":
        raw = raw.replace(b'{"at":', b'{"unknown":1,"at":', 1)
    elif changed == "duplicate":
        raw = raw.replace(b'{"at":', b'{"at":"ignored","at":', 1)
    elif changed == "noncanonical":
        raw += b" "
    elif changed == "oversized":
        raw += b" " * 16384
    elif changed == "wrong-version":
        raw = raw.replace(b"/v1", b"/v2", 1)
    else:
        raw = raw.replace(b'"allowed":true', b'"allowed":1')
    with pytest.raises(ValueError):
        InterpretationMutationFacts.decode(raw)


@pytest.mark.parametrize("age", [-1, 61])
def test_stale_or_future_authorization_is_not_root_execution_permission(age):
    value = facts()
    with pytest.raises(RuntimeError):
        value.validate_observation(value.at + timedelta(seconds=age))


def test_fresh_snapshot_and_remote_observations_have_explicit_root_time_boundary():
    value = facts()
    value.validate_observation(value.at + timedelta(seconds=60))
    replace(value, authorization=None).validate_observation(value.at)


def test_existing_change_context_keeps_its_original_binary_encoding():
    import hashlib

    at = datetime(2026, 10, 21, tzinfo=UTC)
    value = InterpretationMutationFacts(
        b"x" * 32,
        at,
        "00000000-0000-4000-8000-000000000001",
        *("00000000-0000-4000-8000-00000000000" + str(i) for i in range(2, 6)),
        authorization=EvidenceAuthorization(b"t" * 32, True, at),
    )
    assert (
        hashlib.sha256(value.encode()).hexdigest()
        == "e330d857cb13191711131f987c184e5e9b9a5b4aa3120e943f3c69fbb815fd0c"
    )


def test_retry_audit_and_authorization_use_a_distinct_explicit_context_version():
    from qs_ai.contracts.workflow import messaging_pb2 as pb

    value = replace(facts(), kind=pb.PARTICIPANT_RETRY, audit_digest=b"a" * 32)
    raw = value.encode()
    assert raw.startswith(b"qs-ai-participant-retry-plan/v1\0")
    assert InterpretationMutationFacts.decode(raw) == value
    with pytest.raises(ValueError):
        InterpretationMutationFacts.decode(
            raw.replace(b"qs-ai-participant-retry-plan/v1", b"qs-ai-change-plan/v1", 1)
        )
    with pytest.raises(ValueError):
        replace(value, kind=pb.CHANGE)
    with pytest.raises(ValueError):
        replace(value, audit_digest=b"short")
