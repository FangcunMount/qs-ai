"""Freeze target authorization with bounded explicit CHANGE context, no raw business body."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from qs_ai.infrastructure.workflow_transport.change_plan import ChangePlanFacts
from qs_ai.infrastructure.workflow_transport.frozen_authorization import EvidenceAuthorization


def facts():
    at = datetime(2026, 10, 21, tzinfo=UTC)
    return ChangePlanFacts(
        b"x" * 32,
        at,
        str(uuid4()),
        *(str(uuid4()) for _ in range(4)),
        authorization=EvidenceAuthorization(b"t" * 32, True, at),
    )


def test_change_frozen_authorization_context_roundtrips_without_new_resources():
    value = facts()
    assert ChangePlanFacts.decode(value.encode()) == value
    assert value.identity("run") == value.run_id
    with pytest.raises(RuntimeError):
        value.identity("session")
    assert "xxxxxxxx" not in repr(value) and "tttttttt" not in repr(value)
    assert ChangePlanFacts.decode(replace(value, authorization=None).encode()).authorization is None


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
        ChangePlanFacts.decode(raw)


@pytest.mark.parametrize("age", [-1, 61])
def test_stale_or_future_authorization_is_not_root_execution_permission(age):
    value = facts()
    with pytest.raises(RuntimeError):
        value.validate_observation(value.at + timedelta(seconds=age))


def test_fresh_snapshot_and_remote_observations_have_explicit_root_time_boundary():
    value = facts()
    value.validate_observation(value.at + timedelta(seconds=60))
    replace(value, authorization=None).validate_observation(value.at)
