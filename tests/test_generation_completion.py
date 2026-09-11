import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import GenerationCompletion, ProviderReceipt


def completion():
    at = datetime(2026, 9, 12, tzinfo=UTC)
    raw = b'{ "result": true }'
    return GenerationCompletion(
        "execution:1",
        "case:1",
        1,
        1,
        "invocation:1",
        "succeeded",
        at,
        at + timedelta(seconds=1),
        1,
        ProviderReceipt("invocation:1", "request:1", "deepseek", "model", 1, 2, 100),
        raw,
        raw,
        "sha256:" + hashlib.sha256(raw).hexdigest(),
    )


@pytest.mark.parametrize(
    "change",
    [
        {"receipt": None},
        {"raw_output": b""},
        {"normalized_output": b""},
        {"normalized_fingerprint": "sha256:" + "0" * 64},
        {"provider_call_count": 0},
        {"raw_output": b"a" * (256 * 1024 + 1)},
        {"status": "prepared"},
        {"status": "result_unknown"},
    ],
)
def test_incomplete_or_conflicting_terminal_evidence_is_rejected(change):
    with pytest.raises(ValueError):
        replace(completion(), **change)


def test_fingerprint_preserves_exact_bytes_and_rejects_non_json_constants():
    value = completion()
    assert value.normalized_output == b'{ "result": true }'
    for raw in (b'{"result":true}', b"NaN", b"Infinity"):
        with pytest.raises(ValueError):
            replace(value, normalized_output=raw)
    with pytest.raises(ValueError):
        replace(value, receipt=replace(value.receipt, invocation_id="other:1"))


def test_completion_matches_exact_dispatched_identity_not_lease_expiry():
    value = completion()
    cp = ExecutionCheckpoint(
        "execution:1",
        "generation",
        "case:1",
        1,
        "",
        1,
        "worker:1",
        "invocation:1",
        "dispatching",
        value.started_at,
        value.started_at + timedelta(seconds=30),
        value.started_at,
    )
    assert value.matches_checkpoint(cp, "worker:1")
    assert replace(
        value, finished_at=cp.lease_expires_at + timedelta(seconds=10)
    ).matches_checkpoint(cp, "worker:1")
    assert not value.matches_checkpoint(cp, "worker:2")
    for change in ({"execution_id": "other:1"}, {"slot_ordinal": 2}, {"invocation_id": "other:1"}):
        assert not value.matches_checkpoint(replace(cp, **change), "worker:1")
