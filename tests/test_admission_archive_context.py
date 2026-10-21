"""Host-local plan facts bind to the fingerprint without changing original wire."""

from dataclasses import replace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError
from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_archive
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    freeze_admission_plan,
    read_admission_plan,
)
from tests.test_admission_plan import fixture
from tests.test_transaction_archive import fixture as golden


def test_v1_golden_remains_byte_identical_without_local_context():
    vector, archive = golden()
    assert archive.context == b""
    assert archive.encode().hex() == vector["payload_hex"]
    assert archive.message().fingerprint == vector["message_fingerprint"]


def test_frozen_local_context_round_trip_is_owned_and_preserves_original_business_wire():
    incoming, receipt, state = fixture()
    original = bytearray(b"host-local-frozen-proof")
    plan = freeze_admission_plan(
        incoming, str(uuid4()), ((receipt, 2), (state, 2)), context=original
    )
    original[:] = b"changed"
    restored = decode_plan(encode_plan(plan))
    values = read_admission_plan(incoming, restored)
    assert all(v.context == b"host-local-frozen-proof" for v in values)
    assert [(v.body, v.wire) for v in values] == [
        (receipt.body, receipt.wire),
        (state.body, state.wire),
    ]
    assert "host-local-frozen-proof" not in repr(values)
    plain = freeze_admission_plan(incoming, plan.operation_id, ((receipt, 2), (state, 2)))
    assert plan.digest != plain.digest
    assert restored.branches[0].message.content_type != plain.branches[0].message.content_type
    for value in values:
        assert decode_archive(value.encode(), value.wire) == value


@pytest.mark.parametrize("context", [b"a" * 16385, 3, "text"])
def test_invalid_local_context_is_refused_before_reservation(context):
    incoming, receipt, _ = fixture()
    with pytest.raises((ValueError, TypeError)):
        freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2),), context=context)


def test_mixed_local_contexts_cannot_become_one_admission_winner():
    incoming, receipt, state = fixture()
    first = freeze_admission_plan(
        incoming, str(uuid4()), ((receipt, 2), (state, 2)), context=b"first"
    )
    other = freeze_admission_plan(
        incoming, first.operation_id, ((receipt, 2), (state, 2)), context=b"second"
    )
    with pytest.raises(MessagingContractError):
        read_admission_plan(
            incoming, replace(first, branches=(first.branches[0], other.branches[1]))
        )


@pytest.mark.parametrize("mutation", ["truncated", "empty", "oversized", "nested", "trailing"])
def test_context_archive_rejects_ambiguous_or_noncanonical_lengths(mutation):
    from qs_ai.infrastructure.workflow_transport.transaction_archive import CONTEXT_MAGIC

    incoming, receipt, _ = fixture()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2),), context=b"first")
    value = read_admission_plan(incoming, plan)[0]
    payload = value.encode()
    prefix = len(CONTEXT_MAGIC)
    if mutation == "truncated":
        payload = payload[: prefix + 2]
    elif mutation == "empty":
        payload = CONTEXT_MAGIC + (0).to_bytes(4, "big") + payload[prefix + 9 :]
    elif mutation == "oversized":
        payload = CONTEXT_MAGIC + (16385).to_bytes(4, "big") + payload[prefix + 4 :]
    elif mutation == "nested":
        payload = CONTEXT_MAGIC + (1).to_bytes(4, "big") + b"x" + payload
    else:
        payload += b"trailing"
    with pytest.raises(MessagingContractError):
        decode_archive(payload, value.wire)


def test_multibyte_view_cannot_bypass_local_context_byte_limit():
    _, archive = golden()
    view = memoryview(b"x" * 16388).cast("I")
    assert len(view) < 16384 < view.nbytes
    with pytest.raises(MessagingContractError, match="bounds"):
        replace(archive, context=view)


def test_multibyte_view_cannot_bypass_original_wire_byte_limit():
    incoming, _, _ = fixture()
    view = memoryview(b"x" * 262148).cast("I")
    assert len(view) < 262144 < view.nbytes
    with pytest.raises(MessagingContractError, match="bounds"):
        replace(incoming, wire=view)
