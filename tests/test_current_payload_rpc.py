"""Current archive RPC error boundaries; real SQL/mTLS is a separate native gate."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import grpc
import pytest
from reliable_messaging.transaction import Binding

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.transaction_archive import (
    ArchivePayloadReader,
    TransactionArchive,
)
from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError
from qs_ai.infrastructure.workflow_transport.payload_access import MySQLPayloadAccess
from qs_ai.transport.grpc.message_payloads import MessagePayloads
from tests.test_grpc_commands import Aborted, Context


@pytest.fixture
def current_payload():
    raw = b"synthetic current committed body"
    reference = pb.MessagePayloadReference(
        producer="qs-ai",
        destination="qs-server",
        message_id=str(uuid4()),
        organization_id="7",
        body_sha256=hashlib.sha256(raw).hexdigest(),
        body_length=len(raw),
    )
    original = SimpleNamespace(
        organization_id=7,
        body=raw,
        envelope=lambda: SimpleNamespace(body_sha256=reference.body_sha256),
    )
    current = TransactionArchive(Mock(), Binding("ai.mysql", "current-rpc", "m3-1"))
    current.read_committed = AsyncMock(return_value=original)
    legacy = Mock(
        payload=AsyncMock(side_effect=AssertionError("Current RPC must not read old data"))
    )
    reader = ArchivePayloadReader(current)
    validator = MySQLPayloadAccess(Mock(), reader)

    async def read(incoming, workload):
        return await reader.payload(Mock(), incoming, workload)

    access = SimpleNamespace(
        valid_reference=validator.valid_reference,
        payload=AsyncMock(side_effect=read),
        record_failure=AsyncMock(return_value=True),
    )
    return SimpleNamespace(**locals(), rpc=MessagePayloads(access))


async def test_current_committed_rpc_returns_first_body_and_reference_without_publication(
    current_payload,
):
    p = current_payload
    result = await p.rpc.Get(p.reference, Context())
    assert result.reference == p.reference and result.body == p.raw
    p.current.read_committed.assert_awaited_once_with(p.reference.message_id)
    p.legacy.payload.assert_not_awaited()
    p.access.record_failure.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("producer", "qs-server"),
        ("destination", "other"),
        ("organization_id", "8"),
        ("body_sha256", "b" * 64),
        ("body_length", 1),
    ],
)
async def test_current_reference_denial_is_not_misreported_as_storage_outage(
    current_payload, field, value
):
    p = current_payload
    request = pb.MessagePayloadReference()
    request.CopyFrom(p.reference)
    setattr(request, field, value)
    with pytest.raises(Aborted) as error:
        await p.rpc.Get(request, Context())
    assert error.value.args[0] == grpc.StatusCode.NOT_FOUND
    p.access.record_failure.assert_awaited_once_with("payload_serve_reference_mismatch")
    p.legacy.payload.assert_not_awaited()


@pytest.mark.parametrize(
    "failure",
    [
        MessagingContractError("archive has no exact committed witness"),
        OSError("private storage DSN"),
        TimeoutError("private query details"),
    ],
)
async def test_authority_unknown_or_storage_failure_stays_unavailable_without_fallback(
    current_payload, failure
):
    p = current_payload
    p.current.read_committed.side_effect = failure
    with pytest.raises(Aborted) as error:
        await p.rpc.Get(p.reference, Context())
    assert error.value.args[0] == grpc.StatusCode.UNAVAILABLE and "private" not in str(error.value)
    p.access.record_failure.assert_awaited_once_with("payload_serve_storage_unavailable")
    p.legacy.payload.assert_not_awaited()


async def test_untrusted_workload_is_rejected_before_current_archive_read(current_payload):
    p = current_payload
    context = Context()
    context.auth_context = lambda: {}
    with pytest.raises(Aborted) as error:
        await p.rpc.Get(p.reference, context)
    assert error.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    p.current.read_committed.assert_not_awaited()
    p.access.payload.assert_not_awaited()
    p.access.record_failure.assert_awaited_once_with("payload_serve_workload_denied")


async def test_internal_workload_binding_error_retains_original_contract(current_payload):
    p = current_payload
    with pytest.raises(MessagingContractError):
        await p.current.payload(p.reference, "wrong")
    p.current.read_committed.assert_not_awaited()
    p.legacy.payload.assert_not_awaited()


async def test_absent_current_archive_is_not_found_without_reading_historical_body(current_payload):
    p = current_payload
    p.current.read_committed.return_value = None
    with pytest.raises(Aborted) as error:
        await p.rpc.Get(p.reference, Context())
    assert error.value.args[0] == grpc.StatusCode.NOT_FOUND
    p.legacy.payload.assert_not_awaited()
    p.access.record_failure.assert_awaited_once_with("payload_serve_reference_mismatch")
