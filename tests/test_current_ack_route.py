"""Current ACK routing and retired command rejection precede borrowed resources."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError
from reliable_messaging.transport import Received, physical_topic_v2
from reliable_messaging.wire import Envelope

from qs_ai.infrastructure.workflow_transport.messaging import ACKS
from qs_ai.infrastructure.workflow_transport.mq_receiver import CommandReceiver
from tests.route_fixtures import legacy_routes


def boundary():
    value = CommandReceiver(
        object(),
        object(),
        object(),
        object(),
        routes=legacy_routes(),
        decrypt_keys={},
        trusted_signers={},
        signing_key=object(),
        qs_recipient_key=object(),
    )
    value.isolate = AsyncMock()
    return value


@pytest.mark.parametrize("damage", ["provider", "logical", "physical", "valid-route-bad-signature"])
async def test_ack_route_refuses_retired_inputs_before_database_and_authenticates_valid_route(
    damage,
):
    value = boundary()
    received = Received(
        Envelope(str(uuid4()), b"opaque"),
        b"not-authenticated",
        "fixture",
        "127.0.0.1",
        1,
        0,
        provider="nsq" if damage == "provider" else "rocketmq",
        logical_topic="other" if damage == "logical" else ACKS,
        physical_topic="other"
        if damage == "physical"
        else physical_topic_v2(ACKS, "TRANSACTION", "qs.mysql", "m3-1"),
    )
    await value.receive_ack(received)
    expected = (
        "identity_conflict"
        if damage == "valid-route-bad-signature"
        else "transaction_route_mismatch"
    )
    value.isolate.assert_awaited_once_with(received.wire, expected)


@pytest.mark.parametrize("method", ["receive_command", "failed_command"])
async def test_retired_direct_admission_cannot_open_old_business_or_receipt(method):
    value = boundary()
    value.resolve_command = AsyncMock(side_effect=AssertionError("Retired route read input"))
    with pytest.raises(BindingError):
        await getattr(value, method)(object())
    value.resolve_command.assert_not_called()
    value.isolate.assert_not_called()
