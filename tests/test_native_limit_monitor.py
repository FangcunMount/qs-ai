from unittest.mock import AsyncMock

import pytest

import qs_ai.bootstrap.transaction_observation as observation
from qs_ai.bootstrap.transaction_observation import ProtocolMonitor
from qs_ai.bootstrap.transaction_protection import ProtocolProtectionError, ProtocolSample


async def test_native_notice_latches_without_stopping_sdk_and_requires_resolution_then_review():
    monitor = ProtocolMonitor(
        AsyncMock(return_value=ProtocolSample()),
        lambda: {"ready": True},
        workers=1,
        queue=16,
        native_read=AsyncMock(return_value=True),
    )
    monitor.start()
    try:
        await monitor.wait_initial()
        assert monitor.stats()["ready"] is True
        assert not monitor.allowed() and monitor.policy.reason == "native_transaction_limit"
        assert monitor.policy.sample.native_blocked
        with pytest.raises(ProtocolProtectionError):
            monitor.review(monitor.policy.generation, "owned-review")
    finally:
        await monitor.close(grace_seconds=1)
    assert monitor.drained()


async def test_native_and_protocol_read_share_one_budget(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(observation, "monotonic", lambda: clock[0])

    async def native(budget):
        assert budget == 1.0
        clock[0] = 10.25
        return False

    protocol = AsyncMock(return_value=ProtocolSample())
    monitor = ProtocolMonitor(
        protocol, lambda: {"ready": True}, workers=1, queue=16, budget=1.0, native_read=native
    )
    monitor.start()
    try:
        await monitor.wait_initial()
        protocol.assert_awaited_once_with(0.75)
    finally:
        await monitor.close(grace_seconds=1)


async def test_known_native_notice_survives_timeout_before_protocol_read(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(observation, "monotonic", lambda: clock[0])

    async def native(budget):
        clock[0] = 12.0
        return True

    protocol = AsyncMock(return_value=ProtocolSample())
    monitor = ProtocolMonitor(
        protocol, lambda: {"ready": True}, workers=1, queue=16, budget=1, native_read=native
    )
    monitor.start()
    try:
        await monitor.wait_initial()
        protocol.assert_not_awaited()
        assert monitor.policy.reason == "native_transaction_limit"
        assert monitor.policy.sample.native_blocked and not monitor.policy.available
        assert not monitor.allowed()
    finally:
        await monitor.close(grace_seconds=1)
