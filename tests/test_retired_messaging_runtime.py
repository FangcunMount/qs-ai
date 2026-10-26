"""Final producer selection rejects old modes before host resources or key reads."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.bootstrap import messaging, server
from qs_ai.config import MessagingOptions, Settings
from tests.test_transaction_messaging import options


def selected(tmp_path, mode="transaction", *, paused=False):
    value = MessagingOptions.model_validate(options(tmp_path)).model_copy(
        update={"enabled": True, "publish_enabled": not paused}
    )
    changes = {
        "disabled": {"enabled": False},
        "nsq": {"provider": "nsq", "rocketmq": None, "transactions": {}},
        "empty": {"provider": ""},
        "normal-only": {"transactions": {}},
        "wrong-owner": {"transactions": {"qs.mysql": value.transactions["ai.mysql"]}},
    }
    return Settings().model_copy(
        update={"messaging": value.model_copy(update=changes.get(mode, {}))}
    )


@pytest.mark.parametrize("mode", ["disabled", "nsq", "empty", "normal-only", "wrong-owner"])
async def test_retired_modes_fail_before_tls_database_keys_or_transport(
    tmp_path, monkeypatch, mode
):
    settings = selected(tmp_path, mode)
    forbidden = Mock(side_effect=AssertionError("retired mode reached resource acquisition"))
    monkeypatch.setattr(server, "validate_tls", forbidden)
    monkeypatch.setattr(server, "create_container", forbidden)
    monkeypatch.setattr(messaging, "read_key", forbidden)
    container = Mock()
    container.get = AsyncMock(side_effect=forbidden)
    expected = "MQ runtime is required" if mode == "disabled" else "Final ai.mysql RocketMQ"
    with pytest.raises(ValueError, match=expected):
        await server.serve(settings, asyncio.Event())
    with pytest.raises(ValueError, match=expected):
        await messaging.MessagingRuntime.create(container, settings, b"", b"", b"")
    forbidden.assert_not_called()
    container.get.assert_not_awaited()


@pytest.mark.parametrize("paused", [False, True])
def test_final_profile_allows_idle_checker_without_opening_resources(tmp_path, paused):
    messaging.require_final_messaging(selected(tmp_path, paused=paused))


def test_runtime_has_no_legacy_producer_relay_or_http_transport_selection():
    for name in (
        "NSQPublisher",
        "NSQSubscriber",
        "RocketMQPublisher",
        "MQRelay",
        "FailureTopology",
        "AsyncHTTPClient",
        "StateEventRecorder",
    ):
        assert not hasattr(messaging, name), name
    value = messaging.MessagingRuntime()
    for name in ("publishers", "relay", "step", "http"):
        assert not hasattr(value, name), name
