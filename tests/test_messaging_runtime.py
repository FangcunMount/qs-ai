"""Owned lifecycle checks with fake transport; no Broker or model network calls."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from jwcrypto import jwk
from pydantic import ValidationError

from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime, read_key
from qs_ai.config import Settings


def test_messaging_disabled_by_default_and_enabled_requires_explicit_secrets():
    assert not Settings().messaging.enabled
    with pytest.raises(ValidationError):
        Settings(messaging={"enabled": True})


def test_keys_require_private_public_roles_id_and_bounded_file(tmp_path):
    key = jwk.JWK.generate(kty="EC", crv="P-256", kid="test.key")
    path = tmp_path / "key.json"
    path.write_text(key.export_private())
    assert read_key(str(path), private=True, expected_id="test.key").has_private
    for private, kid in [(False, "test.key"), (True, "wrong")]:
        with pytest.raises(ValueError, match="Invalid configured messaging key"):
            read_key(str(path), private=private, expected_id=kid)
    path.write_text(key.export_public())
    assert not read_key(str(path), private=False).has_private
    path.write_text("x" * 65537)
    with pytest.raises(ValueError, match="Invalid configured messaging key"):
        read_key(str(path), private=True)


class Transport:
    def __init__(self, name, events, fail=False):
        self.name, self.events, self.fail = name, events, fail
        self.closed = asyncio.Event()

    async def start(self):
        self.events.append(self.name + ":start")
        if self.fail:
            raise RuntimeError("startup failure")

    async def wait(self):
        await self.closed.wait()

    async def stop(self, **kwargs):
        self.events.append(self.name + ":stop")
        self.closed.set()


@pytest.mark.parametrize("fail", [False, True])
async def test_publisher_starts_before_subscribers_and_stops_after_their_drain(fail):
    events = []
    runtime = MessagingRuntime()
    runtime.publishers = {"a": Transport("pub", events, fail=fail)}
    runtime.subscribers = [Transport("commands", events), Transport("acks", events)]
    stop, state = asyncio.Event(), RuntimeState()
    task = asyncio.create_task(supervise(runtime.components(state, 0.1), state, stop))
    if fail:
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(task, 2)
        assert not state.ready and state.failed
    else:
        async with asyncio.timeout(2):
            while not state.ready:  # noqa: ASYNC110 - observe supervisor readiness
                await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 2)
        assert events[0] == "pub:start"
        assert events.index("pub:stop") > events.index("commands:stop")
        assert events.index("pub:stop") > events.index("acks:stop")
        assert not state.ready
    await runtime.close()


async def test_partial_assembly_failure_closes_owned_clients():
    runtime = MessagingRuntime()
    runtime.publishers = {"a": AsyncMock()}
    runtime.subscribers = [AsyncMock()]
    runtime.http = type("HTTP", (), {"close": lambda self: None})()
    await runtime.close()
    runtime.subscribers[0].stop.assert_awaited_once_with(grace_seconds=0)
    runtime.publishers["a"].stop.assert_awaited_once_with(grace_seconds=0)


async def test_relay_cannot_publish_or_handoff_before_publishers_ready():
    runtime = MessagingRuntime()
    runtime.tx = AsyncMock()
    runtime.relay = AsyncMock()
    assert await runtime.step() == 0
    runtime.tx.open.assert_not_called()
    runtime.relay.step.assert_not_awaited()
