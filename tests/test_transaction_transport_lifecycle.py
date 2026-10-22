"""Host lifecycle ordering with controlled barriers, not real Broker fault proof."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.transaction import ShutdownIncompleteError
from reliable_messaging.transport import RouteBinding, consumer_group, physical_topic

from qs_ai.bootstrap.lifecycle import Component, RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime
from tests.test_messaging_runtime import Transport


class Transactions:
    def __init__(self, events):
        self.events = events
        self.starting, self.started = asyncio.Event(), asyncio.Event()
        self.closed, self.release = asyncio.Event(), asyncio.Event()
        self.admitted = True
        self.incomplete = False

    async def start(self):
        self.events.append("sdk:starting")
        self.starting.set()
        await self.started.wait()
        self.events.append("sdk:started")

    async def close(self, **kwargs):
        self.events.append("sdk:close")
        self.admitted = False
        if self.incomplete:
            raise ShutdownIncompleteError("real call remains", {"pending_calls": 1})
        self.closed.set()

    async def wait(self):
        self.events.append("sdk:wait")
        await self.release.wait()
        self.closed.set()
        self.events.append("sdk:finished")


def runtime():
    events = []
    value = MessagingRuntime()
    sdk = Transactions(events)
    value.transaction_services = SimpleNamespace(runtime=sdk)
    value.subscribers = [Transport("commands", events), Transport("acks", events)]
    value.database = SimpleNamespace(state_events=object())
    value.resources.push_async_callback(resource_closed, events)
    return value, sdk, events


async def resource_closed(events):
    events.append("payloads:closed")


async def wait_ready(state, task):
    async with asyncio.timeout(2):
        while not state.ready:  # noqa: ASYNC110 - observe actual supervisor state
            if task.done():
                await task
            await asyncio.sleep(0.001)


async def test_checker_start_precedes_subscriptions_and_accepted_business_drain_precedes_close():
    value, sdk, events = runtime()
    stop, state = asyncio.Event(), RuntimeState()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def business():
        await value.publisher_ready.wait()
        entered.set()
        await finish.wait()
        assert sdk.admitted  # Accepted work may still send its terminal event.
        events.append("business:finished")

    async def halt_business():
        events.append("business:sealed")

    components = [Component("evaluation", business, halt_business, entered.is_set, 1)]
    components.extend(value.components(state, 1))
    task = asyncio.create_task(supervise(components, state, stop))
    try:
        await sdk.starting.wait()
        assert not entered.is_set() and events == ["sdk:starting"]
        sdk.started.set()
        await wait_ready(state, task)
        stop.set()
        await asyncio.sleep(0.01)
        assert "business:sealed" in events and "sdk:close" not in events
        assert sdk.admitted and not task.done()
        finish.set()
        await asyncio.wait_for(task, 2)
        assert events.index("sdk:started") < events.index("commands:start")
        assert events.index("sdk:close") > events.index("business:finished")
        assert events.index("sdk:close") > events.index("commands:stop")
        assert events.index("sdk:close") > events.index("acks:stop")
    finally:
        finish.set()
        sdk.started.set()
        stop.set()
        await asyncio.gather(task, return_exceptions=True)
        await value.close()
    assert events[-1] == "payloads:closed"
    assert value.database.state_events is None


@pytest.mark.parametrize("cancel", [False, True])
async def test_incomplete_shutdown_and_cancellation_keep_pool_context_until_real_sdk_wait(cancel):
    value, sdk, events = runtime()
    sdk.incomplete = True
    context = value.database.state_events
    close = asyncio.create_task(value.close())
    try:
        async with asyncio.timeout(2):
            while "sdk:wait" not in events:  # noqa: ASYNC110 - controlled close barrier
                await asyncio.sleep(0.001)
        assert not close.done() and value.database.state_events is context
        assert "payloads:closed" not in events and not sdk.closed.is_set()
        if cancel:
            close.cancel()
            await asyncio.sleep(0)
            assert not close.done() and value.database.state_events is context
            close.cancel()
            await asyncio.sleep(0)
            assert not close.done() and value.database.state_events is context
        sdk.release.set()
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await close
        else:
            await close
        assert events.index("sdk:finished") < events.index("payloads:closed")
        assert value.database.state_events is None
        before = list(events)
        await value.close()
        assert events == before
    finally:
        sdk.release.set()
        await asyncio.gather(close, return_exceptions=True)


async def test_paused_producer_starts_checker_without_a_legacy_relay_or_normal_publisher():
    value, sdk, events = runtime()
    value.publish_enabled = False
    sdk.admitted = False
    sdk.started.set()
    stop, state = asyncio.Event(), RuntimeState()
    task = asyncio.create_task(supervise(value.components(state, 0.1), state, stop))
    try:
        await wait_ready(state, task)
        assert value.publisher_ready.is_set() and not sdk.admitted
        assert not value.publishers and value.relay is None
        assert await value.step() == 0
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)
        await value.close()


async def test_real_subscriber_driver_partial_start_retains_calls_before_owner_close(monkeypatch):
    """Real fixed SDK executor/Stop; controlled blocked shutdown, no network fault."""
    value, sdk, events = runtime()
    entered, release = threading.Event(), threading.Event()
    first_timeout = asyncio.Event()

    class Client:
        def shutdown(self):
            entered.set()
            assert release.wait(3), "Owned shutdown fixture was not released"

    subscriber = RocketMQSubscriber(
        Config(
            "127.0.0.1:1",
            "fixture",
            "fixture",
            (RouteBinding("t", "s", physical_topic("t"), consumer_group("t", "s")),),
        ),
        "t",
        "s",
        handler=AsyncMock(),
        failed_handler=AsyncMock(),
        invalid_handler=AsyncMock(),
    )
    subscriber._driver.begin()
    subscriber._driver.client = Client()
    value.subscribers = [subscriber]
    stop = subscriber.stop
    calls = 0

    async def controlled_stop(*, grace_seconds):
        nonlocal calls
        calls += 1
        try:
            await stop(grace_seconds=grace_seconds)
        except asyncio.CancelledError:
            first_timeout.set()
            raise

    monkeypatch.setattr(subscriber, "stop", controlled_stop)
    value._subscriber_shutdown_seconds = 0.01
    close = asyncio.create_task(value.close())
    context = value.database.state_events
    try:
        await asyncio.wait_for(first_timeout.wait(), 2)
        assert entered.is_set() and len(subscriber._driver.pending) == 1
        assert not close.done() and "sdk:close" not in events
        assert value.database.state_events is context
        release.set()
        await asyncio.wait_for(close, 2)
        assert calls >= 2 and not subscriber._driver.pending
        assert subscriber._driver.client is subscriber._driver.executor is None
        assert "sdk:close" in events and events[-1] == "payloads:closed"
        assert value.database.state_events is None
    finally:
        release.set()
        await asyncio.gather(close, return_exceptions=True)
        await stop(grace_seconds=5)


async def test_completed_subscriber_rpc_timeout_is_preserved_after_other_resources_drain():
    value, sdk, events = runtime()
    subscriber = value.subscribers[0]
    subscriber.stop = AsyncMock(side_effect=TimeoutError("controlled completed RPC failure"))
    with pytest.raises(TimeoutError):
        await value.close()
    subscriber.stop.assert_awaited_once()
    assert "acks:stop" in events and "sdk:close" in events
    assert value.database.state_events is None and events[-1] == "payloads:closed"
