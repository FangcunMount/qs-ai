"""Actual pinned SDK wait/Stop with a controlled driver; no Broker/model calls."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.transport import RouteBinding, consumer_group, physical_topic

from qs_ai.bootstrap.messaging import _SubscriberLifetime


def owned_subscriber(monkeypatch, shutdown):
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

    async def start():
        subscriber._driver.begin()
        subscriber._driver.client = SimpleNamespace(shutdown=shutdown)
        subscriber._failure = asyncio.get_running_loop().create_future()

    monkeypatch.setattr(subscriber, "start", start)  # Only network startup is replaced.
    ready = asyncio.Event()
    ready.set()
    return subscriber, _SubscriberLifetime(subscriber, ready, 2)


@pytest.mark.parametrize("notification", ["host_stop", "unexpected", "transport_failure"])
async def test_real_sdk_normal_stop_is_joined_but_unsolicited_or_caused_exit_still_fails(
    monkeypatch, notification
):
    subscriber, lifetime = owned_subscriber(monkeypatch, lambda: None)
    task = asyncio.create_task(lifetime.run())
    try:
        await asyncio.wait_for(lifetime.started.wait(), 2)
        if notification == "host_stop":
            await lifetime.stop()
            await asyncio.wait_for(task, 2)
            assert lifetime.stop_result.result() is True
        else:
            if notification == "transport_failure":
                cause = ValueError("controlled transport failure")
                subscriber._fail(cause)
            else:
                subscriber._failure.set_result(None)
            with pytest.raises(RuntimeError) as result:
                await asyncio.wait_for(task, 2)
            if notification == "transport_failure":
                assert result.value.__cause__ is cause
        await subscriber.stop(grace_seconds=2)
        assert subscriber._driver.client is subscriber._driver.executor is None
        assert not subscriber._driver.pending
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await subscriber.stop(grace_seconds=2)


async def test_caused_transport_failure_during_host_stop_is_not_normalized(monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def shutdown():
        entered.set()
        assert release.wait(3)

    subscriber, lifetime = owned_subscriber(monkeypatch, shutdown)
    task = asyncio.create_task(lifetime.run())
    stopping = None
    try:
        await asyncio.wait_for(lifetime.started.wait(), 2)
        stopping = asyncio.create_task(lifetime.stop())
        assert await asyncio.to_thread(entered.wait, 2)
        cause = ValueError("controlled failure while stopping")
        subscriber._fail(cause)
        with pytest.raises(RuntimeError) as result:
            await asyncio.wait_for(task, 1)
        assert result.value.__cause__ is cause
        assert not stopping.done() and not lifetime.stop_result.done()
        release.set()
        await asyncio.wait_for(stopping, 2)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, *([stopping] if stopping else []), return_exceptions=True)
        await subscriber.stop(grace_seconds=2)


async def test_failed_stop_never_proves_success_even_if_sdk_notifies_wait(monkeypatch):
    subscriber, lifetime = owned_subscriber(monkeypatch, lambda: None)
    task = asyncio.create_task(lifetime.run())
    try:
        await asyncio.wait_for(lifetime.started.wait(), 2)
        real_stop = subscriber.stop

        async def failed_stop(**kwargs):
            subscriber._failure.set_result(None)
            raise TimeoutError("controlled stop failure")

        monkeypatch.setattr(subscriber, "stop", failed_stop)
        with pytest.raises(TimeoutError):
            await lifetime.stop()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(task, 2)
        assert lifetime.stop_result.result() is False
        await real_stop(grace_seconds=2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await subscriber._driver.close(2)
