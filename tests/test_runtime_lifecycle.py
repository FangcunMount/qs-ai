import asyncio

import pytest

from qs_ai.bootstrap.lifecycle import Component, RuntimeState, supervise


async def test_ready_only_after_all_components_start_and_cleared_before_stop():
    stop = asyncio.Event()
    state = RuntimeState()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def run():
        entered.set()
        await release.wait()

    async def halt():
        assert not state.ready
        calls.append("stop")
        release.set()

    component = Component("worker", run, halt, entered.is_set, 1)
    supervisor = asyncio.create_task(supervise([component], state, stop))
    while not state.ready:  # noqa: ASYNC110 - observes supervisor readiness, bounded by test
        await asyncio.sleep(0)
    assert state.healthy
    stop.set()
    await supervisor
    assert calls == ["stop"]
    assert not state.ready


@pytest.mark.parametrize("raises", [False, True])
async def test_unexpected_component_exit_is_fatal_and_drains_peers(raises):
    state = RuntimeState()
    stopped = asyncio.Event()

    async def broken():
        await asyncio.sleep(0)
        if raises:
            raise ValueError("sensitive-payload")

    async def peer():
        await stopped.wait()

    async def halt():
        stopped.set()

    with pytest.raises(RuntimeError):
        await supervise(
            [
                Component("broken", broken, halt, lambda: True, 1),
                Component("peer", peer, halt, lambda: True, 1),
            ],
            state,
            asyncio.Event(),
        )
    assert stopped.is_set()
    assert state.failed and not state.ready
    assert all(t.done() for t in state.tasks.values())


async def test_drain_timeout_cancels_inflight_and_finalizes_resources():
    stop = asyncio.Event()
    finished = asyncio.Event()

    async def busy():
        try:
            stop.set()
            await asyncio.Event().wait()
        finally:
            finished.set()

    async def halt():
        pass

    await supervise([Component("busy", busy, halt, lambda: True, 0.01)], RuntimeState(), stop)
    assert finished.is_set()


async def test_startup_timeout_stops_started_components():
    stopped = asyncio.Event()

    async def run():
        await stopped.wait()

    async def halt():
        stopped.set()

    with pytest.raises(TimeoutError):
        await supervise(
            [Component("slow", run, halt, lambda: False, 1)],
            RuntimeState(),
            asyncio.Event(),
            startup_seconds=0.01,
        )
    assert stopped.is_set()
