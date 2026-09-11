import asyncio

import pytest

from qs_ai.bootstrap.daemon import run_loop


async def run(attempt, stop, **changes):
    options = dict(
        concurrency=1, idle_seconds=0.005, max_backoff_seconds=0.02, shutdown_seconds=0.1
    )
    options.update(changes)
    await asyncio.wait_for(run_loop(attempt, stop, **options), 1)


async def test_idle_wait_is_interruptible():
    stop = asyncio.Event()
    called = asyncio.Event()
    calls = 0

    async def attempt():
        nonlocal calls
        calls += 1
        called.set()
        return False

    task = asyncio.create_task(run(attempt, stop, idle_seconds=30, max_backoff_seconds=30))
    await called.wait()
    stop.set()
    await task
    assert calls == 1


async def test_failure_backs_off_and_recovers_without_logging_exception(caplog):
    stop = asyncio.Event()
    calls = 0

    async def attempt():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("synthetic-sensitive-data")
        stop.set()
        return True

    await run(attempt, stop)
    assert calls == 2
    assert "synthetic-sensitive-data" not in caplog.text


async def test_shutdown_drains_only_existing_attempts():
    stop = asyncio.Event()
    entered = 0
    completed = 0

    async def attempt():
        nonlocal entered, completed
        entered += 1
        if entered == 3:
            stop.set()
        await asyncio.sleep(0.01)
        completed += 1
        return True

    await run(attempt, stop, concurrency=3)
    assert entered == completed == 3


async def test_shutdown_cancels_hung_attempt_and_runs_cleanup():
    stop = asyncio.Event()
    cleaned = asyncio.Event()

    async def attempt():
        stop.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    await run(attempt, stop, shutdown_seconds=0.01)
    assert cleaned.is_set()


async def test_unexpected_consumer_cancellation_does_not_leave_daemon_alive():
    async def attempt():
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run(attempt, asyncio.Event())
