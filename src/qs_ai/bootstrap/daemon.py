"""Bounded process loops. Each attempt owns its own dependency/request scope."""

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


async def run_loop(
    attempt: Callable[[], Awaitable[int | bool]],
    stop: asyncio.Event,
    *,
    concurrency: int,
    idle_seconds: float,
    max_backoff_seconds: float,
    shutdown_seconds: float,
) -> None:
    if (
        not 1 <= concurrency <= 32
        or idle_seconds <= 0
        or max_backoff_seconds < idle_seconds
        or shutdown_seconds < 0
    ):
        raise ValueError("Invalid daemon limits")

    async def wait(delay: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), delay)
        except TimeoutError:
            pass

    async def consume() -> None:
        failures = 0
        while not stop.is_set():
            try:
                processed = await attempt()
            except Exception:
                failures = min(failures + 1, 16)
                # Database/provider exceptions may embed sensitive data.
                logger.warning("Background attempt failed; backing off")
                await wait(min(max_backoff_seconds, idle_seconds * 2**failures))
            else:
                failures = 0
                if not processed:
                    await wait(idle_seconds)

    tasks = [asyncio.create_task(consume()) for _ in range(concurrency)]
    stopping = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait([stopping, *tasks], return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            for task in done:
                task.result()
            raise RuntimeError("Background consumer exited unexpectedly")
        # Stop claiming first; keep leases alive while existing attempts drain.
        await asyncio.wait(tasks, timeout=shutdown_seconds)
    finally:
        for task in [stopping, *tasks]:
            if not task.done():
                task.cancel()
        await asyncio.gather(stopping, *tasks, return_exceptions=True)


async def serve_loop(
    attempt: Callable[[], Awaitable[int | bool]],
    *,
    concurrency: int,
    idle_seconds: float,
    max_backoff_seconds: float,
    shutdown_seconds: float,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    try:
        await run_loop(
            attempt,
            stop,
            concurrency=concurrency,
            idle_seconds=idle_seconds,
            max_backoff_seconds=max_backoff_seconds,
            shutdown_seconds=shutdown_seconds,
        )
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)
