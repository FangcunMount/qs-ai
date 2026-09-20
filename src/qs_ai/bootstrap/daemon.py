"""Bounded process loops. Each attempt owns its own dependency/request scope."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from qs_ai.infrastructure.observability.structured import emit


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

    parent = asyncio.current_task()
    component_name = parent.get_name() if parent else "background"

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
            except Exception as error:
                failures = min(failures + 1, 16)
                # Database/provider exceptions may embed sensitive data.
                emit(
                    "attempt_failed",
                    component_name,
                    level=logging.WARNING,
                    error_type=type(error).__name__,
                    failures=failures,
                )
                await wait(min(max_backoff_seconds, idle_seconds * 2**failures))
            else:
                failures = 0
                if not processed:
                    await wait(idle_seconds)

    tasks = [
        asyncio.create_task(
            consume(),
            name=component_name,
        )
        for _ in range(concurrency)
    ]
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
