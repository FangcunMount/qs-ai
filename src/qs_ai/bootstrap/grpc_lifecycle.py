"""Drain the gRPC server on container termination before closing its dependencies."""

import asyncio
import signal

from grpc import aio


async def serve_grpc(server: aio.Server, grace_seconds: float) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    tasks: list[asyncio.Task[object]] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    try:
        await server.start()
        tasks = [
            asyncio.create_task(stop.wait()),
            asyncio.create_task(server.wait_for_termination()),
        ]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        try:
            # stop() closes admission immediately and bounds the in-flight drain.
            await server.stop(grace_seconds)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
