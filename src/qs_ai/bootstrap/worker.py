"""Worker entry: probe by default, one attempt with --once, daemon with --serve."""

import argparse
import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import AsyncContainer, Provider

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.operations.health import CheckReadiness
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.daemon import serve_loop
from qs_ai.config import Settings


@asynccontextmanager
async def worker_container(
    settings: Settings,
    *providers: Provider,
) -> AsyncIterator[AsyncContainer]:
    container = create_container(settings, *providers)
    try:
        yield container
    finally:
        await container.close()


async def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Process at most one durable job")
    mode.add_argument("--serve", action="store_true", help="Continuously consume durable jobs")
    arguments = parser.parse_args()
    settings = Settings()
    async with worker_container(settings) as container:
        if arguments.serve:
            if not settings.generation.enabled:
                raise ValueError("Continuous execution requires generation to be enabled")
            # Resolve workflow/TLS configuration before claiming any durable jobs.
            async with container() as preflight:
                await preflight.get(ExecuteNext)
                if not (await (await preflight.get(CheckReadiness)).execute()).ready:
                    return 1

            async def attempt() -> bool:
                async with container() as operation:
                    return await (await operation.get(ExecuteNext)).once(
                        settings.worker.lease_seconds
                    )

            await serve_loop(attempt, **settings.worker.model_dump(exclude={"lease_seconds"}))
            return 0
        async with container() as operation:
            if arguments.once:
                processed = await (await operation.get(ExecuteNext)).once(
                    settings.worker.lease_seconds
                )
                print(json.dumps({"mode": "worker_once", "processed": processed}))
                return 0
            result = await (await operation.get(CheckReadiness)).execute()
            print(json.dumps({"mode": "worker_probe", "database": result.database}))
            return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
