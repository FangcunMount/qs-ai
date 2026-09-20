"""Probe by default; explicitly enabled --once execute started evaluations."""

import argparse
import asyncio
import json

from qs_ai.application.operations.health import CheckReadiness
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker


async def run(settings: Settings, *, once: bool = False) -> int:
    if once and not settings.evaluation.enabled:
        raise ValueError("Evaluation execution is disabled")
    container = create_container(settings, EvaluationProvider())
    try:
        async with container() as operation:
            if once:
                # Validate credentials/configuration before selecting any Run.
                await operation.get(EvaluationWorker)
            readiness = await (await operation.get(CheckReadiness)).execute()
            if not readiness.ready:
                return 1
        if not once:
            print(json.dumps({"mode": "evaluation_probe", "database": readiness.database}))
            return 0

        async def attempt() -> bool:
            async with container() as operation:
                return await (await operation.get(EvaluationWorker)).once()

        if once:
            processed = await attempt()
            print(json.dumps({"mode": "evaluation_once", "processed": processed}))
        return 0
    finally:
        await container.close()


async def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Advance at most one evaluation step")
    args = parser.parse_args()
    return await run(Settings(), once=args.once)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
