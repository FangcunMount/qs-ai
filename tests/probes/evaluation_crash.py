"""Disposable-DB subprocess probe; never contacts an external model."""

import asyncio
import os
import sys
from uuid import UUID

from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from tests.integration.test_evaluation_recovery import EXPIRY, pending
from tests.integration.test_evaluation_step import step
from tests.test_generation_completion_assets import assets


class PauseGateway:
    async def generate_messages(self, *args):
        print("dispatching", flush=True)
        await asyncio.Event().wait()
        raise AssertionError("Pause must be terminated")


class NoCalls:
    async def generate_messages(self, *args):
        raise AssertionError("Recovery cannot call a model")


async def main():
    run_id, mode, version = UUID(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    dsn = os.environ["QS_AI_TEST_MYSQL_DSN"].replace("mysql://", "mysql+asyncmy://", 1)
    database = Database(dsn)
    tx = Transactions(database)
    _, output, routes, schemas = assets()
    ready = tx, run_id, output, routes, schemas
    try:
        if mode == "recover":
            worker = EvaluationWorker(
                tx,
                NoCalls(),
                routes,
                schemas,
                "worker:replacement",
                enabled=True,
                clock=lambda: EXPIRY,
            )
            assert await worker.once()
            print("recovered", flush=True)
        elif mode == "prepared":
            await pending(ready, version=version)
            print("prepared", flush=True)
            await asyncio.Event().wait()
        else:
            await step(ready, PauseGateway(), version)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
