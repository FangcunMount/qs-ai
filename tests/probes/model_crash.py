"""Disposable-MySQL subprocess probe. Model responses are synthetic; no network provider."""

import asyncio
import os
import sys

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from tests.integration.test_generation import Gateway, request


class PausingGateway(Gateway):
    async def generate(self, *args):
        if sys.argv[2] == "dispatched":
            print("dispatched", flush=True)
            await asyncio.Event().wait()
        return await super().generate(*args)


async def main():
    dsn = os.environ["QS_AI_TEST_MYSQL_DSN"]
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    try:
        store = MySQLExecutionStore(Transactions(database))
        claim = await store.claim(60)
        assert claim is not None and claim.session.id == sys.argv[1]
        await DurableGeneration(store, PausingGateway(), JSONModelCallCodec()).execute(
            claim, request()
        )
        print("response_received", flush=True)
        await asyncio.Event().wait()
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
