import asyncio
import os
import sys

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from tests.probes.p1_runtime import OfflineWorkflow, SyntheticEvidence


class CrashWindow(OfflineWorkflow):
    async def execute(self, claim, evidence):
        await super().execute(claim, evidence)
        print("CHECKPOINT_SAVED", flush=True)
        await asyncio.Event().wait()


async def main():
    dsn = os.environ["QS_AI_TEST_MYSQL_DSN"]
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    store = MySQLExecutionStore(Transactions(database))
    workflow = CrashWindow(dsn) if sys.argv[1] == "crash-window" else OfflineWorkflow(dsn)
    try:
        await ExecuteNext(store, SyntheticEvidence(), workflow).once()
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
