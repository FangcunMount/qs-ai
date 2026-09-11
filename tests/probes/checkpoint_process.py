"""A real subprocess probe; no model or production task consumer."""

import asyncio
import json
import os
import sys

from langgraph.types import Command

from qs_ai.infrastructure.persistence.mysql.leases import Lease
from qs_ai.infrastructure.workflows.langgraph.checkpoints import guarded_saver
from tests.probes.clarification import build_demo


async def main() -> None:
    mode, thread_id, fence = sys.argv[1:]
    async with guarded_saver(
        os.environ["QS_AI_TEST_MYSQL_DSN"], Lease(thread_id, int(fence))
    ) as saver:
        graph = build_demo(saver)
        config = {"configurable": {"thread_id": thread_id}}
        if mode == "pause":
            result = await graph.ainvoke({"question": "Who answered?"}, config)
            assert result["__interrupt__"]
            print("PAUSED", flush=True)
            await asyncio.Event().wait()
        else:
            result = await graph.ainvoke(Command(resume="Father"), config)
            print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
