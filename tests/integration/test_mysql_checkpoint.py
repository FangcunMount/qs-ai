import os
from uuid import uuid4

import pytest
from langgraph.checkpoint.mysql.asyncmy import AsyncMySaver
from langgraph.types import Command

from tests.probes.clarification import build_demo


@pytest.mark.integration
async def test_mysql_checkpoint_survives_new_connection() -> None:
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Set QS_AI_TEST_MYSQL_DSN to a disposable local MySQL database")
    thread_id = f"qs-ai-test-{uuid4()}"
    config = {"configurable": {"thread_id": thread_id}}
    async with AsyncMySaver.from_conn_string(dsn) as saver:
        await saver.setup()
        paused = await build_demo(saver).ainvoke({"question": "Who answered?"}, config)
        assert paused["__interrupt__"]
    # No reuse of the original connection or in-memory checkpointer.
    async with AsyncMySaver.from_conn_string(dsn) as saver:
        resumed = await build_demo(saver).ainvoke(Command(resume="Father"), config)
        assert resumed["answer"] == "Father"
        assert resumed["status"] == "answered"
        await saver.adelete_thread(thread_id)
