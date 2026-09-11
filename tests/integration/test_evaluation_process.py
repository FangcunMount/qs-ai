import asyncio
import os
import sys

import pytest

from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


async def spawn(run_id, mode, version):
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.probes.evaluation_crash",
        str(run_id),
        mode,
        str(version),
        env=os.environ.copy(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )


async def stop(child):
    if child.returncode is None:
        child.kill()
    await asyncio.wait_for(child.wait(), 10)


@pytest.mark.parametrize("boundary", ["prepared", "generation_dispatch", "semantic_dispatch"])
async def test_killed_process_recovers_in_new_process_without_send(ready, boundary):
    tx, run_id, *_ = ready
    version = 3
    if boundary == "semantic_dispatch":
        version = (await step(ready, Gateway(ready))).version
    child = await spawn(run_id, boundary, version)
    try:
        marker = "prepared" if boundary == "prepared" else "dispatching"
        assert await asyncio.wait_for(child.stdout.readline(), 20) == f"{marker}\n".encode()
        persisted = (await rows(tx, run_id))[2]
        assert persisted["checkpoint_json"]["phase"] == marker
        child.kill()
        assert await asyncio.wait_for(child.wait(), 10) != 0
    finally:
        await stop(child)

    replacement = await spawn(run_id, "recover", version)
    try:
        assert await asyncio.wait_for(replacement.stdout.readline(), 20) == b"recovered\n"
        assert await asyncio.wait_for(replacement.wait(), 10) == 0
    finally:
        await stop(replacement)
    run, _, checkpoint = await rows(tx, run_id)
    assert checkpoint["checkpoint_json"] is None
    assert checkpoint["version"] == persisted["version"] + 1
    assert len(run["progress_json"]["recoveries"]) == 1
    expected = "collecting" if boundary == "prepared" else "blocked"
    assert run["progress_json"]["status"] == expected
