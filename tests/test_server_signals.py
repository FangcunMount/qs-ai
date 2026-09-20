"""Real subprocess signals must drain RPCs rather than requiring Docker SIGKILL."""

import asyncio
import os
import signal
import sys
import time
from pathlib import Path

import grpc
import pytest

SCRIPT = r"""
import asyncio, sys
from pathlib import Path
import grpc
from qs_ai.bootstrap import server as entry
from qs_ai.bootstrap.lifecycle import Component, RuntimeState, supervise
root=Path(sys.argv[1])
async def main(settings, stop):
 server=grpc.aio.server()
 async def call(request, context):
  (root/'started').touch()
  try:
   while not (root/'release').exists(): await asyncio.sleep(.01)
   return b'completed'
  finally: (root/'handler_closed').touch()
 server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('Test', {
  'Call': grpc.unary_unary_rpc_method_handler(call)}),))
 port=server.add_insecure_port('127.0.0.1:0')
 async def ready():
  async with grpc.aio.insecure_channel('127.0.0.1:'+str(port)) as channel:
   await channel.channel_ready()
   (root/'port').write_text(str(port))
 task=asyncio.create_task(ready())
 async def serve():
  await server.start()
  await server.wait_for_termination()
 async def halt(): await server.stop(.5)
 try: await supervise([Component('grpc', serve, halt, lambda: True, 1)], RuntimeState(), stop)
 finally:
  await task
  (root/'closed').touch()
entry.serve = main
entry.main()
"""


def wait_file_sync(path: Path) -> None:
    deadline = time.monotonic() + 5
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Child process did not create {path.name}")
        time.sleep(0.01)


async def wait_file(path: Path) -> None:
    await asyncio.to_thread(wait_file_sync, path)


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("release", [True, False])
async def test_signal_drains_or_cancels_inflight_and_exits_cleanly(tmp_path, signum, release):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        SCRIPT,
        str(tmp_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await wait_file(tmp_path / "port")
        address = "127.0.0.1:" + (tmp_path / "port").read_text()
        async with grpc.aio.insecure_channel(address) as channel:
            call = channel.unary_unary("/Test/Call")(b"test", timeout=5)
            await wait_file(tmp_path / "started")
            os.kill(process.pid, signum)
            if release:
                await asyncio.sleep(0.1)
                (tmp_path / "release").touch()
                assert await call == b"completed"
            else:
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await call
                assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
        assert process.returncode == 0, stderr.decode()
        assert (tmp_path / "handler_closed").exists()
        assert (tmp_path / "closed").exists()
    finally:
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.wait(), timeout=5)
