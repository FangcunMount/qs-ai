import asyncio
import json
import os
import time

from qs_ai.bootstrap.daemon_health import healthy, heartbeat


async def test_heartbeat_is_live_and_removed_at_shutdown(tmp_path):
    path = tmp_path / "health.json"
    stop = asyncio.Event()
    task = asyncio.create_task(heartbeat(path, stop, 0.01))
    try:
        for _ in range(100):
            if path.exists():
                break
            await asyncio.sleep(0.001)
        assert healthy(path)
    finally:
        stop.set()
        await task
    assert not healthy(path)


def test_missing_corrupt_stale_or_future_health_is_not_live(tmp_path):
    path = tmp_path / "health.json"
    assert not healthy(path)
    for value in (
        "not json",
        "{}",
        json.dumps({"pid": os.getpid(), "heartbeat": time.monotonic() - 60}),
        json.dumps({"pid": os.getpid(), "heartbeat": time.monotonic() + 60}),
        json.dumps({"pid": -1, "heartbeat": time.monotonic()}),
    ):
        path.write_text(value)
        assert not healthy(path)


async def test_real_process_sigterm_stops_loop_and_removes_health(tmp_path):
    import sys

    path = tmp_path / "process.json"
    code = """
import asyncio, sys
from qs_ai.bootstrap.daemon import serve_loop
async def attempt():
    await asyncio.sleep(0.01)
    return False
asyncio.run(serve_loop(attempt, concurrency=1, idle_seconds=0.01,
    max_backoff_seconds=0.1, shutdown_seconds=0.1, health_file=sys.argv[1]))
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        code,
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        for _ in range(200):
            if healthy(path):
                break
            await asyncio.sleep(0.01)
        assert healthy(path)
        process.terminate()
        _, error = await asyncio.wait_for(process.communicate(), 5)
        assert process.returncode == 0, error.decode()
        assert not path.exists()
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_cancellation_during_cleanup_still_removes_marker(tmp_path, monkeypatch):
    import threading
    from pathlib import Path

    path = tmp_path / "health.json"
    stop = asyncio.Event()
    entered = threading.Event()
    release = threading.Event()
    original = Path.unlink

    def unlink(target, *args, **kwargs):
        if target == path.with_suffix(".tmp"):
            entered.set()
            release.wait(5)
        return original(target, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    pulse = asyncio.create_task(heartbeat(path, stop, 0.01))
    try:
        for _ in range(200):
            if healthy(path):
                break
            await asyncio.sleep(0.01)
        assert healthy(path)
        stop.set()
        assert await asyncio.to_thread(entered.wait, 5)
        pulse.cancel()
        await asyncio.sleep(0)
        assert not pulse.done()
    finally:
        release.set()
        await asyncio.gather(pulse, return_exceptions=True)
    assert not path.exists()
