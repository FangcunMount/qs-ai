"""Event-loop liveness only; database readiness and business delivery are separate."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path


def healthy(path: Path, max_age: float = 15) -> bool:
    try:
        record = json.loads(path.read_text())
        age = time.monotonic() - record["heartbeat"]
        if not 0 <= age <= max_age or type(record["pid"]) is not int or record["pid"] <= 0:
            return False
        os.kill(record["pid"], 0)
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


async def heartbeat(path: Path, stop: asyncio.Event, interval: float = 2) -> None:
    temporary = path.with_suffix(".tmp")

    def write() -> None:
        temporary.write_text(json.dumps({"pid": os.getpid(), "heartbeat": time.monotonic()}))
        temporary.replace(path)

    def clear() -> None:
        temporary.unlink(missing_ok=True)
        path.unlink(missing_ok=True)

    try:
        while not stop.is_set():
            writing = asyncio.create_task(asyncio.to_thread(write))
            try:
                await asyncio.shield(writing)
            except asyncio.CancelledError:
                # Finish an already scheduled write before removing the marker.
                await writing
                raise
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass
    finally:
        clearing = asyncio.create_task(asyncio.to_thread(clear))
        try:
            await asyncio.shield(clearing)
        except asyncio.CancelledError:
            # serve_loop may cancel the pulse after stop already entered cleanup.
            await clearing
            raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    return 0 if healthy(args.path) else 1


if __name__ == "__main__":
    raise SystemExit(main())
