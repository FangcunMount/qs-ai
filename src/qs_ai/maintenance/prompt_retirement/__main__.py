"""Explicit AI-only Prompt retirement, using the pinned application's native dependencies."""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from qs_ai.maintenance.prompt_retirement import service
from qs_ai.maintenance.schema_refactor import control
from qs_ai.maintenance.schema_refactor.__main__ import (
    _bounded_cleanup,
    _invalidate_owned,
    bounded_operation,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--url-env", default="QS_AI_DATABASE_URL")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("plan", "backup", "verify", "restore", "apply"):
        command = commands.add_parser(name)
        command.add_argument("--schema", default="ai")
        command.add_argument("--revision", required=True)
        command.add_argument("--image-id", required=True)
        command.add_argument("--plan", required=True)
        if name in ("backup", "restore", "apply"):
            command.add_argument("--backup", required=True)
        if name in ("backup", "apply"):
            command.add_argument("--receipt", required=True)
        if name in ("backup", "restore"):
            command.add_argument("--target", required=True)
        if name == "verify":
            command.add_argument("--backup")
            command.add_argument("--receipt")
            command.add_argument("--journal")
        if name == "apply":
            command.add_argument("--journal", required=True)
            command.add_argument("--writers-stopped", action="store_true")
            command.add_argument("--stopped-at", required=True)
    return result


def paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        name: Path(value).absolute()
        for name in ("plan", "backup", "receipt", "journal")
        if (value := getattr(args, name, None)) is not None
    }


def execute(conn: Any, args: argparse.Namespace, files: dict[str, Path]) -> dict[str, Any]:
    common = {"revision": args.revision, "image_id": args.image_id}
    if args.command == "plan":
        return service.plan(conn, args.schema, files["plan"], **common)
    if args.command == "backup":
        return service.backup(
            conn,
            args.schema,
            files["plan"],
            files["backup"],
            files["receipt"],
            args.target,
            **common,
        )
    if args.command == "restore":
        return service.restore(
            conn, args.schema, files["plan"], files["backup"], args.target, **common
        )
    if args.command == "verify":
        return service.verify(
            conn,
            args.schema,
            files["plan"],
            **common,
            backup_path=files.get("backup"),
            receipt_path=files.get("receipt"),
            journal_path=files.get("journal"),
        )
    return service.apply(
        conn,
        args.schema,
        files["plan"],
        files["backup"],
        files["receipt"],
        files["journal"],
        **common,
        writers_stopped=args.writers_stopped,
        stopped_at=args.stopped_at,
    )


async def run(args: argparse.Namespace) -> dict[str, Any]:
    url = os.environ.get(args.url_env, "")
    if not url.startswith("mysql+asyncmy://"):
        raise ValueError("A MySQL asyncmy URL is required")
    files = await asyncio.to_thread(paths, args)
    # Apply retains its original clock. An existing attempt only gets a bounded read-only
    # classification, even when that clock is expired; it never retries source DML.
    deadline = time.monotonic() + 20 * 60
    if args.command == "apply":
        deadline = (
            time.monotonic() + 60
            if files["journal"].exists()
            else control.deadline(args.stopped_at)
        )
    engine = create_async_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    connection: AsyncConnection | None = None
    watchdog: AsyncConnection | None = None
    locked = False
    output: dict[str, Any] | None = None
    try:
        try:
            async with asyncio.timeout(3):
                connection = await engine.connect()
                locked = (
                    await connection.scalar(sa.text("SELECT GET_LOCK('qs-ai-schema-cutover',0)"))
                    == 1
                )
                watchdog = await engine.connect()
                await connection.rollback()
            if not locked:
                raise ValueError("Another AI maintenance operation holds the database lock")

            async def invoke() -> dict[str, Any]:
                # The watchdog identity probes autobegin; the service takes ownership
                # of its own idle connection transaction, so settle those reads first.
                await connection.rollback()  # type: ignore[union-attr]
                return await connection.run_sync(  # type: ignore[union-attr]
                    lambda c: execute(c, args, files)
                )

            output = await bounded_operation(
                connection,
                watchdog,
                invoke,
                deadline=deadline,
            )
        finally:
            failed = sys.exc_info()[0] is not None
            cleaned = True
            if connection is not None and locked and not connection.invalidated:
                cleaned = await _bounded_cleanup(
                    lambda: connection.execute(  # type: ignore[union-attr]
                        sa.text("SELECT RELEASE_LOCK('qs-ai-schema-cutover')")
                    )
                )
                if not cleaned:
                    await _invalidate_owned(connection)
            for item in (watchdog, connection):
                if item is not None and not await _bounded_cleanup(item.close):
                    cleaned = False
                    await _invalidate_owned(item)
            if not cleaned and not failed:
                raise TimeoutError("Connection cleanup unconfirmed; inspect private evidence")
    finally:
        failed = sys.exc_info()[0] is not None
        if not await _bounded_cleanup(engine.dispose) and not failed:
            raise TimeoutError("Engine cleanup unconfirmed; inspect private evidence")
    if output is None:
        raise ValueError("Retirement command did not produce evidence")
    return output


def main() -> None:
    try:
        result = asyncio.run(run(parser().parse_args()))
    except Exception as error:
        # Do not disclose driver SQL, business rows, credentials or arbitrary exception text.
        print(json.dumps({"prompt_retirement": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))
    if result["prompt_retirement"] != "ok":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
