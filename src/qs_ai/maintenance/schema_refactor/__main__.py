"""Explicit operator entrypoint; reads credentials from an environment variable only."""

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from qs_ai.maintenance.schema_refactor import control
from qs_ai.maintenance.schema_refactor.layouts import COPY_DEADLINE_SECONDS

T = TypeVar("T")
SWITCH_DEADLINE_SECONDS = COPY_DEADLINE_SECONDS + 5 * 60
RECOVERY_PROBE_SECONDS = 60
CONNECTION_CLEANUP_SECONDS = 3


def operation_deadline(args: argparse.Namespace, state: dict[str, Any] | None) -> float | None:
    """Retain the original stop clock; overdue pending exchanges only get a bounded probe."""
    if args.command not in ("copy", "verify", "switch", "rollback"):
        return None
    if state is None:
        raise ValueError("Maintenance command requires a journal")
    if args.command == "copy":
        stopped_at = control._same_stop(state, "stopped_at", args.stopped_at)
    elif args.command == "rollback":
        stopped_at = control._same_stop(state, "rollback_stopped_at", args.stopped_at)
    else:
        stopped_at = state["stopped_at"]
    start = datetime.fromisoformat(stopped_at)
    if start.tzinfo is None or start.utcoffset() is None:
        raise ValueError("Stopped-at timestamp requires an explicit time zone")
    elapsed = (datetime.now(UTC) - start).total_seconds()
    if elapsed < 0:
        raise TimeoutError("Maintenance stop clock is in the future")
    duration = SWITCH_DEADLINE_SECONDS if args.command == "switch" else COPY_DEADLINE_SECONDS
    if elapsed >= duration:
        if (args.command, state["phase"]) in (
            ("switch", "switch_pending"),
            ("rollback", "rollback_pending"),
        ):
            # The controller probes first and its unchanged 25-minute deadline forbids a new
            # exchange/copy if the existing exchange has not happened. This cannot restart writers.
            return time.monotonic() + RECOVERY_PROBE_SECONDS
        raise TimeoutError("Maintenance operation deadline exceeded")
    return time.monotonic() + duration - elapsed


def _consume_task(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


async def _stop_owned_operation(
    connection: AsyncConnection,
    watchdog: AsyncConnection,
    connection_id: int,
    task: asyncio.Task[Any],
) -> bool:
    killed = False
    try:
        # The main socket is still owned and open here. Never cancel/invalidate it before KILL,
        # which could otherwise turn a saved connection ID into an unrelated connection target.
        if not connection.invalidated and not connection.closed:
            async with asyncio.timeout(CONNECTION_CLEANUP_SECONDS):
                await watchdog.execute(sa.text(f"KILL CONNECTION {connection_id}"))
            killed = True
    except Exception:
        # A lost KILL reply is itself uncertain. Do not use it to infer any DDL outcome.
        pass
    finally:
        task.cancel()
        await asyncio.wait({task}, timeout=CONNECTION_CLEANUP_SECONDS)
        if task.done():
            _consume_task(task)
        else:
            task.add_done_callback(_consume_task)
        if not connection.invalidated:
            try:
                async with asyncio.timeout(CONNECTION_CLEANUP_SECONDS):
                    await connection.invalidate()
            except Exception:
                pass
    return killed


async def bounded_operation(
    connection: AsyncConnection,
    watchdog: AsyncConnection,
    action: Callable[[], Awaitable[T]],
    *,
    deadline: float,
) -> T:
    """Cancel only this CLI's owned MySQL connection, before cancelling the client task.

    Adapted database IO yields to this timer. Inline CPU work and synchronous fsync are not
    OS hard-real-time operations; batch clock checks supplement this SQL blocking guard.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Maintenance operation deadline exceeded")
    async with asyncio.timeout(min(CONNECTION_CLEANUP_SECONDS, remaining)):
        connection_id = await connection.scalar(sa.text("SELECT CONNECTION_ID()"))
        server = await connection.scalar(sa.text("SELECT @@server_uuid"))
        if not server or server != await watchdog.scalar(sa.text("SELECT @@server_uuid")):
            raise ValueError("Cancellation connection belongs to another database server")
        if type(connection_id) is not int or connection_id <= 0:
            raise ValueError("Maintenance connection identity is unavailable")
        # It must not appear as an extra writer connection in any maintained schema.
        await watchdog.execute(sa.text("USE information_schema"))
    connection.info["schema_refactor_deadline"] = deadline

    async def invoke() -> T:
        return await action()

    task: asyncio.Task[T] = asyncio.create_task(invoke())
    try:
        done, _ = await asyncio.wait({task}, timeout=max(0, deadline - time.monotonic()))
        if done:
            return await task
        killed = await _stop_owned_operation(connection, watchdog, connection_id, task)
    except asyncio.CancelledError:
        await asyncio.shield(_stop_owned_operation(connection, watchdog, connection_id, task))
        raise
    finally:
        if not connection.invalidated and not connection.closed:
            connection.info.pop("schema_refactor_deadline", None)
    reason = "Maintenance deadline reached; keep writers stopped and probe the saved journal"
    if not killed:
        reason += "; server cancellation could not be confirmed"
    # Never save the possibly advanced in-memory phase after an uncertain COMMIT/RENAME.
    raise TimeoutError(reason)


async def execute_command(
    connection: AsyncConnection,
    args: argparse.Namespace,
    state: dict[str, Any] | None,
    journal: Path,
) -> dict[str, Any]:
    if args.command == "plan":
        inherited = None
        if args.inherit_journal:
            inherited_path = await asyncio.to_thread(journal_path, "inherit", args.inherit_journal)
            inherited = await asyncio.to_thread(control.read, inherited_path)
        state = await connection.run_sync(
            lambda c: control.plan(
                c,
                args.source,
                args.target,
                args.archive,
                args.old_image,
                args.new_image,
                inherited,
            )
        )
    else:
        if state is None:
            raise ValueError("Maintenance command requires a journal")
        if args.command == "prepare":
            await connection.run_sync(lambda c: control.prepare(c, state, journal))
        elif args.command == "copy":
            await connection.run_sync(
                lambda c: control.copy(c, state, args.writers_stopped, args.stopped_at, journal)
            )
        elif args.command == "verify":
            await connection.run_sync(lambda c: control.verified(c, state, args.writers_stopped))
        elif args.command == "switch":
            await connection.run_sync(
                lambda c: control.switch(c, state, args.writers_stopped, journal)
            )
        elif args.command == "rollback":
            await connection.run_sync(
                lambda c: control.rollback(
                    c,
                    state,
                    args.writers_stopped,
                    args.stopped_at,
                    args.runtime_never_started,
                    journal,
                )
            )
        elif args.command == "cleanup":
            await connection.run_sync(lambda c: control.cleanup(c, state, journal))
    await connection.commit()
    # Cancellation cannot stop an already running fsync thread. This save is safe to finish:
    # COMMIT has already been confirmed, so the advanced journal phase is a durable fact.
    await asyncio.to_thread(control.save, journal, state)
    return state


def journal_path(command_name: str, value: str) -> Path:
    journal = Path(value).absolute()
    if command_name == "plan" and journal.exists():
        raise ValueError("Journal already exists; use its exact recorded plan")
    if not journal.parent.is_dir() or journal.is_symlink():
        raise ValueError("Journal requires an existing private directory")
    return journal


async def _bounded_cleanup(action: Callable[[], Awaitable[Any]]) -> bool:
    """Do not let a lost cleanup reply extend the stop window indefinitely."""

    async def invoke() -> Any:
        return await action()

    task = asyncio.create_task(invoke())
    done, _ = await asyncio.wait({task}, timeout=CONNECTION_CLEANUP_SECONDS)
    if done:
        try:
            await task
        except Exception:
            return False
        return True
    task.cancel()
    task.add_done_callback(_consume_task)
    return False


async def _invalidate_owned(connection: AsyncConnection) -> None:
    if not connection.invalidated:
        await _bounded_cleanup(connection.invalidate)


async def run(args: argparse.Namespace) -> None:
    url = os.environ.get(args.url_env, "")
    if not url.startswith("mysql+asyncmy://"):
        raise ValueError("A MySQL asyncmy URL is required in the selected environment variable")
    journal = await asyncio.to_thread(journal_path, args.command, args.journal)
    state = None if args.command == "plan" else await asyncio.to_thread(control.read, journal)
    deadline = operation_deadline(args, state)
    engine = create_async_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    connection: AsyncConnection | None = None
    watchdog: AsyncConnection | None = None
    locked = None
    try:
        try:
            setup: float = CONNECTION_CLEANUP_SECONDS
            if deadline is not None:
                setup = min(setup, max(0, deadline - time.monotonic()))
            async with asyncio.timeout(setup):
                connection = await engine.connect()
                locked = await connection.scalar(
                    sa.text("SELECT GET_LOCK('qs-ai-schema-cutover',0)")
                )
                watchdog = await engine.connect() if deadline is not None else None
            if locked != 1:
                raise ValueError("Another schema maintenance operation holds the lock")
            if deadline is None:
                state = await execute_command(connection, args, state, journal)
            else:
                if watchdog is None:
                    raise ValueError("Cancellation connection was not prepared")
                state = await bounded_operation(
                    connection,
                    watchdog,
                    lambda: execute_command(connection, args, state, journal),
                    deadline=deadline,
                )
        finally:
            error_active = sys.exc_info()[0] is not None
            cleaned = True
            if connection is not None and locked == 1 and not connection.invalidated:
                cleaned = await _bounded_cleanup(
                    lambda: connection.execute(  # type: ignore[union-attr]
                        sa.text("SELECT RELEASE_LOCK('qs-ai-schema-cutover')")
                    )
                )
                if not cleaned:
                    await _invalidate_owned(connection)
            for owned in (watchdog, connection):
                if owned is not None and not await _bounded_cleanup(owned.close):
                    cleaned = False
                    await _invalidate_owned(owned)
            if not cleaned and not error_active:
                raise TimeoutError(
                    "Maintenance connection cleanup could not be confirmed; "
                    "keep writers stopped and probe the saved journal"
                )
    finally:
        error_active = sys.exc_info()[0] is not None
        if not await _bounded_cleanup(engine.dispose) and not error_active:
            raise TimeoutError(
                "Maintenance engine cleanup could not be confirmed; "
                "keep writers stopped and probe the saved journal"
            )
    if state is None:
        raise ValueError("Maintenance command did not return a journal")
    print(
        json.dumps(
            {
                "schema_refactor": "ok",
                "phase": state["phase"],
                "source_head": state["source_head"],
                "target_head": state["target_head"],
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url-env", default="QS_AI_DATABASE_URL")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "prepare", "copy", "verify", "switch", "rollback", "cleanup"):
        command = sub.add_parser(name)
        command.add_argument("--journal", required=True)
        if name == "plan":
            for option in ("source", "target", "archive", "old-image", "new-image"):
                command.add_argument("--" + option, required=True)
            command.add_argument("--inherit-journal")
        if name in ("copy", "verify", "switch", "rollback"):
            command.add_argument("--writers-stopped", action="store_true")
        if name in ("copy", "rollback"):
            command.add_argument("--stopped-at", required=True)
        if name == "rollback":
            command.add_argument("--runtime-never-started", action="store_true")
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        # Driver exceptions can contain credentials, original SQL and business bodies.
        safe = str(error) if type(error) in (ValueError, TimeoutError) else None
        print(
            json.dumps(
                {"schema_refactor": "failed", "error_type": type(error).__name__, "reason": safe}
            )
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
