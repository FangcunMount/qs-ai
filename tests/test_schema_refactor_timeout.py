"""Deadline cancellation targets the CLI-owned socket and retains uncertain journals."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from qs_ai.maintenance.schema_refactor import __main__ as cli
from qs_ai.maintenance.schema_refactor import control


class Connection:
    def __init__(self, events, label, server="server-1", identity=41):
        self.events, self.label, self.server, self.identity = events, label, server, identity
        self.invalidated, self.closed, self.info = False, False, {}
        self.kill_error = False
        self.block_commit = False

    async def scalar(self, statement):
        if "GET_LOCK" in str(statement):
            return 1
        return self.identity if "CONNECTION_ID" in str(statement) else self.server

    async def execute(self, statement):
        self.events.append((self.label, str(statement)))
        if self.kill_error and "KILL" in str(statement):
            raise RuntimeError("Cancellation reply lost")

    async def invalidate(self):
        self.events.append((self.label, "invalidate"))
        self.invalidated = True

    async def close(self):
        self.events.append((self.label, "close"))
        self.closed = True

    async def run_sync(self, call):
        return call(self)

    async def commit(self):
        if self.block_commit:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.events.append((self.label, "cancel commit"))
                raise


async def test_deadline_kills_owned_socket_before_client_cancellation_and_binds_batch_clock():
    events = []
    connection, watchdog = Connection(events, "main"), Connection(events, "watchdog", identity=42)
    budget = time.monotonic() + 0.05

    async def blocked():
        assert connection.info["schema_refactor_deadline"] == budget
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append(("main", "cancel action"))
            raise

    with pytest.raises(TimeoutError, match="keep writers stopped"):
        await cli.bounded_operation(connection, watchdog, blocked, deadline=budget)
    assert (
        events.index(("watchdog", "USE information_schema"))
        < events.index(("watchdog", "KILL CONNECTION 41"))
        < events.index(("main", "cancel action"))
        < events.index(("main", "invalidate"))
    )


async def test_blocking_commit_leaves_original_pending_journal_and_never_saves_advanced_state(
    monkeypatch, tmp_path
):
    events = []
    connection, watchdog = Connection(events, "main"), Connection(events, "watchdog", identity=42)
    connection.block_commit = True
    journal = tmp_path / "journal.json"
    state = dict(format="qs-ai-schema-refactor/v1", phase="prepared")
    control.save(journal, state)

    def copy(conn, value, stopped, stopped_at, path):
        value["phase"] = "copy_pending"
        control.save(path, value)
        value["phase"] = "copied"

    monkeypatch.setattr(control, "copy", copy)
    args = SimpleNamespace(command="copy", writers_stopped=True, stopped_at="unused")
    with pytest.raises(TimeoutError):
        await cli.bounded_operation(
            connection,
            watchdog,
            lambda: cli.execute_command(connection, args, state, journal),
            deadline=time.monotonic() + 0.05,
        )
    assert control.read(journal)["phase"] == "copy_pending"
    assert state["phase"] == "copied"
    assert events.index(("watchdog", "KILL CONNECTION 41")) < events.index(
        ("main", "cancel commit")
    )


async def test_success_returns_action_result_without_kill_or_stale_batch_clock():
    events = []
    connection, watchdog = Connection(events, "main"), Connection(events, "watchdog", identity=42)

    async def completed():
        return "accepted"

    assert (
        await cli.bounded_operation(connection, watchdog, completed, deadline=time.monotonic() + 1)
        == "accepted"
    )
    assert not any("KILL" in sql for _, sql in events)
    assert "schema_refactor_deadline" not in connection.info


async def test_other_server_cannot_cancel_any_connection_or_start_action():
    events = []
    connection = Connection(events, "main")
    watchdog = Connection(events, "watchdog", server="server-2", identity=42)

    async def action():
        raise AssertionError("Operation must not start without ownership binding")

    with pytest.raises(ValueError, match="another database server"):
        await cli.bounded_operation(connection, watchdog, action, deadline=time.monotonic() + 1)
    assert not events


async def test_unconfirmed_kill_fails_closed_and_still_invalidates_owned_socket():
    events = []
    connection, watchdog = Connection(events, "main"), Connection(events, "watchdog", identity=42)
    watchdog.kill_error = True

    async def blocked():
        await asyncio.Event().wait()

    with pytest.raises(TimeoutError, match="cancellation could not be confirmed"):
        await cli.bounded_operation(connection, watchdog, blocked, deadline=time.monotonic() + 0.05)
    assert connection.invalidated


@pytest.mark.parametrize(
    "command,minutes", [("copy", 24), ("verify", 24), ("switch", 29), ("rollback", 24)]
)
def test_each_command_reuses_original_stop_budget(monkeypatch, command, minutes):
    monkeypatch.setattr(cli.time, "monotonic", lambda: 100)
    stopped = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    state = dict(phase="verified", stopped_at=stopped, rollback_stopped_at=stopped)
    args = SimpleNamespace(command=command, stopped_at=stopped)
    assert cli.operation_deadline(args, state) == pytest.approx(160, abs=0.1)


@pytest.mark.parametrize(
    "command,phase", [("switch", "switch_pending"), ("rollback", "rollback_pending")]
)
def test_overdue_unknown_exchange_has_short_probe_budget_without_resetting_stop_clock(
    monkeypatch, command, phase
):
    monkeypatch.setattr(cli.time, "monotonic", lambda: 100)
    stopped = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    state = dict(phase=phase, stopped_at=stopped, rollback_stopped_at=stopped)
    assert (
        cli.operation_deadline(SimpleNamespace(command=command, stopped_at=stopped), state) == 160
    )
    assert state["stopped_at"] == stopped
    with pytest.raises(TimeoutError):
        control.deadline(stopped)  # Controller still forbids any fresh copy/RENAME.


def test_completed_or_unstarted_operations_cannot_extend_expired_window():
    stopped = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    with pytest.raises(TimeoutError):
        cli.operation_deadline(
            SimpleNamespace(command="switch"), dict(phase="switched", stopped_at=stopped)
        )
    with pytest.raises(ValueError, match="original stopped-at"):
        cli.operation_deadline(
            SimpleNamespace(command="copy", stopped_at=datetime.now(UTC).isoformat()),
            dict(phase="copy_pending", stopped_at=stopped),
        )


@pytest.mark.parametrize("blocked_step", ["release", "close", "dispose"])
async def test_cleanup_is_bounded_and_success_is_not_printed_before_cleanup(
    monkeypatch, tmp_path, capsys, blocked_step
):
    events = []
    connection = Connection(events, "main")
    journal = tmp_path / "journal.json"
    state = dict(
        format="qs-ai-schema-refactor/v1", phase="cleaned", source_head="0038", target_head="0040"
    )
    control.save(journal, state)
    monkeypatch.setenv("TEST_MAINTENANCE_URL", "mysql+asyncmy://test@localhost/isolated")
    monkeypatch.setattr(cli, "CONNECTION_CLEANUP_SECONDS", 0.02)
    native_execute, native_close = connection.execute, connection.close

    async def execute(statement):
        if blocked_step == "release" and "RELEASE_LOCK" in str(statement):
            await asyncio.Event().wait()
        return await native_execute(statement)

    async def close():
        if blocked_step == "close":
            await asyncio.Event().wait()
        await native_close()

    async def completed(*args):
        return state

    class Engine:
        async def connect(self):
            return connection

        async def dispose(self):
            if blocked_step == "dispose":
                await asyncio.Event().wait()

    monkeypatch.setattr(connection, "execute", execute)
    monkeypatch.setattr(connection, "close", close)
    monkeypatch.setattr(cli, "create_async_engine", lambda *args, **kwargs: Engine())
    monkeypatch.setattr(cli, "execute_command", completed)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="cleanup could not be confirmed"):
        await cli.run(
            SimpleNamespace(command="cleanup", url_env="TEST_MAINTENANCE_URL", journal=str(journal))
        )
    assert time.monotonic() - started < 0.5
    assert capsys.readouterr().out == ""
    if blocked_step != "dispose":
        assert connection.invalidated


async def test_cleanup_failure_preserves_original_operation_error(monkeypatch, tmp_path):
    connection = Connection([], "main")
    journal = tmp_path / "journal.json"
    control.save(journal, dict(format="qs-ai-schema-refactor/v1", phase="prepared"))
    monkeypatch.setenv("TEST_MAINTENANCE_URL", "mysql+asyncmy://test@localhost/isolated")

    async def broken(*args):
        raise ValueError("Preserve the original layout uncertainty")

    class Engine:
        async def connect(self):
            return connection

        async def dispose(self):
            raise ConnectionError("Lost disposal reply")

    monkeypatch.setattr(cli, "create_async_engine", lambda *args, **kwargs: Engine())
    monkeypatch.setattr(cli, "execute_command", broken)
    with pytest.raises(ValueError, match="original layout uncertainty"):
        await cli.run(
            SimpleNamespace(command="cleanup", url_env="TEST_MAINTENANCE_URL", journal=str(journal))
        )
