"""The CLI cancels its own native MySQL work without inferring an unknown exchange."""

import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.maintenance.schema_refactor import __main__ as cli
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.conversion import manifest
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, qualified
from tests.integration.test_schema_refactor_conversion import cloned as cloned
from tests.integration.test_schema_refactor_conversion import cutover

pytestmark = pytest.mark.integration


def asynchronous_engine(conn, source):
    # Both connections are owned by this test/CLI and use the same disposable server.
    url = conn.engine.url.set(drivername="mysql+asyncmy", database=source)
    return create_async_engine(url, connect_args={"connect_timeout": 3})


def stop_sync_writer(conn):
    conn.commit()
    conn.execute(sa.text("USE information_schema"))
    conn.commit()


async def test_blocked_native_select_is_killed_and_owned_connection_is_invalidated(cloned):
    conn, state, _ = cloned
    stop_sync_writer(conn)
    engine = asynchronous_engine(conn, state["source"])
    try:
        async with engine.connect() as main, engine.connect() as watchdog:
            identity = await main.scalar(sa.text("SELECT CONNECTION_ID()"))
            started = time.monotonic()
            with pytest.raises(TimeoutError, match="keep writers stopped"):
                await cli.bounded_operation(
                    main,
                    watchdog,
                    lambda: main.execute(sa.text("SELECT SLEEP(10)")),
                    deadline=time.monotonic() + 0.2,
                )
            assert time.monotonic() - started < 5
            assert main.invalidated
            assert (
                conn.scalar(
                    sa.text(
                        "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID=:identity"
                    ),
                    {"identity": identity},
                )
                == 0
            )
    finally:
        await engine.dispose()


async def test_blocked_native_insert_select_is_rolled_back_after_owned_kill(cloned):
    conn, state, _ = cloned
    source = state["source"]
    for name in ("timeout_input", "timeout_output"):
        conn.execute(
            sa.text(f"CREATE TABLE {qualified(source, name)} (id INT PRIMARY KEY,value INT)")
        )
    conn.execute(sa.text(f"INSERT INTO {qualified(source, 'timeout_input')} VALUES (1,2)"))
    stop_sync_writer(conn)
    # Hold a row lock with a separate connection; this schema is UUID-owned by this fixture.
    conn.execute(sa.text(f"UPDATE {qualified(source, 'timeout_input')} SET value=3 WHERE id=1"))
    engine = asynchronous_engine(conn, source)
    try:
        async with engine.connect() as main, engine.connect() as watchdog:
            with pytest.raises(TimeoutError, match="keep writers stopped"):
                await cli.bounded_operation(
                    main,
                    watchdog,
                    lambda: main.execute(
                        sa.text(
                            f"INSERT INTO {qualified(source, 'timeout_output')} "
                            f"SELECT id,value FROM {qualified(source, 'timeout_input')} FOR SHARE"
                        )
                    ),
                    deadline=time.monotonic() + 0.2,
                )
            assert main.invalidated
        conn.rollback()
        assert (
            conn.scalar(sa.text(f"SELECT COUNT(*) FROM {qualified(source, 'timeout_output')}")) == 0
        )
        assert conn.scalar(sa.text(f"SELECT value FROM {qualified(source, 'timeout_input')}")) == 2
    finally:
        conn.rollback()
        await engine.dispose()


async def test_native_rename_metadata_lock_timeout_retains_pending_and_both_accepted_copies(
    cloned, monkeypatch
):
    conn, state, journal = cloned
    accepted = manifest(conn, state["source"], OLD_HEAD)
    control.prepare(conn, state, journal)
    control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
    conn.commit()
    control.verified(conn, state, True)
    control.save(journal, state)
    stop_sync_writer(conn)
    native_exchange = control.exchange
    exchanges = []

    def observe_exchange(*args):
        exchanges.append(True)
        return native_exchange(*args)

    monkeypatch.setattr(control, "exchange", observe_exchange)
    engine = asynchronous_engine(conn, state["source"])
    args = SimpleNamespace(command="switch", writers_stopped=True)
    try:
        with conn.engine.connect() as blocker:
            blocker.execute(sa.text("USE information_schema"))
            blocker.execute(sa.text(f"SELECT * FROM {qualified(state['source'], 'prompt_assets')}"))
            async with engine.connect() as main, engine.connect() as watchdog:
                with pytest.raises(TimeoutError, match="keep writers stopped"):
                    await cli.bounded_operation(
                        main,
                        watchdog,
                        lambda: cli.execute_command(main, args, state, journal),
                        deadline=time.monotonic() + 8,
                    )
                assert main.invalidated
            blocker.rollback()
        assert exchanges == [True]  # The native atomic DDL was genuinely blocked and killed.
        pending = control.read(journal)
        assert pending["phase"] == "switch_pending" and pending["source_manifest"] == accepted
        assert contracts.head(conn, pending["source"]) == OLD_HEAD
        assert contracts.head(conn, pending["target"]) == NEW_HEAD
        assert not contracts.tables(conn, pending["archive"])
        assert manifest(conn, pending["source"], OLD_HEAD) == accepted
        assert manifest(conn, pending["target"], NEW_HEAD) == accepted
    finally:
        await engine.dispose()


@pytest.mark.parametrize("exchanged", [False, True])
async def test_expired_pending_gets_bounded_probe_without_a_second_or_fresh_native_rename(
    cloned, monkeypatch, exchanged
):
    conn, state, journal = cloned
    accepted = manifest(conn, state["source"], OLD_HEAD)
    if exchanged:
        cutover(conn, state, journal)
    else:
        control.prepare(conn, state, journal)
        control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
        conn.commit()
        control.verified(conn, state, True)
    state["phase"] = "switch_pending"
    state["stopped_at"] = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    control.save(journal, state)
    stop_sync_writer(conn)

    def forbidden_exchange(*args):
        raise AssertionError("Expired recovery cannot perform a new RENAME")

    monkeypatch.setattr(control, "exchange", forbidden_exchange)
    args = SimpleNamespace(command="switch", writers_stopped=True)
    deadline = cli.operation_deadline(args, state)
    assert time.monotonic() + 59 < deadline < time.monotonic() + 61
    engine = asynchronous_engine(conn, state["source"])
    try:
        async with engine.connect() as main, engine.connect() as watchdog:

            async def operation():
                return await cli.execute_command(main, args, state, journal)

            if exchanged:
                result = await cli.bounded_operation(main, watchdog, operation, deadline=deadline)
                assert result["phase"] == control.read(journal)["phase"] == "switched"
            else:
                with pytest.raises(TimeoutError, match="exchange deadline exceeded"):
                    await cli.bounded_operation(main, watchdog, operation, deadline=deadline)
                assert control.read(journal)["phase"] == "switch_pending"
        head = NEW_HEAD if exchanged else OLD_HEAD
        assert contracts.head(conn, state["source"]) == head
        assert manifest(conn, state["source"], head) == accepted
        if exchanged:
            assert manifest(conn, state["archive"], OLD_HEAD) == accepted
            assert not contracts.tables(conn, state["target"])
        else:
            assert manifest(conn, state["target"], NEW_HEAD) == accepted
            assert not contracts.tables(conn, state["archive"])
    finally:
        await engine.dispose()
