"""Reconnect to native MySQL after losing the reply around an atomic RENAME."""

import stat
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory

from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.conversion import manifest
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, qualified
from qs_ai.maintenance.schema_refactor.validation import require_schema
from tests.integration.test_schema_refactor_conversion import cloned as cloned
from tests.integration.test_schema_refactor_conversion import cutover

pytestmark = pytest.mark.integration


def lose_connection_at_exchange(monkeypatch, moment):
    """Execute native DDL, but make the client unable to know its outcome."""
    native_exchange = control.exchange
    executed = []

    def disconnect(conn, source, target, archive):
        if moment == "after":
            native_exchange(conn, source, target, archive)
            executed.append((source, target, archive))
        conn.invalidate()
        raise ConnectionError("Maintenance connection lost around RENAME")

    def retry(conn, source, target, archive):
        native_exchange(conn, source, target, archive)
        executed.append((source, target, archive))

    monkeypatch.setattr(control, "exchange", disconnect)
    return executed, retry


@pytest.mark.parametrize("moment", ["before", "after"])
def test_switch_reconnect_probes_native_layout_without_repeating_rename(
    cloned, monkeypatch, moment
):
    conn, state, journal = cloned
    original_connection = conn.scalar(sa.text("SELECT CONNECTION_ID()"))
    accepted = manifest(conn, state["source"], OLD_HEAD)
    assert len(accepted) == 53 and all(row["rows"] >= 1 for row in accepted.values())
    control.prepare(conn, state, journal)
    control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
    conn.commit()
    control.verified(conn, state, True)
    executed, retry = lose_connection_at_exchange(monkeypatch, moment)
    with pytest.raises(ConnectionError, match="connection lost"):
        control.switch(conn, state, True, journal)
    assert conn.invalidated
    pending = control.read(journal)
    assert stat.S_IMODE(journal.stat().st_mode) == 0o600
    assert pending["phase"] == "switch_pending" and pending["source_manifest"] == accepted
    conn.rollback()  # Clear the lost transaction before the fixture later reconnects for cleanup.

    with conn.engine.connect() as resumed:
        assert resumed.scalar(sa.text("SELECT CONNECTION_ID()")) != original_connection
        expected_head = OLD_HEAD if moment == "before" else NEW_HEAD
        assert contracts.head(resumed, pending["source"]) == expected_head
        assert manifest(resumed, pending["source"], expected_head) == accepted
        if moment == "after":
            assert not contracts.tables(resumed, pending["target"])
            assert manifest(resumed, pending["archive"], OLD_HEAD) == accepted
        else:
            assert not contracts.tables(resumed, pending["archive"])
            assert manifest(resumed, pending["target"], NEW_HEAD) == accepted
        monkeypatch.setattr(control, "exchange", retry)
        control.switch(resumed, pending, True, journal)
        resumed.commit()
        assert len(executed) == 1
        assert pending["phase"] == control.read(journal)["phase"] == "switched"
        require_schema(resumed, NEW_HEAD, pending["source"])
        require_schema(resumed, OLD_HEAD, pending["archive"])
        assert manifest(resumed, pending["source"], NEW_HEAD) == accepted
        assert manifest(resumed, pending["archive"], OLD_HEAD) == accepted
        assert not contracts.tables(resumed, pending["target"])
        control.switch(resumed, control.read(journal), True, journal)
        assert len(executed) == 1


@pytest.mark.parametrize("moment", ["before", "after"])
def test_reverse_rollback_reconnect_preserves_new_writes_and_does_not_repeat_rename(
    cloned, monkeypatch, moment
):
    conn, state, journal = cloned
    cutover(conn, state, journal)
    original = state["source_manifest"]
    conn.execute(
        sa.text(
            f"UPDATE {qualified(state['source'], 'messaging_observations')} "
            "SET recorded_count=recorded_count+17,last_observed_at=:observed"
        ),
        {"observed": datetime.now(UTC).replace(tzinfo=None)},
    )
    conn.commit()
    accepted = manifest(conn, state["source"], NEW_HEAD)
    assert accepted != original and len(accepted) == 53
    stopped_at = datetime.now(UTC).isoformat()
    original_connection = conn.scalar(sa.text("SELECT CONNECTION_ID()"))
    executed, retry = lose_connection_at_exchange(monkeypatch, moment)
    with pytest.raises(ConnectionError, match="connection lost"):
        control.rollback(conn, state, True, stopped_at, journal=journal)
    assert conn.invalidated
    pending = control.read(journal)
    assert stat.S_IMODE(journal.stat().st_mode) == 0o600
    assert pending["phase"] == "rollback_pending" and pending["rollback_mode"] == "reverse"
    assert pending["rollback_manifest"] == accepted
    assert pending["rollback_stopped_at"] == stopped_at
    assert pending["rollback_target"] in pending["owned_schemas"]
    assert pending["rollback_archive"] in pending["owned_schemas"]
    conn.rollback()

    with conn.engine.connect() as resumed:
        assert resumed.scalar(sa.text("SELECT CONNECTION_ID()")) != original_connection
        expected_head = NEW_HEAD if moment == "before" else OLD_HEAD
        assert contracts.head(resumed, pending["source"]) == expected_head
        assert manifest(resumed, pending["source"], expected_head) == accepted
        assert manifest(resumed, pending["archive"], OLD_HEAD) == original
        pristine = False
        if moment == "after":
            assert not contracts.tables(resumed, pending["rollback_target"])
            assert manifest(resumed, pending["rollback_archive"], NEW_HEAD) == accepted
        else:
            assert not contracts.tables(resumed, pending["rollback_archive"])
            pristine = control._pristine(resumed, pending["rollback_target"], OLD_HEAD)
            if pristine:
                # The final Alembic version/seed inserts may also have been uncommitted.
                assert contracts.tables(resumed, pending["rollback_target"]) == (
                    set(contracts.v0038.metadata.tables) | {"alembic_version"}
                )
                versions = (
                    resumed.execute(
                        sa.text(
                            "SELECT version_num FROM "
                            + qualified(pending["rollback_target"], "alembic_version")
                        )
                    )
                    .scalars()
                    .all()
                )
                revisions = {
                    r.revision
                    for r in ScriptDirectory.from_config(Config("alembic.ini")).walk_revisions()
                }
                assert len(versions) <= 1 and set(versions) <= revisions
            else:
                # Every nonempty target must be the complete accepted copy, never a partial copy.
                require_schema(
                    resumed,
                    OLD_HEAD,
                    pending["rollback_target"],
                    column_collations=pending["source_column_collations"],
                )
                assert manifest(resumed, pending["rollback_target"], OLD_HEAD) == accepted
        native_copy = control.copy_data
        recopied = []

        def observe_copy(*args, **kwargs):
            native_copy(*args, **kwargs)
            recopied.append(True)

        monkeypatch.setattr(control, "copy_data", observe_copy)
        monkeypatch.setattr(control, "exchange", retry)
        control.rollback(resumed, pending, True, stopped_at, journal=journal)
        resumed.commit()
        assert len(recopied) == int(pristine)
        assert len(executed) == 1
        assert pending["phase"] == control.read(journal)["phase"] == "rolled_back"
        require_schema(resumed, OLD_HEAD, pending["source"])
        require_schema(resumed, NEW_HEAD, pending["rollback_archive"])
        assert manifest(resumed, pending["source"], OLD_HEAD) == accepted
        assert manifest(resumed, pending["rollback_archive"], NEW_HEAD) == accepted
        assert manifest(resumed, pending["archive"], OLD_HEAD) == original
        assert not contracts.tables(resumed, pending["rollback_target"])
        control.rollback(resumed, control.read(journal), True, stopped_at, journal=journal)
        assert len(executed) == 1
