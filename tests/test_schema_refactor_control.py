"""Fault contracts for journalled exchanges; unexpected data is never overwritten."""

import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa

from qs_ai.maintenance.schema_refactor import control
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier, qualified


def facts(rows=1):
    return {"logical_table": {"rows": rows, "sha256": f"original:{rows}"}}


class Store:
    def __init__(self):
        self.schemas = {
            "ai_test": {"head": OLD_HEAD, "facts": facts(), "valid": True},
            "ai_refactor_test": {"head": NEW_HEAD, "facts": facts(), "valid": True},
            "ai_backup_test": None,
        }
        self.conn = SimpleNamespace(
            scalar=self.scalar, execute=self.execute, invalidated=False, info={}
        )
        self.exchange_fault = None
        self.drop_fault = False
        self.creates = []
        self.copies = []
        self.exchanges = []
        self.checked = []
        self.grants = "GRANT PROCESS ON *.*"
        self.connections = {}

    def scalar(self, statement, parameters=None):
        if "@@server_uuid" in str(statement):
            return "isolated-server"
        if "lock_wait_timeout" in str(statement):
            return 31536000
        if "PROCESSLIST" in str(statement):
            return self.connections.get(parameters["schema"], 0)
        raise AssertionError(str(statement))

    def execute(self, statement, parameters=None):
        sql = str(statement)
        if "SCHEMA_NAME FROM information_schema.SCHEMATA" in sql:
            return SimpleNamespace(scalars=lambda: self.schemas.keys())
        if sql == "SHOW GRANTS":
            return [(self.grants,)]
        if sql.startswith("SET SESSION"):
            return None
        if sql.startswith("DROP DATABASE"):
            name = sql.split("`")[1]
            del self.schemas[name]
            if self.drop_fault:
                self.drop_fault = False
                raise ConnectionError("lost reply after DROP")
            return None
        raise AssertionError(sql)

    def tables(self, conn, schema):
        return {"alembic_version", "logical_table"} if self.schemas.get(schema) else set()

    def head(self, conn, schema):
        return self.schemas[schema]["head"]

    def require(self, conn, head, schema, **kwargs):
        self.checked.append((schema, head))
        row = self.schemas.get(schema)
        if not row or row["head"] != head or not row["valid"]:
            raise ValueError("Schema/FK contract mismatch")

    def manifest(self, conn, schema, head):
        assert self.schemas[schema]["head"] == head
        return deepcopy(self.schemas[schema]["facts"])

    def verify(self, conn, source, target, source_head, target_head, **kwargs):
        left = self.manifest(conn, source, source_head)
        right = self.manifest(conn, target, target_head)
        if left != right:
            raise ValueError("Data projection mismatch")
        return left

    def create(self, conn, schema, source):
        assert schema not in self.schemas
        self.creates.append(schema)
        self.schemas[schema] = None

    def upgrade(self, conn, schema, head, source):
        if schema in self.schemas:
            self.require(conn, head, schema)
            if not self.pristine(conn, schema, head):
                raise ValueError("Owned destination contains business data")
        else:
            self.creates.append(schema)
            self.schemas[schema] = {"head": head, "facts": facts(0), "valid": True}

    def pristine(self, conn, schema, head):
        return self.schemas[schema]["facts"] == facts(0)

    def copy(self, conn, source, target, source_head, target_head, **kwargs):
        self.copies.append((source, target))
        self.schemas[target]["facts"] = self.manifest(conn, source, source_head)

    def exchange(self, conn, source, target, archive):
        self.exchanges.append((source, target, archive))
        assert self.schemas[archive] is None
        if self.exchange_fault == "before":
            self.exchange_fault = None
            raise ConnectionError("lost before RENAME")
        self.schemas[archive] = self.schemas[source]
        self.schemas[source] = self.schemas[target]
        self.schemas[target] = None
        fault, self.exchange_fault = self.exchange_fault, None
        if fault == "after":
            raise ConnectionError("lost reply after RENAME")
        if fault == "late_write":
            self.schemas[archive]["facts"] = facts(2)
        if fault == "broken_fk":
            self.schemas[source]["valid"] = False


@pytest.fixture
def simulation(monkeypatch, tmp_path):
    store = Store()
    monkeypatch.setattr(control, "_binding", lambda *args: None)
    monkeypatch.setattr(control, "_exists", lambda conn, schema: schema in store.schemas)
    monkeypatch.setattr(control, "require_schema", store.require)
    monkeypatch.setattr(control, "manifest", store.manifest)
    monkeypatch.setattr(control, "verify", store.verify)
    monkeypatch.setattr(control, "upgrade_isolated", store.upgrade)
    monkeypatch.setattr(control, "_pristine", store.pristine)
    monkeypatch.setattr(control, "copy_data", store.copy)
    monkeypatch.setattr(control, "align_counters", lambda *args, **kwargs: None)
    monkeypatch.setattr(control, "exchange", store.exchange)
    monkeypatch.setattr(control.contracts, "head", store.head)
    monkeypatch.setattr(control.contracts, "tables", store.tables)
    monkeypatch.setattr(control.contracts, "create_schema", store.create)
    monkeypatch.setattr(
        control.contracts, "schema_options", lambda *args: ("utf8mb4", "utf8mb4_bin")
    )
    now = datetime.now(UTC).isoformat()
    state = dict(
        format="qs-ai-schema-refactor/v1",
        phase="verified",
        source="ai_test",
        target="ai_refactor_test",
        archive="ai_backup_test",
        source_head=OLD_HEAD,
        target_head=NEW_HEAD,
        source_manifest=facts(),
        stopped_at=now,
        created_at=now,
        owned_schemas=["ai_refactor_test", "ai_backup_test"],
    )
    return store, state, tmp_path / "journal.json"


def switched(simulation):
    store, state, journal = simulation
    control.switch(store.conn, state, True, journal)
    return store, state, journal


def test_prepare_refuses_destinations_created_outside_its_pending_journal(simulation):
    store, state, journal = simulation
    state["phase"] = "planned"
    with pytest.raises(ValueError, match="already exist"):
        control.prepare(store.conn, state, journal)
    assert state["phase"] == "planned" and not store.creates


def test_reverse_plan_requires_the_original_forward_column_evidence(simulation, monkeypatch):
    store, state, _ = switched(simulation)
    image = "sha256:" + "a" * 64
    with pytest.raises(ValueError, match="original forward journal"):
        control.plan(
            store.conn, state["source"], "ai_rollback_plan", "ai_failed_plan", image, image
        )
    original = {"prompt_drafts": {"draft_id": "utf8mb4_general_ci"}}
    cohort = {"draft_id": "utf8mb4_general_ci", "command_id": "utf8mb4_general_ci"}
    state.update(source_column_collations=original, legacy_prompt_collations=cohort)
    monkeypatch.setattr(
        control,
        "_binding",
        lambda conn, inherited: conn.info.update(
            source_column_collations=inherited["source_column_collations"]
        ),
    )
    monkeypatch.setattr(
        control.contracts,
        "source_column_collations",
        lambda conn, *args: conn.info["source_column_collations"],
        raising=False,
    )
    monkeypatch.setattr(control.contracts, "legacy_collations", lambda *args: cohort)
    result = control.plan(
        store.conn,
        state["source"],
        "ai_rollback_plan",
        "ai_failed_plan",
        image,
        image,
        state,
    )
    assert result["source_head"] == NEW_HEAD and result["target_head"] == OLD_HEAD
    assert result["source_column_collations"] == original
    other = {**state, "source": "ai_some_other_source"}
    with pytest.raises(ValueError, match="this source"):
        control.plan(
            store.conn,
            state["source"],
            "ai_rollback_plan",
            "ai_failed_plan",
            image,
            image,
            other,
        )


@pytest.mark.parametrize("empty", [True, False])
def test_prepare_pending_resumes_exact_owned_pristine_structure_only(simulation, empty):
    store, state, journal = simulation
    state["phase"] = "prepare_pending"
    store.schemas[state["target"]]["facts"] = facts(0) if empty else facts(2)
    if empty:
        control.prepare(store.conn, state, journal)
        assert state["phase"] == "prepared"
    else:
        with pytest.raises(ValueError, match="business data"):
            control.prepare(store.conn, state, journal)
        assert store.schemas[state["target"]]["facts"] == facts(2)
    assert not store.creates


@pytest.mark.parametrize("fault", ["before", "after"])
def test_switch_lost_reply_resumes_each_atomic_rename_outcome(simulation, fault):
    store, state, journal = simulation
    store.exchange_fault = fault
    with pytest.raises(ConnectionError):
        control.switch(store.conn, state, True, journal)
    state = control.read(journal)
    assert state["phase"] == "switch_pending"
    control.switch(store.conn, state, True, journal)
    assert state["phase"] == "switched"
    assert store.schemas["ai_test"]["facts"] == store.schemas["ai_backup_test"]["facts"] == facts()
    assert ("ai_test", NEW_HEAD) in store.checked and ("ai_backup_test", OLD_HEAD) in store.checked
    assert len(store.exchanges) == (2 if fault == "before" else 1)


@pytest.mark.parametrize("fault", ["late_write", "broken_fk"])
def test_switch_checks_archived_late_writes_and_active_fk_after_exchange(simulation, fault):
    store, state, journal = simulation
    store.exchange_fault = fault
    with pytest.raises(ValueError):
        control.switch(store.conn, state, True, journal)
    assert control.read(journal)["phase"] == "switch_pending"
    assert store.schemas["ai_backup_test"] is not None


def test_completed_switch_retry_retains_legitimate_runtime_writes(simulation):
    store, state, journal = switched(simulation)
    store.schemas["ai_test"]["facts"] = facts(3)
    control.switch(store.conn, state, True, journal)
    assert store.schemas["ai_test"]["facts"] == facts(3)
    assert len(store.exchanges) == 1


@pytest.mark.parametrize("complete", [False, True])
def test_interrupted_copy_reuses_only_fully_equivalent_destination(simulation, complete):
    store, state, journal = simulation
    state["phase"] = "copy_pending"
    store.schemas[state["target"]]["facts"] = facts() if complete else facts(2)
    if complete:
        control.copy(store.conn, state, True, state["stopped_at"], journal)
        assert state["phase"] == "copied"
    else:
        with pytest.raises(ValueError, match="projection mismatch"):
            control.copy(store.conn, state, True, state["stopped_at"], journal)
        assert store.schemas[state["target"]]["facts"] == facts(2)
    assert not store.copies


def test_interrupted_copy_refuses_changed_frozen_source(simulation):
    store, state, journal = simulation
    state["phase"] = "copy_pending"
    store.schemas[state["source"]]["facts"] = facts(3)
    with pytest.raises(ValueError, match="source changed"):
        control.copy(store.conn, state, True, state["stopped_at"], journal)
    assert not store.copies


@pytest.mark.parametrize("fault", ["before", "after"])
def test_reverse_rollback_lost_reply_preserves_new_writes_and_is_retryable(simulation, fault):
    store, state, journal = switched(simulation)
    store.schemas[state["source"]]["facts"] = facts(3)
    store.exchange_fault = fault
    stopped = datetime.now(UTC).isoformat()
    with pytest.raises(ConnectionError):
        control.rollback(store.conn, state, True, stopped, journal=journal)
    state = control.read(journal)
    assert state["phase"] == "rollback_pending" and state["rollback_manifest"] == facts(3)
    copies, creates = list(store.copies), list(store.creates)
    control.rollback(store.conn, state, True, stopped, journal=journal)
    assert state["phase"] == "rolled_back"
    assert store.schemas[state["source"]]["head"] == OLD_HEAD
    assert store.schemas[state["source"]]["facts"] == facts(3)
    assert store.schemas[state["rollback_archive"]]["facts"] == facts(3)
    assert store.copies == copies and store.creates == creates


def test_fast_rollback_refuses_to_discard_new_runtime_data(simulation):
    store, state, journal = switched(simulation)
    store.schemas[state["source"]]["facts"] = facts(3)
    with pytest.raises(ValueError, match="discard new data"):
        control.rollback(store.conn, state, True, datetime.now(UTC).isoformat(), True, journal)
    assert store.schemas[state["source"]]["facts"] == facts(3)
    assert not store.creates


def test_fast_rollback_cleanup_handles_empty_moved_archive_and_retains_active_data(simulation):
    store, state, journal = switched(simulation)
    control.rollback(store.conn, state, True, datetime.now(UTC).isoformat(), True, journal)
    assert store.schemas[state["archive"]] is None
    assert len(state["retained_archives"]) == 2
    for record in state["retained_archives"]:
        record["retained_since"] = (datetime.now(UTC) - timedelta(days=31)).isoformat()
    control.cleanup(store.conn, state, journal)
    assert state["phase"] == "cleaned"
    assert state["archive"] not in store.schemas and state["rollback_archive"] not in store.schemas
    assert store.schemas[state["source"]]["facts"] == facts()


def test_cleanup_lost_drop_reply_resumes_only_its_recorded_pending_archive(simulation):
    store, state, journal = switched(simulation)
    state["retained_archives"][0]["retained_since"] = (
        datetime.now(UTC) - timedelta(days=31)
    ).isoformat()
    store.drop_fault = True
    with pytest.raises(ConnectionError):
        control.cleanup(store.conn, state, journal)
    state = control.read(journal)
    assert state["phase"] == "cleanup_pending" and state["retained_archives"][0]["drop_pending"]
    control.cleanup(store.conn, state, journal)
    assert state["phase"] == "cleaned" and store.schemas[state["source"]]["facts"] == facts()


def test_each_rollback_archive_gets_its_own_full_retention_window(simulation):
    store, state, journal = switched(simulation)
    control.rollback(store.conn, state, True, datetime.now(UTC).isoformat(), journal=journal)
    state["retained_archives"][0]["retained_since"] = (
        datetime.now(UTC) - timedelta(days=31)
    ).isoformat()
    with pytest.raises(ValueError, match="30-day"):
        control.cleanup(store.conn, state, journal)
    assert state["archive"] in store.schemas and state["rollback_archive"] in store.schemas


def test_pending_rollback_refuses_partial_committed_copy_instead_of_overwriting(simulation):
    store, state, journal = switched(simulation)
    store.schemas[state["source"]]["facts"] = facts(3)
    store.exchange_fault = "before"
    stopped = datetime.now(UTC).isoformat()
    with pytest.raises(ConnectionError):
        control.rollback(store.conn, state, True, stopped, journal=journal)
    state = control.read(journal)
    store.schemas[state["rollback_target"]]["facts"] = facts(2)
    with pytest.raises(ValueError, match="projection mismatch"):
        control.rollback(store.conn, state, True, stopped, journal=journal)
    assert store.schemas[state["source"]]["facts"] == facts(3)
    assert store.schemas[state["rollback_target"]]["facts"] == facts(2)


def test_deadline_and_original_stop_timestamp_cannot_be_extended(simulation):
    store, state, journal = simulation
    state["phase"] = "switch_pending"
    state["stopped_at"] = (datetime.now(UTC) - timedelta(minutes=26)).isoformat()
    with pytest.raises(TimeoutError):
        control.switch(store.conn, state, True, journal)
    assert not store.exchanges
    with pytest.raises(ValueError, match="original stopped-at"):
        control._same_stop(state, "stopped_at", datetime.now(UTC).isoformat())
    with pytest.raises(ValueError, match="time zone"):
        control.deadline(datetime.now().isoformat())
    with pytest.raises(TimeoutError):
        control.deadline((datetime.now(UTC) + timedelta(seconds=1)).isoformat())


def test_writer_fence_requires_operator_stop_process_visibility_and_no_remaining_pool(simulation):
    store, state, _ = simulation
    with pytest.raises(ValueError, match="Stop all writers"):
        control.fenced(store.conn, (state["source"],), False)
    store.grants = "GRANT SELECT ON ai_test.*"
    with pytest.raises(ValueError, match="PROCESS"):
        control.fenced(store.conn, (state["source"],), True)
    store.grants = "GRANT PROCESS ON *.*"
    store.connections[state["source"]] = 1
    with pytest.raises(ValueError, match="Other database connections"):
        control.fenced(store.conn, (state["source"],), True)


@pytest.fixture
def interrupted_ddl(monkeypatch):
    data = SimpleNamespace(
        tables={"alembic_version", "interpretation_sessions"},
        business_rows=0,
        observations=[],
        versions=[NEW_HEAD],
        extra_objects=0,
        external_fks=0,
        dropped=[],
        upgraded=[],
    )
    state = dict(source="ai_test", owned_schemas=["ai_refactor_test"])

    def execute(statement, parameters=None):
        sql = str(statement)
        if "information_schema.TABLES" in sql:
            return SimpleNamespace(all=lambda: [("InnoDB", "BASE TABLE")])
        if sql.startswith("SELECT version_num"):
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: data.versions))
        if "SELECT kind,recorded_count" in sql:
            return SimpleNamespace(all=lambda: data.observations)
        if sql.startswith("DROP DATABASE"):
            data.dropped.append("ai_refactor_test")
            return None
        raise AssertionError(sql)

    def scalar(statement, parameters=None):
        sql = str(statement)
        if "KEY_COLUMN_USAGE" in sql:
            return data.external_fks
        if "information_schema." in sql:
            return data.extra_objects
        if "SELECT COUNT(*)" in sql:
            return data.business_rows
        raise AssertionError(sql)

    def require(*args, **kwargs):
        raise ValueError("Interrupted DDL or collation FK alignment")

    conn = SimpleNamespace(execute=execute, scalar=scalar)
    monkeypatch.setattr(control, "_exists", lambda *args: True)
    monkeypatch.setattr(control, "require_schema", require)
    monkeypatch.setattr(control.contracts, "tables", lambda *args: data.tables)
    monkeypatch.setattr(
        control.contracts, "schema_options", lambda *args: ("utf8mb4", "utf8mb4_bin")
    )
    monkeypatch.setattr(
        control,
        "upgrade_isolated",
        lambda conn, target, head, source: data.upgraded.append((target, head, source)),
    )
    return data, conn, state


def test_interrupted_owned_empty_ddl_is_recreated_before_upgrade(interrupted_ddl):
    data, conn, state = interrupted_ddl
    control._resume_isolated(conn, state, "ai_refactor_test", NEW_HEAD, "ai_test")
    assert data.dropped == ["ai_refactor_test"]
    assert data.upgraded == [("ai_refactor_test", NEW_HEAD, "ai_test")]


@pytest.mark.parametrize(
    "corruption,reason",
    [
        ("ownership", "not owned"),
        ("table", "unknown tables"),
        ("business", "business data"),
        ("observation", "business data"),
        ("revision", "unknown migration evidence"),
        ("objects", "unknown database objects"),
        ("fk", "External foreign key"),
    ],
)
def test_interrupted_ddl_recreation_refuses_unknown_or_nonempty_data(
    interrupted_ddl, corruption, reason
):
    data, conn, state = interrupted_ddl
    if corruption == "ownership":
        state["owned_schemas"] = []
    elif corruption == "table":
        data.tables.add("unrelated_data")
    elif corruption == "business":
        data.business_rows = 1
    elif corruption == "observation":
        data.tables.add("messaging_observations")
        data.observations = [("duplicate_ack", 1, None)]
    elif corruption == "revision":
        data.versions = ["unknown_revision"]
    elif corruption == "objects":
        data.extra_objects = 1
    elif corruption == "fk":
        data.external_fks = 1
    with pytest.raises(ValueError, match=reason):
        control._resume_isolated(conn, state, "ai_refactor_test", NEW_HEAD, "ai_test")
    assert not data.dropped and not data.upgraded


def test_pristine_old_destination_validates_unbound_before_realigning_original_columns(
    interrupted_ddl, monkeypatch
):
    data, conn, state = interrupted_ddl
    calls = []
    monkeypatch.setattr(control, "require_schema", lambda *args, **kwargs: calls.append(kwargs))
    monkeypatch.setattr(control, "_pristine", lambda *args: True)
    control._resume_isolated(conn, state, "ai_refactor_test", OLD_HEAD, "ai_test")
    assert calls == [{"column_collations": {}}]
    assert not data.dropped and data.upgraded == [("ai_refactor_test", OLD_HEAD, "ai_test")]


@pytest.mark.integration
@pytest.mark.parametrize("damage", ["partial", "foreign_key", "unaligned", "nonempty"])
def test_native_owned_ddl_recovery_and_original_column_evidence(tmp_path, damage):
    url = os.environ.get("QS_AI_SCHEMA_TEST_SERVER")
    if not url:
        pytest.skip("Requires this run's disposable schema-refactor MySQL server")
    engine = sa.create_engine(url)
    suffix = uuid4().hex[:12]
    source, target = "ai_control_source_" + suffix, "ai_refactor_" + suffix

    def create_old(conn, schema):
        conn.execute(sa.text(f"USE {identifier(schema)}"))
        control.contracts.v0038.metadata.create_all(conn)
        conn.execute(
            sa.text(
                "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY) "
                "ENGINE=InnoDB"
            )
        )
        conn.execute(sa.text("INSERT INTO alembic_version VALUES (:head)"), {"head": OLD_HEAD})
        conn.commit()

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    sa.text(
                        f"CREATE DATABASE {identifier(source)} CHARACTER SET utf8mb4 "
                        "COLLATE utf8mb4_general_ci"
                    )
                )
                create_old(conn, source)
                columns = control.contracts.source_column_collations(conn, source)
                conn.info["source_column_collations"] = columns
                conn.execute(
                    sa.text(f"ALTER DATABASE {identifier(source)} COLLATE utf8mb4_0900_ai_ci")
                )
                control.contracts.create_schema(conn, target, source)
                if damage in ("foreign_key", "unaligned"):
                    create_old(conn, target)
                    if damage == "foreign_key":
                        name = sa.inspect(conn).get_foreign_keys(
                            "external_requests", schema=target
                        )[0]["name"]
                        conn.execute(
                            sa.text(
                                f"ALTER TABLE {qualified(target, 'external_requests')} "
                                f"DROP FOREIGN KEY {identifier(name)}"
                            )
                        )
                else:
                    conn.execute(
                        sa.text(
                            f"CREATE TABLE {qualified(target, 'interpretation_sessions')} "
                            "(marker INT NOT NULL) ENGINE=InnoDB"
                        )
                    )
                    if damage == "nonempty":
                        conn.execute(
                            sa.text(
                                f"INSERT INTO {qualified(target, 'interpretation_sessions')} "
                                "VALUES (1)"
                            )
                        )
                        conn.commit()
                conn.execute(sa.text(f"USE {identifier(source)}"))
                state = dict(source=source, owned_schemas=[target])
                if damage == "nonempty":
                    with pytest.raises(ValueError, match="business data"):
                        control._resume_isolated(conn, state, target, OLD_HEAD, source)
                    assert (
                        conn.scalar(
                            sa.text(
                                f"SELECT marker FROM {qualified(target, 'interpretation_sessions')}"
                            )
                        )
                        == 1
                    )
                else:
                    control._resume_isolated(conn, state, target, OLD_HEAD, source)
                    control.require_schema(conn, OLD_HEAD, target, column_collations=columns)
                    assert control.contracts.source_column_collations(conn, target) == columns
                control.require_schema(conn, OLD_HEAD, source, column_collations=columns)
            finally:
                conn.rollback()
                conn.execute(sa.text("USE mysql"))
                for schema in (target, source):
                    conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(schema)}"))
    finally:
        engine.dispose()
