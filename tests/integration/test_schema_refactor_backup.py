"""Real 54/44-table private native-byte backup and restore on disposable MySQL."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa

from qs_ai.maintenance.schema_refactor import backup, contracts, control
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor.conversion import manifest
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier, qualified
from qs_ai.maintenance.schema_refactor.validation import require_schema
from tests.integration.test_schema_refactor_conversion import cloned as cloned

pytestmark = pytest.mark.integration


def target_name():
    return "ai_refactor_rehearsal_" + uuid4().hex[:12]


def to_new(conn, state, journal):
    control.prepare(conn, state, journal)
    control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
    control.verified(conn, state, True)
    control.save(journal, state)
    control.switch(conn, state, True, journal)


def set_extra_facts(conn, source):
    conn.execute(
        sa.text(
            f"UPDATE {qualified(source, 'evaluation_checkpoints')} "
            "SET checkpoint_json=CAST('null' AS JSON)"
        )
    )
    conn.execute(
        sa.text(
            f"INSERT INTO {qualified(source, 'evaluation_checkpoints')} "
            "(run_id,version,checkpoint_json) "
            "VALUES ('00000000-0000-0000-0000-000000000002',2,NULL)"
        )
    )
    conn.execute(
        sa.text(
            f"UPDATE {qualified(source, 'route_assets')} "
            "SET route='CaseTail ',source_ref='原始😀  '"
        )
    )
    for table, floor in (
        ("evaluation_admission_locks", 424242),
        ("participant_admission_locks", 99999),
    ):
        conn.execute(sa.text(f"ALTER TABLE {qualified(source, table)} AUTO_INCREMENT={floor}"))
    conn.commit()


@pytest.mark.parametrize("head", [OLD_HEAD, NEW_HEAD])
async def test_every_original_column_and_native_null_bytes_survive_independent_restore(
    cloned, tmp_path, head
):
    conn, state, journal = cloned
    source = state["source"]
    set_extra_facts(conn, source)
    # Source table columns retain their real old default after the database default changes.
    conn.execute(sa.text(f"ALTER DATABASE {identifier(source)} COLLATE utf8mb4_general_ci"))
    if head == NEW_HEAD:
        to_new(conn, state, journal)
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'governance_asset_versions')} "
                "AUTO_INCREMENT=7654321"
            )
        )
    before = manifest(conn, source, head)
    conn.commit()
    path, target = tmp_path / "ai.jsonl.gz", target_name()
    receipt = backup.backup(conn, source, path, expected_head=head)
    assert receipt["manifest_sha256"] == fmt.digest(before)
    assert receipt["tables"] == (54 if head == OLD_HEAD else 44)
    assert path.stat().st_mode & 0o777 == 0o600
    assert fmt.receipt_path(path).stat().st_mode & 0o777 == 0o600
    assert backup.inspect_backup(source, path, expected_sha256=receipt["backup_sha256"])["verified"]
    assert b"original" not in fmt.receipt_path(path).read_bytes()
    try:
        restored = backup.restore(conn, source, path, target, receipt["backup_sha256"])
        assert restored["verified"]
        assert restored["physical_manifest_sha256"] == receipt["physical_manifest_sha256"]
        assert restored["source_server_uuid"] == restored["server_uuid"]
        require_schema(conn, head, target, column_collations={})
        assert manifest(conn, target, head) == before
        assert manifest(conn, source, head) == before
        assert contracts.schema_options(conn, target)[1] == "utf8mb4_general_ci"
        values = conn.execute(
            sa.text(
                "SELECT checkpoint_json IS NULL,CAST(checkpoint_json AS BINARY) "
                f"FROM {qualified(target, 'evaluation_checkpoints')} ORDER BY run_id"
            )
        ).all()
        assert values == [(0, b"null"), (1, None)]
        table = "route_assets" if head == OLD_HEAD else "governance_asset_versions"
        field = "route" if head == OLD_HEAD else "asset_id"
        predicate = "1=1" if head == OLD_HEAD else "asset_kind='route'"
        assert (
            conn.scalar(
                sa.text(
                    f"SELECT CAST({identifier(field)} AS BINARY) "
                    f"FROM {qualified(target, table)} WHERE {predicate}"
                )
            )
            == b"CaseTail "
        )
    finally:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(target)}"))


async def test_live_read_only_snapshot_remains_consistent_during_another_writer(
    cloned, tmp_path, monkeypatch
):
    conn, state, _ = cloned
    source, target = state["source"], target_name()
    before = manifest(conn, source, OLD_HEAD)
    conn.commit()
    original, changed = backup._native_rows, False

    def rows(connection, schema, name, table, spec):
        nonlocal changed
        if not changed:
            changed = True
            with conn.engine.begin() as writer:
                writer.execute(
                    sa.text(f"UPDATE {qualified(source, 'evaluation_checkpoints')} SET version=99")
                )
        yield from original(connection, schema, name, table, spec)

    monkeypatch.setattr(backup, "_native_rows", rows)
    path = tmp_path / "live.jsonl.gz"
    receipt = backup.backup(conn, source, path)
    assert receipt["manifest_sha256"] == fmt.digest(before)
    assert manifest(conn, source, OLD_HEAD) != before
    conn.commit()
    try:
        backup.restore(conn, source, path, target, receipt["backup_sha256"])
        assert manifest(conn, target, OLD_HEAD) == before
        assert (
            conn.scalar(
                sa.text(f"SELECT version FROM {qualified(source, 'evaluation_checkpoints')}")
            )
            == 99
        )
    finally:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(target)}"))


async def test_preflight_has_no_ddl_or_business_writes_and_cli_redacts_credentials(
    cloned, tmp_path
):
    conn, state, _ = cloned
    source, target = state["source"], target_name()
    before = manifest(conn, source, OLD_HEAD)
    schemas = set(conn.execute(sa.text("SHOW DATABASES")).scalars())
    result = backup.preflight(conn, source, target)
    assert result["ok"] and result["capabilities"]["process"]
    assert result["cross_schema_fk_visibility"] == "all_schemas"
    assert result["permission_check"] == "grant_metadata_only_no_database_created"
    assert set(conn.execute(sa.text("SHOW DATABASES")).scalars()) == schemas
    assert manifest(conn, source, OLD_HEAD) == before
    command = [
        sys.executable,
        "-m",
        "qs_ai.maintenance.schema_refactor.backup",
        "preflight",
        "--schema",
        source,
    ]
    actual = await asyncio.to_thread(
        subprocess.run,
        command,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "QS_AI_DATABASE_URL": "mysql+asyncmy://secret-user:DO_NOT_PRINT_PRIVATE_PASSWORD@127.0.0.1:1/mysql",
        },
    )
    assert actual.returncode == 1
    assert "DO_NOT_PRINT" not in actual.stdout + actual.stderr
    assert json.loads(actual.stdout)["preflight"] == "failed"


async def test_bad_source_or_file_checksum_never_creates_a_restore_database(cloned, tmp_path):
    conn, state, _ = cloned
    source, target = state["source"], target_name()
    conn.commit()
    path = tmp_path / "original.jsonl.gz"
    receipt = backup.backup(conn, source, path)
    with pytest.raises(ValueError, match="checksum"):
        backup.restore(conn, source, path, target, "0" * 64)
    with pytest.raises(ValueError, match="source schema"):
        backup.restore(conn, "ai", path, target, receipt["backup_sha256"])
    with pytest.raises(ValueError, match="another source server"):
        backup.restore(
            conn,
            source,
            path,
            target,
            receipt["backup_sha256"],
            expected_server_uuid="00000000-0000-0000-0000-000000000000",
        )
    assert not conn.scalar(
        sa.text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:schema"),
        {"schema": target},
    )
    conn.commit()
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="checksum"):
        backup.restore(conn, source, path, target, receipt["backup_sha256"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() != receipt["backup_sha256"]


async def test_non_mvcc_auto_increment_changes_abort_before_artifact_publication(
    cloned, tmp_path, monkeypatch
):
    conn, state, _ = cloned
    source = state["source"]
    conn.commit()
    original, changed = backup._native_rows, False

    def rows(connection, schema, name, table, spec):
        nonlocal changed
        if not changed:
            changed = True
            with conn.engine.begin() as writer:
                writer.execute(
                    sa.text(
                        f"INSERT INTO {qualified(source, 'evaluation_admission_locks')} "
                        "(organization_id) VALUES (2)"
                    )
                )
        yield from original(connection, schema, name, table, spec)

    monkeypatch.setattr(backup, "_native_rows", rows)
    path = tmp_path / "changed-counter.jsonl.gz"
    with pytest.raises(ValueError, match="auto increment evidence changed"):
        backup.backup(conn, source, path)
    assert not path.exists() and not fmt.receipt_path(path).exists()
    assert not list(tmp_path.glob(".backup-*"))


async def test_real_asyncmy_cli_only_returns_safe_metadata_and_proves_restore(cloned, tmp_path):
    conn, state, _ = cloned
    source, target = state["source"], target_name()
    conn.commit()
    path = tmp_path / "cli.jsonl.gz"
    url = sa.engine.make_url(os.environ["QS_AI_SCHEMA_TEST_SERVER"]).set(drivername="mysql+asyncmy")

    async def command(name, *arguments):
        process = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                "-m",
                "qs_ai.maintenance.schema_refactor.backup",
                name,
                "--schema",
                source,
                *arguments,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "QS_AI_DATABASE_URL": url.render_as_string(hide_password=False)},
        )
        assert process.returncode == 0, process.stdout + process.stderr
        value = json.loads(process.stdout)
        assert not process.stderr
        assert "original" not in process.stdout and "local_root" not in process.stdout
        return value

    proof = await command("preflight", "--target", target)
    assert proof["ok"] and all(proof["capabilities"].values())
    receipt = await command("backup", "--path", str(path), "--expected-head", OLD_HEAD)
    try:
        inspected = await command(
            "inspect", "--path", str(path), "--expected-sha256", receipt["backup_sha256"]
        )
        assert inspected["verified"]
        restored = await command(
            "restore",
            "--path",
            str(path),
            "--target",
            target,
            "--expected-sha256",
            receipt["backup_sha256"],
            "--expected-server-uuid",
            receipt["source_server_uuid"],
        )
        assert restored["verified"]
        assert manifest(conn, target, OLD_HEAD) == manifest(conn, source, OLD_HEAD)
    finally:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(target)}"))


async def test_archived_show_create_is_evidence_and_is_never_executed(cloned, tmp_path):
    conn, state, _ = cloned
    source, target = state["source"], target_name()
    conn.commit()
    original = tmp_path / "original.jsonl.gz"
    receipt = backup.backup(conn, source, original)
    with fmt.private_file(original) as stream:
        records = list(fmt.records(stream))
    header, footer = records[0], records[-1]
    table = header["tables"]["route_assets"]
    # Even a separately re-signed archive cannot inject SQL into the restore path.
    table["create_sql"] = f"DROP DATABASE {identifier(source)}; CREATE TABLE injected(id INT)"
    table["ddl_sha256"] = hashlib.sha256(table["create_sql"].encode()).hexdigest()
    untrusted = tmp_path / "untrusted.jsonl.gz"
    import gzip

    with fmt.unpublished(untrusted) as (_, raw):
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
            for record in records:
                fmt.emit(compressed, record)
        raw.flush()
        checksum = fmt.checksum(raw)
        size = os.fstat(raw.fileno()).st_size
    altered = {**backup._summary(header, footer, checksum), "bytes": size}
    fmt.write_private(fmt.receipt_path(untrusted), altered)
    try:
        result = backup.restore(conn, source, untrusted, target, checksum)
        assert result["verified"]
        assert manifest(conn, source, OLD_HEAD) == manifest(conn, target, OLD_HEAD)
        assert not conn.scalar(
            sa.text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME='injected'"
            ),
            {"schema": target},
        )
        assert receipt["manifest_sha256"] == result["manifest_sha256"]
    finally:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(target)}"))
