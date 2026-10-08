"""Only disposable UUID schemas; real native backup, locks and retirement transactions."""

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
from alembic import command
from alembic.config import Config

from qs_ai.maintenance.prompt_retirement import policy, service, snapshot
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier, qualified
from tests.integration.test_schema_refactor_conversion import cloned as cloned

pytestmark = pytest.mark.integration
REVISION = "1" * 40
IMAGE = "sha256:" + "2" * 64
EXECUTION = {"revision": REVISION, "image_id": IMAGE}


def add_prompts(conn, source):
    for version in policy.VERSIONS:
        body = (
            '{ "Ref": {"TemplateID": '
            + json.dumps(policy.TEMPLATE_ID)
            + ', "Version": '
            + json.dumps(version)
            + '}, "number":1.00 }\r\n'
        )
        conn.execute(
            sa.text(
                f"INSERT INTO {qualified(source, 'prompt_assets')} "
                "(template_id,version,fingerprint,package_sha256,package_json,"
                "source_ref,imported_by,created_at) "
                "VALUES (:id,:version,:fingerprint,:hash,:body,'fixture','fixture',NULL)"
            ),
            {
                "id": policy.TEMPLATE_ID,
                "version": version,
                "fingerprint": "sha256:" + hashlib.sha256(version.encode()).hexdigest(),
                "hash": hashlib.sha256(body.encode()).hexdigest(),
                "body": body,
            },
        )
    conn.commit()


def migrate(conn, state, journal, head):
    if head == NEW_HEAD:
        control.prepare(conn, state, journal)
        control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
        control.verified(conn, state, True)
        control.switch(conn, state, True, journal)
        conn.commit()
    conn.rollback()


def files(tmp_path):
    return {
        key: tmp_path / filename
        for key, filename in (
            ("plan", "plan.json"),
            ("backup", "backup.jsonl.gz"),
            ("receipt", "restore.json"),
            ("journal", "application.json"),
        )
    }


def proof(conn, source, tmp_path):
    paths = files(tmp_path)
    service.plan(conn, source, paths["plan"], **EXECUTION)
    target = "ai_refactor_prompt_" + uuid4().hex[:12]
    result = service.backup(
        conn, source, paths["plan"], paths["backup"], paths["receipt"], target, **EXECUTION
    )
    assert result["restore_verified"] and not result["restore_target_retained"]
    assert not conn.scalar(
        sa.text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:name"),
        {"name": target},
    )
    conn.rollback()
    return paths


def test_full_native_proof_and_exact_prompt_retirement(cloned, tmp_path):
    head = NEW_HEAD
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    # Referenced v1 must survive; the seeded published v6 remains protected.
    reference = json.dumps({"prompt_template_id": policy.TEMPLATE_ID, "prompt_version": "v1"})
    conn.execute(
        sa.text(f"UPDATE {qualified(source, 'profile_assets')} SET definition_json=:body"),
        {"body": reference},
    )
    conn.commit()
    migrate(conn, state, journal, head)
    paths = proof(conn, source, tmp_path)
    value = service.load_plan(paths["plan"], source, **EXECUTION)
    assert [row["version"] for row in value["inventory"]["candidates"]] == ["v2", "v3", "v4", "v5"]
    assert value["inventory"]["retained"][0]["version"] == "v1"
    if head == NEW_HEAD:
        assert len(value["inventory"]["physical_manifest"]) == 44
        assert not any(fk["prompt_reference"] for fk in value["inventory"]["foreign_keys"])
    assert service.verify(
        conn,
        source,
        paths["plan"],
        **EXECUTION,
        backup_path=paths["backup"],
        receipt_path=paths["receipt"],
    )["verified"]
    result = service.apply(
        conn,
        source,
        paths["plan"],
        paths["backup"],
        paths["receipt"],
        paths["journal"],
        **EXECUTION,
        writers_stopped=True,
        stopped_at=datetime.now(UTC).isoformat(),
    )
    assert result["status"] == "committed"
    assert (
        service.verify(conn, source, paths["plan"], **EXECUTION, journal_path=paths["journal"])[
            "status"
        ]
        == "committed"
    )
    assert (
        service.apply(
            conn,
            source,
            paths["plan"],
            paths["backup"],
            paths["receipt"],
            paths["journal"],
            **EXECUTION,
            writers_stopped=False,
            stopped_at="2000-01-01T00:00:00+00:00",
        )["status"]
        == "committed"
    )


@pytest.mark.parametrize("head", [OLD_HEAD, "0039_data_consolidation"])
def test_new_format3_rejects_old_and_intermediate_source(cloned, tmp_path, head):
    conn, state, _ = cloned
    if head != OLD_HEAD:
        config = Config("alembic.ini")
        config.attributes["connection"] = conn
        command.upgrade(config, head)
        conn.commit()
    conn.rollback()
    path = tmp_path / "rejected.json"
    with pytest.raises(ValueError, match="0040"):
        service.plan(conn, state["source"], path, **EXECUTION)
    assert not path.exists() and not conn.in_transaction()


@pytest.mark.parametrize(
    "kind", ["profile", "route", "schema", "execution_policy", "gate_policy", "semantic_prompt"]
)
def test_other_asset_kind_body_references_retain_prompt(cloned, tmp_path, kind):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    body = json.dumps({"proof": {"template_id": policy.TEMPLATE_ID, "version": "v1"}}).encode()
    body_format = "semantic_markdown" if kind == "semantic_prompt" else "definition_json"
    conn.execute(
        sa.text(
            f"INSERT INTO {qualified(source, 'governance_asset_versions')} "
            "(asset_kind,owner_organization_id,asset_id,version,fingerprint,body_format,"
            "body_bytes,source_ref,imported_by) VALUES (:kind,0,'ref-proof','v1',:fp,"
            ":format,:body,'fixture','fixture')"
        ),
        {"kind": kind, "fp": "sha256:" + "a" * 64, "format": body_format, "body": body},
    )
    conn.commit()
    path = tmp_path / "plan.json"
    service.plan(conn, source, path, **EXECUTION)
    value = service.load_plan(path, source, **EXECUTION)
    assert "v1" not in {row["version"] for row in value["inventory"]["candidates"]}
    assert "v1" in {row["version"] for row in value["inventory"]["retained"]}


def test_unknown_references_and_cross_schema_cascade_are_retained(cloned, tmp_path):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    child = "qs_ai_prompt_fk_" + uuid4().hex[:12]
    conn.execute(sa.text(f"CREATE DATABASE {identifier(child)}"))
    try:
        conn.execute(
            sa.text(
                f"CREATE TABLE {qualified(child, 'references_prompt')} "
                "(id BIGINT UNSIGNED PRIMARY KEY, "
                f"FOREIGN KEY(id) REFERENCES {qualified(source, 'governance_asset_versions')}"
                "(asset_row_id) ON DELETE CASCADE)"
            )
        )
        conn.commit()
        path = tmp_path / "plan.json"
        result = service.plan(conn, source, path, **EXECUTION)
        assert result["candidates"] == 0 and result["retained"] == 5
        value = service.load_plan(path, source, **EXECUTION)
        assert any(
            row["schema"] == child and row["prompt_reference"]
            for row in value["inventory"]["foreign_keys"]
        )
        # Full native backup retains its original stricter exchange gate. A zero-
        # candidate foreign-FK plan is useful evidence, never permission to delete.
        with pytest.raises(ValueError, match="External foreign key"):
            service.backup(
                conn,
                source,
                path,
                tmp_path / "blocked.gz",
                tmp_path / "proof.json",
                "ai_refactor_prompt_" + uuid4().hex[:12],
                **EXECUTION,
            )
        assert not (tmp_path / "blocked.gz").exists()
        assert not (tmp_path / "proof.json").exists()
    finally:
        conn.rollback()
        conn.execute(sa.text(f"DROP DATABASE {identifier(child)}"))


def test_drift_and_second_delete_failure_leave_all_prompts(cloned, tmp_path, monkeypatch):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    paths = proof(conn, source, tmp_path)
    original = service._delete
    count = 0

    def fail_second(connection, schema, head, row):
        nonlocal count
        count += 1
        if count == 2:
            raise ValueError("synthetic second delete failure")
        original(connection, schema, head, row)

    monkeypatch.setattr(service, "_delete", fail_second)
    with pytest.raises(ValueError, match="second delete"):
        service.apply(
            conn,
            source,
            paths["plan"],
            paths["backup"],
            paths["receipt"],
            paths["journal"],
            **EXECUTION,
            writers_stopped=True,
            stopped_at=datetime.now(UTC).isoformat(),
        )
    value = service.load_plan(paths["plan"], source, **EXECUTION)
    assert service.classify(conn, value) == "not_applied"
    assert count == 2
    retry = service.apply(
        conn,
        source,
        paths["plan"],
        paths["backup"],
        paths["receipt"],
        paths["journal"],
        **EXECUTION,
        writers_stopped=True,
        stopped_at=datetime.now(UTC).isoformat(),
    )
    assert retry["status"] == "not_applied" and count == 2
    conn.execute(
        sa.text(
            f"UPDATE {qualified(source, 'governance_asset_versions')} SET source_ref='changed' "
            "WHERE asset_kind='prompt' AND version='v1'"
        )
    )
    conn.commit()
    assert service.classify(conn, value) == "unknown"


async def test_native_cli_uses_asyncmy_and_only_safe_metadata(cloned, tmp_path):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    url = sa.engine.make_url(os.environ["QS_AI_SCHEMA_TEST_SERVER"]).set(drivername="mysql+asyncmy")
    path = tmp_path / "cli-plan.json"

    async def command_cli(operation, *extra):
        process = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                "-m",
                "qs_ai.maintenance.prompt_retirement",
                operation,
                "--schema",
                source,
                "--plan",
                str(path),
                "--revision",
                REVISION,
                "--image-id",
                IMAGE,
                *extra,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "QS_AI_DATABASE_URL": url.render_as_string(hide_password=False)},
        )
        assert process.returncode == 0, process.stdout + process.stderr
        result = json.loads(process.stdout)
        assert result["prompt_retirement"] == "ok" and result["candidates"] == 5
        assert not process.stderr
        assert "package_json" not in process.stdout and "body_bytes" not in process.stdout
        assert "fixture" not in process.stdout
        return result

    await command_cli("plan")
    backup_path, receipt_path = tmp_path / "cli.gz", tmp_path / "cli-restore.json"
    restored = "ai_refactor_prompt_" + uuid4().hex[:12]
    proved = await command_cli(
        "backup",
        "--backup",
        str(backup_path),
        "--receipt",
        str(receipt_path),
        "--target",
        "ai_refactor_prompt_" + uuid4().hex[:12],
    )
    assert proved["restore_verified"] and not proved["restore_target_retained"]
    await command_cli("verify", "--backup", str(backup_path), "--receipt", str(receipt_path))
    try:
        result = await command_cli("restore", "--backup", str(backup_path), "--target", restored)
        assert result["restore_verified"] and result["restore_target_retained"]
    finally:
        conn.rollback()
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(restored)}"))


def test_incomplete_metadata_visibility_fails_before_plan_publication(
    cloned, tmp_path, monkeypatch
):
    conn, state, journal = cloned
    migrate(conn, state, journal, NEW_HEAD)
    original = snapshot.native.preflight

    def limited(*args, **kwargs):
        result = original(*args, **kwargs)
        result["cross_schema_fk_visibility"] = "schema_scoped_requires_admin_evidence"
        return result

    monkeypatch.setattr(snapshot.native, "preflight", limited)
    path = tmp_path / "blocked.json"
    with pytest.raises(ValueError, match="visibility"):
        service.plan(conn, state["source"], path, **EXECUTION)
    assert not path.exists() and not conn.in_transaction()


def test_backup_restore_preserves_raw_bytes_and_generated_keys(cloned, tmp_path):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    paths = proof(conn, source, tmp_path)
    value = service.load_plan(paths["plan"], source, **EXECUTION)
    with fmt.private_file(paths["backup"]) as stream:
        records = list(fmt.records(stream))
    header = records[0]
    assert "native_id_key" in header["tables"]["governance_asset_versions"]["generated"]
    assert "native_id_key" not in header["tables"]["governance_asset_versions"]["columns"]
    assert len(value["inventory"]["protected_manifest"]) == 44
    assert contracts.head(conn, source) == NEW_HEAD


@pytest.mark.parametrize("boundary", ["before", "after"])
def test_real_commit_disconnect_is_only_classified_never_reapplied(
    cloned, tmp_path, monkeypatch, boundary
):
    conn, state, journal = cloned
    source = state["source"]
    add_prompts(conn, source)
    migrate(conn, state, journal, NEW_HEAD)
    paths = proof(conn, source, tmp_path)
    original_commit = sa.engine.Connection.commit
    original_delete = service._delete
    deletes = []

    def record_delete(*args):
        deletes.append(args[-1]["version"])
        return original_delete(*args)

    def disconnect(connection):
        if (
            connection is conn
            and paths["journal"].exists()
            and json.loads(paths["journal"].read_text())["phase"] == "commit_pending"
        ):
            if boundary == "after":
                original_commit(connection)
            connection.invalidate()
            raise ConnectionError("synthetic lost COMMIT reply")
        return original_commit(connection)

    monkeypatch.setattr(service, "_delete", record_delete)
    monkeypatch.setattr(sa.engine.Connection, "commit", disconnect)
    with pytest.raises(ConnectionError, match="COMMIT"):
        service.apply(
            conn,
            source,
            paths["plan"],
            paths["backup"],
            paths["receipt"],
            paths["journal"],
            **EXECUTION,
            writers_stopped=True,
            stopped_at=datetime.now(UTC).isoformat(),
        )
    assert json.loads(paths["journal"].read_text())["phase"] == "commit_pending"
    assert len(deletes) == 5
    with conn.engine.connect() as fresh:
        expected = "committed" if boundary == "after" else "not_applied"
        result = service.apply(
            fresh,
            source,
            paths["plan"],
            paths["backup"],
            paths["receipt"],
            paths["journal"],
            **EXECUTION,
            writers_stopped=False,
            stopped_at="2000-01-01T00:00:00+00:00",
        )
        assert result["status"] == expected and len(deletes) == 5
