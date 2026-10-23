"""Actual independent maintenance CLI against isolated, explicitly migrated MySQL."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

from tests.integration.test_transaction_schema import installed as installed

pytestmark = pytest.mark.integration


@pytest.fixture
async def binding_engine(installed):
    engine = installed.kw["bind"]
    # This empty fact table belongs to the adapter fixture, not the runtime schema.
    async with engine.begin() as conn:
        await conn.execute(sa.text("DROP TABLE m3_install_fact"))
    return engine


async def invoke(engine, path: Path, action: str, *extras: str):
    source = (
        await asyncio.to_thread(subprocess.check_output, ["git", "rev-parse", "HEAD"], text=True)
    ).strip()
    env = dict(
        os.environ,
        QS_AI_RELEASE_SHA=source,
        QS_AI_DATABASE_URL=engine.url.render_as_string(hide_password=False),
    )
    command = [
        sys.executable,
        "-m",
        "qs_ai.maintenance.transaction_binding",
        "--action",
        action,
        "--plan",
        str(path),
        *extras,
    ]
    process = await asyncio.create_subprocess_exec(
        *command, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 45)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
    return process.returncode, json.loads(stdout) if stdout else {}, stderr.decode()


async def preview(engine, path):
    result = await invoke(
        engine,
        path,
        "preview",
        "--operation-id",
        "owned-ci-operation",
        "--store-id",
        "owned-original-primary",
        "--writer-epoch",
        "m3-1",
    )
    assert result[0] == 0 and result[1]["read_only"] is True
    assert result[1]["cutover_authorized"] is False
    assert path.stat().st_mode & 0o777 == 0o600
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def test_initial_binding_cli_preserves_old_responsibility_and_only_installs_reviewed_root(
    binding_engine, tmp_path
):
    engine = binding_engine
    async with engine.connect() as conn:
        before = await conn.scalar(
            sa.text(
                "SELECT SHA2(CONCAT_WS('|',stage,attempts,error_code,HEX(wire)),256) "
                "FROM messaging_outbox WHERE message_id='old'"
            )
        )
    path = tmp_path / "review.json"
    approved = await preview(engine, path)
    refused = await invoke(engine, path, "apply", "--reviewed-sha256", approved)
    assert refused[0] != 0  # no writer exclusion attestation
    applied = await invoke(
        engine, path, "apply", "--reviewed-sha256", approved, "--writers-excluded"
    )
    assert applied[0] == 0 and applied[1]["completed"] is True
    assert applied[1]["read_only"] is False and applied[1]["cutover_authorized"] is False
    verified = await invoke(engine, path, "verify", "--reviewed-sha256", approved)
    assert verified[0] == 0 and verified[1]["read_only"] is True
    repeated = await invoke(
        engine, path, "apply", "--reviewed-sha256", approved, "--writers-excluded"
    )
    assert repeated[0] != 0
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                sa.text("SELECT owner,store_id,writer_epoch,schema_version FROM rm_tx_binding")
            )
        ).one()
        assert row == (b"ai.mysql", b"owned-original-primary", b"m3-1", 1)
        after = await conn.scalar(
            sa.text(
                "SELECT SHA2(CONCAT_WS('|',stage,attempts,error_code,HEX(wire)),256) "
                "FROM messaging_outbox WHERE message_id='old'"
            )
        )
        assert before == after
        for table in (
            "rm_tx_request",
            "rm_tx_execution",
            "rm_tx_witness",
            "messaging_archive",
            "messaging_event_receipts",
            "messaging_native_limit_notices",
        ):
            assert await conn.scalar(sa.text(f"SELECT COUNT(*) FROM `{table}`")) == 0


async def test_initial_binding_rejects_actual_new_protocol_responsibility(binding_engine, tmp_path):
    engine = binding_engine
    path = tmp_path / "review.json"
    approved = await preview(engine, path)
    async with engine.begin() as conn:
        await conn.execute(
            sa.text("INSERT INTO rm_tx_request VALUES(:key,:owner,:scope,:idem,:hash,:execution)"),
            {
                "key": b"r" * 32,
                "owner": b"ai.mysql",
                "scope": b"owned",
                "idem": b"new",
                "hash": b"h" * 32,
                "execution": b"e" * 32,
            },
        )
    refused = await invoke(
        engine, path, "apply", "--reviewed-sha256", approved, "--writers-excluded"
    )
    assert refused[0] != 0
    async with engine.connect() as conn:
        assert await conn.scalar(sa.text("SELECT COUNT(*) FROM rm_tx_binding")) == 0
        assert await conn.scalar(sa.text("SELECT COUNT(*) FROM rm_tx_request")) == 1


async def test_initial_binding_refuses_reviewed_primary_identity_mismatch(binding_engine, tmp_path):
    engine = binding_engine
    path = tmp_path / "review.json"
    await preview(engine, path)
    plan = json.loads(path.read_bytes())
    plan["identity_sha256"] = "f" * 64
    path.write_text(json.dumps(plan))
    approved = hashlib.sha256(path.read_bytes()).hexdigest()
    refused = await invoke(
        engine, path, "apply", "--reviewed-sha256", approved, "--writers-excluded"
    )
    assert refused[0] != 0
    async with engine.connect() as conn:
        assert await conn.scalar(sa.text("SELECT COUNT(*) FROM rm_tx_binding")) == 0


async def test_initial_binding_two_real_cli_installers_choose_one_original_winner(
    binding_engine, tmp_path
):
    engine = binding_engine
    first = tmp_path / "first.json"
    first_digest = await preview(engine, first)
    other = tmp_path / "other.json"
    plan = json.loads(first.read_bytes())
    plan["operation_id"] = "other-owned-ci-operation"
    plan["store_id"] = "other-reviewed-primary-label"
    from qs_ai.maintenance.transaction_binding import write_plan

    other_digest = write_plan(str(other), plan)
    results = await asyncio.gather(
        invoke(engine, first, "apply", "--reviewed-sha256", first_digest, "--writers-excluded"),
        invoke(engine, other, "apply", "--reviewed-sha256", other_digest, "--writers-excluded"),
    )
    assert sum(result[0] == 0 for result in results) == 1
    async with engine.connect() as conn:
        rows = (await conn.execute(sa.text("SELECT owner,store_id FROM rm_tx_binding"))).all()
        assert len(rows) == 1 and rows[0][0] == b"ai.mysql"
        assert rows[0][1] in (b"owned-original-primary", b"other-reviewed-primary-label")
        for table in ("rm_tx_request", "rm_tx_execution", "rm_tx_witness"):
            assert await conn.scalar(sa.text(f"SELECT COUNT(*) FROM `{table}`")) == 0
