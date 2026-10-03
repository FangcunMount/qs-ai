"""Reviewed maintenance boundaries on disposable storage, without a running Relay."""

import asyncio
import copy
import json
import os
import sys
from uuid import uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError

from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.maintenance import legacy_results as tool
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import mq_env as mq_env
from tests.integration.test_mq_admission import saved
from tests.integration.test_mq_legacy_handoff import legacy as legacy
from tests.integration.test_mq_storage import keys as keys
from tests.test_input_binding import bound_case

pytestmark = pytest.mark.integration


async def test_handoff_dry_run_is_storage_read_only_and_retains_host_pool(legacy, monkeypatch):
    tx, event_id, recorder, _ = legacy
    before = await saved(tx, result_outbox)
    original_header = tool.header

    async def read_only(db):
        value = await original_header(db)
        with pytest.raises(DBAPIError) as error:
            await db.execute(update(result_outbox).values(attempts=7))
        assert error.value.orig.args[0] == 1792
        return value

    monkeypatch.setattr(tool, "header", read_only)
    manifest = await tool.ResultHandoff(tx, recorder).dry_run([event_id])
    assert tool.validate_manifest(manifest, manifest["digest"]) == manifest
    assert await saved(tx, result_outbox) == before
    assert not await saved(tx, outbox)
    assert "payload" not in manifest["rows"][0]
    async with tx.open() as db:
        assert await db.scalar(select(result_outbox.c.event_id)) == event_id


async def test_reviewed_source_drift_stops_before_transfer(legacy):
    tx, event_id, recorder, _ = legacy
    service = tool.ResultHandoff(tx, recorder)
    manifest = await service.dry_run([event_id])
    async with tx.open() as db:
        await db.execute(
            update(result_outbox).where(result_outbox.c.event_id == event_id).values(attempts=3)
        )
        await db.commit()
    with pytest.raises(tool.ApplyError) as error:
        await service.apply(
            manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
        )
    assert error.value.result["rows"] == [{"event_id": event_id, "status": "stopped"}]
    assert not (await saved(tx, result_outbox))[0]["mq_owned"]
    assert not await saved(tx, outbox)


async def test_apply_preserves_exhausted_budget_and_repeat_never_seals(legacy, monkeypatch):
    tx, event_id, recorder, _ = legacy
    async with tx.open() as db:
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.event_id == event_id)
            .values(attempts=10, created_at=None)
        )
        await db.commit()
    service = tool.ResultHandoff(tx, recorder)
    manifest = await service.dry_run([event_id])
    first = await service.apply(
        manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
    )
    wire = (await saved(tx, outbox))[0]
    assert wire["stage"] == "held" and wire["attempts"] == 10
    assert manifest["rows"][0]["source"]["created_at"] is None
    assert first["rows"][0]["status"] == "transferred"
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("replay cannot seal"),
    )
    repeated = await service.apply(
        manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
    )
    assert repeated["rows"][0]["status"] == "retained"
    assert (await saved(tx, outbox))[0] == wire


async def test_partial_apply_retains_first_commit_and_stops_on_next_drift(legacy, kit, monkeypatch):
    tx, event_id, recorder, _ = legacy
    before_ids = {r["event_id"] for r in await saved(tx, result_outbox)}
    tx.database.state_events = None
    try:
        await kit.service.start_external(
            kit.actor, "7", ("42",), "另一原身份", str(uuid4()), bound_case()[1].items
        )
    finally:
        tx.database.state_events = recorder
    second_id = next(
        r["event_id"] for r in await saved(tx, result_outbox) if r["event_id"] not in before_ids
    )
    service = tool.ResultHandoff(tx, recorder)
    manifest = await service.dry_run([event_id, second_id])
    async with tx.open() as db:
        await db.execute(
            update(result_outbox).where(result_outbox.c.event_id == second_id).values(attempts=3)
        )
        await db.commit()
    with pytest.raises(tool.ApplyError) as first:
        await service.apply(
            manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
        )
    assert [r["status"] for r in first.value.result["rows"]] == ["transferred", "stopped"]
    wire = (await saved(tx, outbox))[0]
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("partial replay cannot reseal"),
    )
    with pytest.raises(tool.ApplyError) as second:
        await service.apply(
            manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
        )
    assert [r["status"] for r in second.value.result["rows"]] == ["retained", "stopped"]
    assert (await saved(tx, outbox))[0] == wire


async def test_manifest_digest_and_stopped_attestation_precede_apply(legacy):
    tx, event_id, recorder, _ = legacy
    service = tool.ResultHandoff(tx, recorder)
    manifest = await service.dry_run([event_id])
    changed = copy.deepcopy(manifest)
    changed["rows"][0]["source"]["attempts"] = 1
    with pytest.raises(tool.HandoffError):
        await service.apply(
            changed, manifest["digest"], all_claimers_stopped_and_admission_closed=True
        )
    with pytest.raises(tool.HandoffError):
        await service.apply(
            manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=False
        )
    assert not await saved(tx, outbox)
    assert not (await saved(tx, result_outbox))[0]["mq_owned"]


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
async def test_native_handoff_process_kill_preserves_atomic_ownership(legacy, tmp_path, boundary):
    """Real one-shot CLI killed at SQL/returned-commit boundaries, without any Relay."""
    tx, event_id, recorder, keys = legacy
    service = tool.ResultHandoff(tx, recorder)
    manifest = await service.dry_run([event_id])
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    signing, recipient = tmp_path / "ai-sign.json", tmp_path / "qs-encrypt.json"
    signing.write_text(keys["ai.sign"].export_private())
    recipient.write_text(keys["qs.encrypt"].export_public())
    signing.chmod(0o600)
    recipient.chmod(0o600)
    assert tx.database.engine is not None
    env = {k: v for k, v in os.environ.items() if not k.startswith("QS_AI_")}
    env["QS_AI_MESSAGING_HANDOFF_DATABASE_URL"] = tx.database.engine.url.render_as_string(
        hide_password=False
    )
    options = [
        "--action",
        "apply",
        "--manifest",
        str(path),
        "--reviewed-digest",
        manifest["digest"],
        "--all-claimers-stopped-and-admission-closed",
        "--signing-key-file",
        str(signing),
        "--qs-recipient-key-file",
        str(recipient),
    ]
    barrier, marker = ("mq_tool_" + uuid4().hex for _ in range(2))
    child = None
    trigger = False
    async with tx.database.engine.connect() as control:
        try:
            if boundary == "before_commit":
                assert (
                    await control.scalar(text("SELECT GET_LOCK(:lock,0)"), {"lock": barrier}) == 1
                )
                # The old-row ownership UPDATE follows the new wire INSERT, both
                # inside the same root transaction. Named locks identify the actual
                # pending SQL, rather than relying on elapsed sleeps as evidence.
                await control.execute(
                    text(
                        "CREATE TRIGGER mq_tool_kill BEFORE UPDATE ON result_outbox FOR EACH ROW "
                        "BEGIN IF NEW.mq_owned=TRUE AND OLD.mq_owned=FALSE THEN "
                        f"SET @mq_tool_marker=GET_LOCK('{marker}',0); "
                        f"SET @mq_tool_barrier=GET_LOCK('{barrier}',30); END IF; END"
                    )
                )
                trigger = True
                await control.commit()
                argv = [sys.executable, "-m", "qs_ai.maintenance.messaging_handoff", *options]
            else:
                # Test-only outer process pause after the unmodified native CLI has
                # returned success. No transaction method or production hook changes.
                wrapper = (
                    "import signal\nfrom qs_ai.maintenance.messaging_handoff import main\n"
                    "try:\n main()\nexcept SystemExit as e:\n"
                    " if e.code: raise\n"
                    " print('POST_COMMIT_BOUNDARY',flush=True)\n signal.pause()\n"
                )
                argv = [sys.executable, "-c", wrapper, *options]
            child = await asyncio.create_subprocess_exec(
                *argv, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            if boundary == "before_commit":
                async with asyncio.timeout(15):
                    while (
                        await control.scalar(text("SELECT IS_USED_LOCK(:lock)"), {"lock": marker})
                        is None
                    ):
                        assert child.returncode is None
                        await asyncio.sleep(0.05)
            else:
                assert child.stdout is not None
                output = json.loads(await asyncio.wait_for(child.stdout.readline(), 15))
                assert output["result"]["complete"] is True
                assert (
                    await asyncio.wait_for(child.stdout.readline(), 5) == b"POST_COMMIT_BOUNDARY\n"
                )
            child.kill()
            await asyncio.wait_for(child.wait(), 10)
            assert child.returncode == -9
            if boundary == "before_commit":
                await control.scalar(text("SELECT RELEASE_LOCK(:lock)"), {"lock": barrier})
                async with asyncio.timeout(15):
                    while (
                        await control.scalar(text("SELECT IS_USED_LOCK(:lock)"), {"lock": marker})
                        is not None
                    ):
                        assert child.returncode == -9
                        await asyncio.sleep(0.05)
                assert not await saved(tx, outbox)
                assert not (await saved(tx, result_outbox))[0]["mq_owned"]
            else:
                assert (await saved(tx, result_outbox))[0]["mq_owned"]
                assert len(await saved(tx, outbox)) == 1
        finally:
            if child is not None and child.returncode is None:
                child.kill()
                await child.wait()
            await control.scalar(text("SELECT RELEASE_LOCK(:lock)"), {"lock": barrier})
            if trigger:
                await control.execute(text("DROP TRIGGER mq_tool_kill"))
    # Resume the same reviewed identity after either interruption. First durable
    # wire is fixed; no model or task execution is introduced by maintenance.
    first = await service.apply(
        manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
    )
    wire = (await saved(tx, outbox))[0]
    assert first["rows"][0]["status"] == (
        "transferred" if boundary == "before_commit" else "retained"
    )
    await service.apply(
        manifest, manifest["digest"], all_claimers_stopped_and_admission_closed=True
    )
    assert (await saved(tx, outbox))[0] == wire
