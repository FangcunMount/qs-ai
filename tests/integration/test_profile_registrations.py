"""Real MySQL atomic registration; no model invocation, approval, or publication."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.profile_registration import RegisterProfile
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
from qs_ai.infrastructure.persistence.mysql.profile_registrations import (
    MySQLProfileRegistrar,
    apply_registration,
    reference,
)
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets, profile_registrations
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.fixture
async def registration(setup_run, persisted_assets, complete_release):
    tx, *_ = setup_run
    async with tx.open() as db:
        source, manifest = await generation_snapshot(db, complete_release)
    definition = json.loads(source.definition_json)
    definition["version"] = "registered-" + str(uuid4())
    definition["generation_policy"]["max_output_characters"] = 8000
    command = RegisterProfile(
        uuid4(),
        reference(source),
        json.dumps(definition, ensure_ascii=False),
        manifest.prompt,
        manifest.generation_route,
        "注册调整后的配置",
    )
    scope, at = DraftScope(1, 42), datetime.now(UTC)
    try:
        yield tx, MySQLProfileRegistrar(tx), scope, command, at, source
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(profile_registrations).where(
                    profile_registrations.c.profile_version == definition["version"]
                )
            )
            await db.execute(
                delete(profile_assets).where(profile_assets.c.version == definition["version"])
            )
            await db.commit()


async def records(tx, command):
    definition = json.loads(command.definition_json)
    async with tx.open() as db:
        return [
            (await db.execute(select(t).where(condition))).mappings().all()
            for t, condition in (
                (
                    profile_assets,
                    (profile_assets.c.profile_id == definition["profile_id"])
                    & (profile_assets.c.version == definition["version"]),
                ),
                (
                    profile_registrations,
                    profile_registrations.c.command_id == str(command.command_id),
                ),
            )
        ]


async def test_register_replay_new_store_receipt_and_unchanged_source(registration):
    tx, store, scope, command, at, source = registration
    receipt = await store.register(scope, command, at)
    assert receipt.manifest.profile.version == json.loads(command.definition_json)["version"]
    assert receipt.manifest.prompt == command.prompt
    assert await store.register(scope, command, at + timedelta(seconds=1)) == receipt
    assert await MySQLProfileRegistrar(tx).get_receipt(scope, command.command_id) == receipt
    rows = await records(tx, command)
    assert [len(r) for r in rows] == [1, 1]
    assert rows[0][0]["imported_by"] == "qs:org/1/user/42"
    from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets

    assert await MySQLProfileAssets(tx).get(source.profile_id, source.version) == source


async def test_concurrent_same_command_and_competing_target_version(registration):
    tx, store, scope, command, at, _ = registration
    first, second = await asyncio.gather(
        store.register(scope, command, at), store.register(scope, command, at)
    )
    assert first == second
    with pytest.raises(AssetConflict):
        await store.register(scope, replace(command, command_id=uuid4()), at)
    with pytest.raises(AssetConflict):
        await store.register(scope, replace(command, reason="changed"), at)
    assert [len(r) for r in await records(tx, command)] == [1, 1]


@pytest.mark.parametrize(
    "damage",
    [
        "source",
        "prompt",
        "route",
        "unknown_field",
        "unsafe_policy",
        "bad_json",
        "duplicate",
        "version",
    ],
)
async def test_invalid_definition_or_references_never_leave_partial_asset(registration, damage):
    tx, store, scope, command, at, source = registration
    changed = command
    if damage in {"source", "prompt", "route"}:
        name = "generation_route" if damage == "route" else damage
        changed = replace(
            command, **{name: replace(getattr(command, name), content_sha256="0" * 64)}
        )
    else:
        definition = json.loads(command.definition_json)
        if damage == "unknown_field":
            definition["api_key"] = "not-allowed"
        elif damage == "unsafe_policy":
            definition["insight_policy"]["allow_causal_claims"] = True
        elif damage == "version":
            definition["version"] = source.version
        raw = json.dumps(definition)
        if damage == "bad_json":
            raw = "{invalid"
        if damage == "duplicate":
            raw = raw[:-1] + ', "version":"other"}'
        changed = replace(command, definition_json=raw)
    with pytest.raises(ValueError):
        await store.register(scope, changed, at)
    assert [len(r) for r in await records(tx, command)] == [0, 0]


async def test_rollback_after_both_inserts_and_scope_isolation(registration):
    tx, store, scope, command, at, _ = registration
    with pytest.raises(RuntimeError):
        async with tx.open() as db:
            await apply_registration(db, scope, command, at)
            raise RuntimeError("injected before commit")
    assert [len(r) for r in await records(tx, command)] == [0, 0]
    await store.register(scope, command, at)
    for foreign in (DraftScope(2, 42), DraftScope(1, 43)):
        with pytest.raises(NotFound):
            await store.get_receipt(foreign, command.command_id)
        with pytest.raises(NotFound):
            await store.register(foreign, command, at)


@pytest.mark.parametrize("damage", ["receipt", "audit", "asset"])
async def test_tampered_registration_is_not_reported_as_confirmed(registration, damage):
    tx, store, scope, command, at, _ = registration
    receipt = await store.register(scope, command, at)
    async with tx.open() as db:
        if damage == "receipt":
            await db.execute(
                update(profile_registrations)
                .where(profile_registrations.c.command_id == str(command.command_id))
                .values(receipt_sha256="0" * 64)
            )
        else:
            fields = {"imported_by": "other"} if damage == "audit" else {"definition_json": "{}"}
            await db.execute(
                update(profile_assets)
                .where(profile_assets.c.version == receipt.manifest.profile.version)
                .values(**fields)
            )
        await db.commit()
    with pytest.raises(ValueError):
        await store.get_receipt(scope, command.command_id)


async def test_register_binds_a_new_native_prompt_without_publishing(registration):
    from qs_ai.application.governance.prompt_drafts import CreatePromptDraft
    from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
    from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
    from qs_ai.infrastructure.persistence.mysql.prompt_freezes import MySQLPromptFreezer
    from qs_ai.infrastructure.persistence.mysql.schema import (
        prompt_assets,
        prompt_draft_freezes,
        prompt_draft_revisions,
        prompt_drafts,
    )

    tx, store, scope, command, at, _ = registration
    draft_id = uuid4()
    version = "native-" + str(uuid4())
    draft = CreatePromptDraft(
        draft_id, uuid4(), command.prompt, command.prompt.identity, version, "复用既有模板"
    )
    try:
        await MySQLPromptDrafts(tx).apply(scope, draft, at)
        frozen = await MySQLPromptFreezer(tx).freeze(
            scope, FreezePromptDraft(draft_id, uuid4(), 1, "冻结新模板"), at
        )
        definition = json.loads(command.definition_json)
        definition["generation_policy"]["prompt_version"] = version
        bound = replace(command, definition_json=json.dumps(definition), prompt=frozen.asset)
        receipt = await store.register(scope, bound, at)
        assert receipt.manifest.prompt == frozen.asset
        assert receipt.manifest.profile.fingerprint != command.source.fingerprint
        assert await store.get_receipt(scope, command.command_id) == receipt
    finally:
        # This test's uniquely named assets only; registration is removed before its dependency.
        async with tx.open() as db:
            await db.execute(
                delete(profile_registrations).where(
                    profile_registrations.c.command_id == str(command.command_id)
                )
            )
            await db.execute(
                delete(prompt_draft_freezes).where(prompt_draft_freezes.c.draft_id == str(draft_id))
            )
            await db.execute(
                delete(prompt_draft_revisions).where(
                    prompt_draft_revisions.c.draft_id == str(draft_id)
                )
            )
            await db.execute(delete(prompt_drafts).where(prompt_drafts.c.draft_id == str(draft_id)))
            await db.execute(delete(prompt_assets).where(prompt_assets.c.version == version))
            await db.commit()


async def test_concurrent_distinct_commands_cannot_share_target_version(registration):
    tx, store, scope, command, at, _ = registration
    other = replace(command, command_id=uuid4(), reason="另一条注册命令")
    results = await asyncio.gather(
        store.register(scope, command, at), store.register(scope, other, at), return_exceptions=True
    )
    assert sum(isinstance(result, AssetConflict) for result in results) == 1
    winner = next(result for result in results if not isinstance(result, BaseException))
    assert await store.get_receipt(scope, winner.command.command_id) == winner
    loser = command if winner.command == other else other
    with pytest.raises(NotFound):
        await store.get_receipt(scope, loser.command_id)
    assert len((await records(tx, command))[0]) == 1
