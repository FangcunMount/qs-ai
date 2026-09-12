"""Actual native Prompt/Profile/suite storage and Run execution; synthetic facts/model only."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.prompt_drafts import CreatePromptDraft, RevisePromptDraft
from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
from qs_ai.application.governance.suite_registration import RegisterSuite
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator, create_run
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import (
    MySQLSuiteRegistrar,
    apply_registration,
    load_registered_suite,
)
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import MySQLPromptFreezer
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_suites,
    prompt_assets,
    prompt_draft_freezes,
    prompt_draft_revisions,
    prompt_drafts,
)
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.fixture
async def suite_registration(registration, complete_release):
    tx, profile_store, scope, profile_command, at, _ = registration
    draft_id, prompt_version = uuid4(), "suite-prompt-" + str(uuid4())
    suite_id = "native-suite-" + str(uuid4())
    try:
        drafts = MySQLPromptDrafts(tx)
        draft = await drafts.apply(
            scope,
            CreatePromptDraft(
                draft_id,
                uuid4(),
                profile_command.prompt,
                profile_command.prompt.identity,
                prompt_version,
                "复用模板用于新套件",
            ),
            at,
        )
        await drafts.apply(
            scope,
            RevisePromptDraft(
                draft_id,
                uuid4(),
                1,
                replace(
                    draft.content,
                    system_message=draft.content.system_message + "\nQS_NATIVE_SUITE_TEST",
                ),
                "修改新模板",
            ),
            at,
        )
        frozen = await MySQLPromptFreezer(tx).freeze(
            scope, FreezePromptDraft(draft_id, uuid4(), 2, "冻结套件模板"), at
        )
        definition = json.loads(profile_command.definition_json)
        definition["generation_policy"]["prompt_version"] = prompt_version
        profile = await profile_store.register(
            scope,
            replace(profile_command, definition_json=json.dumps(definition), prompt=frozen.asset),
            at,
        )
        manifest = profile.manifest
        command = RegisterSuite(
            uuid4(),
            V6_PUBLISHED,
            suite_id,
            "v1",
            manifest.profile,
            manifest.prompt,
            manifest.generation_route,
            "为新配置登记完整案例",
        )
        release = replace(
            complete_release,
            profile=FrozenContractRef(
                manifest.profile.identity, manifest.profile.version, manifest.profile.fingerprint
            ),
            prompt=FrozenContractRef(
                manifest.prompt.identity, manifest.prompt.version, manifest.prompt.fingerprint
            ),
        )
        yield tx, MySQLSuiteRegistrar(tx), scope, command, at, manifest, release
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_suites).where(evaluation_suites.c.suite_id == suite_id)
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
            await db.execute(delete(prompt_assets).where(prompt_assets.c.version == prompt_version))
            await db.commit()


async def test_register_replay_scoped_receipt_and_original_cases(suite_registration):
    tx, store, scope, command, at, manifest, _ = suite_registration
    first = await store.register(scope, command, at)
    assert first.manifest == manifest
    assert await store.register(scope, command, at + timedelta(seconds=1)) == first
    assert await MySQLSuiteRegistrar(tx).get_receipt(scope, command.command_id) == first
    for foreign in (replace(scope, organization_id=2), replace(scope, operator_user_id=43)):
        with pytest.raises(NotFound):
            await store.get_receipt(foreign, command.command_id)
    async with tx.open() as db:
        suite = await load_registered_suite(db, first.suite)
    assert suite.manifest == manifest
    assert (
        json.loads(suite.definition_json)["cases"]
        == json.loads(load_suite(V6_PUBLISHED).definition_json)["cases"]
    )


async def test_competing_commands_and_transaction_rollback(suite_registration):
    tx, store, scope, command, at, *_ = suite_registration
    async with tx.open() as db:
        await apply_registration(db, scope, command, at)
    with pytest.raises(NotFound):
        await store.get_receipt(scope, command.command_id)
    first, replay = await asyncio.gather(
        store.register(scope, command, at), store.register(scope, command, at)
    )
    assert first == replay
    with pytest.raises(AssetConflict):
        await store.register(scope, replace(command, command_id=uuid4()), at)
    with pytest.raises(AssetConflict):
        await store.register(scope, replace(command, reason="另一个指令"), at)


@pytest.mark.parametrize("damage", ["profile", "prompt", "route", "source", "retained_identity"])
async def test_invalid_assets_never_register(suite_registration, damage):
    tx, store, scope, command, at, *_ = suite_registration
    if damage == "retained_identity":
        command = replace(command, suite_id=V6_PUBLISHED.id, suite_version=V6_PUBLISHED.version)
    elif damage == "source":
        command = replace(command, source=replace(command.source, fingerprint="sha256:" + "0" * 64))
    else:
        name = "generation_route" if damage == "route" else damage
        command = replace(
            command, **{name: replace(getattr(command, name), content_sha256="0" * 64)}
        )
    with pytest.raises(ValueError):
        await store.register(scope, command, at)
    with pytest.raises(NotFound):
        await store.get_receipt(scope, command.command_id)


@pytest.fixture
async def native_creation(suite_registration, monkeypatch):
    from tests.integration import test_evaluation_step

    tx, store, scope, command, at, manifest, release = suite_registration
    registered = await store.register(scope, command, at)
    release = replace(release, suite=registered.suite)

    async def create(db, run_id, ignored):
        return await create_run(
            db, run_id, release, 1, "actor:1", "评测新的原生配置", AT, generation_manifest=manifest
        )

    monkeypatch.setattr(test_evaluation_step, "create", create)
    return registered, release


@pytest.fixture
async def native_ready(native_creation, ready):
    return ready


async def test_native_run_dispatch_acceptance_and_semantic_use_new_assets(native_ready):
    class Capture(Gateway):
        async def generate_messages(self, messages, *args):
            if "candidate_output" not in json.loads(messages.data_json):
                assert "QS_NATIVE_SUITE_TEST" in messages.system_message
            return await super().generate_messages(messages, *args)

    gateway = Capture(native_ready)
    state = await step(native_ready, gateway)
    state = await step(native_ready, gateway, state.version)
    assert state.version == 9 and gateway.calls == 2


async def test_official_creator_freezes_suite_and_rejects_old_manifest(
    suite_registration, setup_run
):
    tx, store, scope, command, at, manifest, release = suite_registration
    registered = await store.register(scope, command, at)
    run_id = setup_run[1]
    release = replace(release, suite=registered.suite)
    creator = MySQLRunCreator(
        tx,
        MySQLProfileAssets(tx),
        MySQLPromptAssets(tx),
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
    )
    await creator.create(run_id, release, 1, "actor:1", "登记后创建新 Run", at)
    record = (await rows(tx, run_id))[0]
    definition = json.loads(record["definition_json"])
    assert definition["generation_manifest_json"] == manifest.canonical_json()
    assert json.loads(definition["suite_json"])["suite_id"] == command.suite_id
    async with tx.open() as db:
        with pytest.raises(ValueError, match="registered generation manifest"):
            await create_run(db, uuid4(), release, 1, "actor:1", "不能省略清单", at)


@pytest.mark.parametrize(
    "damage", ["definition_json", "receipt_sha256", "organization_id", "fingerprint", "missing"]
)
async def test_registered_suite_damage_blocks_dispatch_and_receipt(
    native_ready, suite_registration, damage
):
    tx, store, scope, command, *_ = suite_registration
    async with tx.open() as db:
        where = evaluation_suites.c.command_id == str(command.command_id)
        if damage == "missing":
            await db.execute(delete(evaluation_suites).where(where))
        else:
            row = (await db.execute(select(evaluation_suites).where(where))).mappings().one()
            value = (
                row[damage] + " "
                if damage == "definition_json"
                else 99
                if damage == "organization_id"
                else "0" * 64
                if damage == "receipt_sha256"
                else "sha256:" + "0" * 64
            )
            await db.execute(update(evaluation_suites).where(where).values(**{damage: value}))
        await db.commit()
    gateway = Gateway(native_ready)
    with pytest.raises(ValueError):
        await step(native_ready, gateway)
    assert gateway.calls == 0
    with pytest.raises((ValueError, NotFound)):
        await store.get_receipt(scope, command.command_id)


async def test_distinct_commands_racing_for_same_version_have_one_winner(suite_registration):
    _, store, scope, command, at, *_ = suite_registration
    other = replace(command, command_id=uuid4())
    results = await asyncio.gather(
        store.register(scope, command, at), store.register(scope, other, at), return_exceptions=True
    )
    assert sum(isinstance(r, AssetConflict) for r in results) == 1
    winner = next(r for r in results if not isinstance(r, BaseException))
    assert await store.get_receipt(scope, winner.command.command_id) == winner
    loser = other if winner.command == command else command
    with pytest.raises(NotFound):
        await store.get_receipt(scope, loser.command_id)


async def test_suite_damage_after_dispatch_keeps_unknown_execution_pending(
    native_ready, suite_registration
):
    tx, _, _, command, *_ = suite_registration

    class Mutating(Gateway):
        async def generate_messages(self, *args):
            result = await super().generate_messages(*args)
            async with tx.open() as db:
                await db.execute(
                    update(evaluation_suites)
                    .where(evaluation_suites.c.command_id == str(command.command_id))
                    .values(receipt_sha256="0" * 64)
                )
                await db.commit()
            return result

    model = Mutating(native_ready)
    with pytest.raises(ValueError):
        await step(native_ready, model)
    checkpoint = (await rows(tx, native_ready[1]))[2]
    assert model.calls == 1 and checkpoint["version"] == 5
    assert checkpoint["checkpoint_json"]["phase"] == "dispatching"


async def test_valid_changed_eligibility_requires_new_case_contract(
    suite_registration, registration
):
    from qs_ai.infrastructure.persistence.mysql.schema import profile_assets, profile_registrations

    tx, store, scope, command, at, *_ = suite_registration
    _, profiles, _, original, _, _ = registration
    definition = json.loads(original.definition_json)
    definition["version"] += "-input"
    definition["eligibility"]["min_eligible_dimensions"] = 3
    changed = replace(original, command_id=uuid4(), definition_json=json.dumps(definition))
    try:
        profile = await profiles.register(scope, changed, at)
        binding = replace(command, profile=profile.manifest.profile, prompt=profile.manifest.prompt)
        with pytest.raises(ValueError, match="New input policy requires"):
            await store.register(scope, binding, at)
        with pytest.raises(NotFound):
            await store.get_receipt(scope, command.command_id)
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(profile_registrations).where(
                    profile_registrations.c.command_id == str(changed.command_id)
                )
            )
            await db.execute(
                delete(profile_assets).where(profile_assets.c.version == definition["version"])
            )
            await db.commit()
