"""Real MySQL solution saves, CAS, atomic preparation and original receipts."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4, uuid5

import pytest
from sqlalchemy import delete, select

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import CreateSolution, PrepareSolution, SaveSolution
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.config import Settings
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql import schema as tables
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import (
    prepare_run_case,
    run_model_route,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.qs_server.deepseek_request import build_messages_request
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from qs_ai.transport.grpc.solution_input import parse_command
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.fixture
async def workspace(setup_run, persisted_assets, complete_release):
    tx, run_id, _ = setup_run
    release = replace(complete_release, suite=V6_PUBLISHED)
    scope, solution_id, at = DraftScope(1, 42), uuid4(), datetime.now(UTC)
    async with tx.open() as db:
        _, manifest = await generation_snapshot(db, release)
        await create_run(
            db, run_id, release, 1, "user:42", "源测试", at, generation_manifest=manifest
        )
        await db.commit()
    store = MySQLSolutions(tx, Settings())
    create = CreateSolution(
        command_id=uuid4(), reason="改进表达", title="迭代版本", source_run_id=run_id
    )
    try:
        yield tx, store, scope, solution_id, create, at
    finally:
        draft_id, new_run = str(uuid5(solution_id, "prompt")), str(uuid5(solution_id, "evaluation"))
        version = f"solution-{solution_id}"
        async with tx.open() as db:
            for table in (
                tables.evaluation_checkpoints,
                tables.evaluation_run_policies,
                tables.evaluation_runs,
            ):
                await db.execute(delete(table).where(table.c.run_id == new_run))
            for table, column in (
                (tables.solution_commands, "solution_id"),
                (tables.solution_revisions, "solution_id"),
            ):
                await db.execute(delete(table).where(table.c[column] == str(solution_id)))
            await db.execute(
                delete(tables.evaluation_suites).where(
                    tables.evaluation_suites.c.suite_version == version
                )
            )
            await db.execute(
                delete(tables.profile_registrations).where(
                    tables.profile_registrations.c.profile_version == version
                )
            )
            for table in (
                tables.prompt_draft_freezes,
                tables.prompt_draft_revisions,
                tables.prompt_drafts,
            ):
                await db.execute(delete(table).where(table.c.draft_id == draft_id))
            await db.execute(
                delete(tables.profile_assets).where(tables.profile_assets.c.version == version)
            )
            await db.execute(
                delete(tables.prompt_assets).where(tables.prompt_assets.c.version == version)
            )
            await db.execute(
                delete(tables.route_assets).where(
                    tables.route_assets.c.revision.in_(
                        [version + "-generation", version + "-semantic"]
                    )
                )
            )
            await db.commit()


def edit(state, **changes):
    value = {
        "command_id": str(uuid4()),
        "expected_revision": state["revision"],
        "title": state["title"],
        "reason": "调整输出上限",
        "content": state["content"],
        "generation": {**state["generation"], "max_output_tokens": 4000},
        "semantic": state["semantic"],
        **changes,
    }
    return parse_command(json.dumps(value), SaveSolution)


async def test_save_survives_new_store_and_receipt_loss_and_scope_boundaries(workspace):
    tx, store, scope, sid, create, at = workspace
    first = await store.apply(scope, sid, create, at)
    command = edit(first)
    saved = await store.apply(scope, sid, command, at)
    assert saved["revision"] == 2 and saved["generation"]["max_output_tokens"] == 4000
    assert await MySQLSolutions(tx, Settings()).get(scope, sid) == saved
    assert await store.apply(scope, sid, command, at) == saved
    assert await store.receipt(scope, command.command_id) == saved
    assert saved["original_models"]["generation"]["max_output_tokens"] != 4000
    with pytest.raises(NotFound):
        await store.get(DraftScope(2, 42), sid)
    with pytest.raises(NotFound):
        await store.receipt(DraftScope(1, 43), command.command_id)
    assert (await store.get(DraftScope(1, 43), sid))["revision"] == 2
    assert any(row["solution_id"] == str(sid) for row in (await store.list(scope))["items"])


async def test_concurrent_saves_do_not_overwrite_and_replay_checks_body(workspace):
    _, store, scope, sid, create, at = workspace
    first = await store.apply(scope, sid, create, at)
    outcomes = await asyncio.gather(
        *(store.apply(scope, sid, edit(first), at) for _ in range(2)), return_exceptions=True
    )
    assert sum(isinstance(item, DraftConflict) for item in outcomes) == 1
    assert (await store.get(scope, sid))["revision"] == 2
    with pytest.raises(DraftConflict):
        await store.apply(scope, sid, replace(create, title="不同请求"), at)


async def test_prepare_is_atomic_and_uses_saved_model_parameters(workspace):
    tx, store, scope, sid, create, at = workspace
    first = await store.apply(scope, sid, create, at)
    await store.apply(scope, sid, edit(first), at)
    command = PrepareSolution(command_id=uuid4(), expected_revision=2, reason="完整评测")
    prepared = await store.apply(scope, sid, command, at)
    assert await store.apply(scope, sid, command, at) == prepared
    assert len(prepared["prepared"]["steps"]) == 5
    async with tx.open() as db:
        row = (
            (
                await db.execute(
                    select(tables.evaluation_runs).where(
                        tables.evaluation_runs.c.run_id == prepared["prepared"]["run_id"]
                    )
                )
            )
            .mappings()
            .one()
        )
        creation = json.loads(row["definition_json"])
        assert len(creation["slots"]) == 35 and creation["status"] == "requested"
        route = await run_model_route(db, creation, semantic=False)
        assert route.max_output_tokens == 4000
        case = await prepare_run_case(db, creation, creation["slots"][0]["case_id"])
        request = build_messages_request(case.messages, route, {"title": "test", "type": "object"})
        assert request["max_output_tokens"] == 4000
    with pytest.raises(DraftConflict):
        await store.apply(scope, sid, edit(prepared), at)


async def test_preparation_failure_rolls_back_all_steps_and_can_retry(workspace, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import solution_assets

    tx, store, scope, sid, create, at = workspace
    first = await store.apply(scope, sid, create, at)
    register = solution_assets.register_suite

    async def fail(*args, **kwargs):
        raise RuntimeError("injected failure after profile registration")

    monkeypatch.setattr(solution_assets, "register_suite", fail)
    command = PrepareSolution(command_id=uuid4(), expected_revision=1, reason="测试恢复")
    with pytest.raises(RuntimeError):
        await store.apply(scope, sid, command, at)
    assert (await store.get(scope, sid))["revision"] == 1
    async with tx.open() as db:
        assert not (
            await db.execute(
                select(tables.prompt_draft_freezes).where(
                    tables.prompt_draft_freezes.c.draft_id == first["draft_id"]
                )
            )
        ).first()
        assert not (
            await db.execute(
                select(tables.profile_assets).where(
                    tables.profile_assets.c.version == first["target_version"]
                )
            )
        ).first()
    monkeypatch.setattr(solution_assets, "register_suite", register)
    assert (await store.apply(scope, sid, command, at))["prepared"]


async def test_revoked_editor_model_blocks_new_start_without_rewriting_snapshot(workspace):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.application.governance.solution_models import EditableModelPolicy
    from qs_ai.infrastructure.persistence.mysql.editable_model_policy import check_editable_models
    from qs_ai.infrastructure.persistence.mysql.evaluation_management import (
        MySQLEvaluationManagement,
    )
    from qs_ai.infrastructure.persistence.mysql.solution_assets import release_from

    tx, store, scope, sid, create, at = workspace
    await store.apply(scope, sid, create, at)
    prepared = await store.apply(
        scope, sid, PrepareSolution(command_id=uuid4(), expected_revision=1, reason="固定测试"), at
    )
    from uuid import UUID

    run_scope = ManagementScope(
        UUID(prepared["prepared"]["run_id"]), scope.organization_id, scope.operator_user_id
    )
    management = MySQLEvaluationManagement(tx, models=EditableModelPolicy(()))
    with pytest.raises(ValueError, match="no longer enabled"):
        await management.start(run_scope, 1, "启动", at, confirm=True)
    assert (await management.get(run_scope)).status == "requested"
    async with tx.open() as db:
        with pytest.raises(ValueError, match="no longer enabled"):
            await check_editable_models(
                db, release_from(prepared["prepared"]["release"]), EditableModelPolicy(())
            )
    assert (await store.get(scope, sid))["prepared"] == prepared["prepared"]


async def test_external_prompt_edit_cannot_be_silently_prepared(workspace):
    from uuid import UUID

    from qs_ai.application.governance.prompt_drafts import RevisePromptDraft
    from qs_ai.domain.governance.prompt_draft import PromptDraftContent
    from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts

    tx, store, scope, sid, create, at = workspace
    first = await store.apply(scope, sid, create, at)
    content = first["content"]
    await MySQLPromptDrafts(tx).apply(
        scope,
        RevisePromptDraft(
            UUID(first["draft_id"]), uuid4(), 1, PromptDraftContent(**content), "外部编辑"
        ),
        at,
    )
    with pytest.raises(DraftConflict, match="outside"):
        await store.apply(
            scope, sid, PrepareSolution(command_id=uuid4(), expected_revision=1, reason="测试"), at
        )


async def test_solution_fixed_contract_parsing_does_not_block_event_loop(workspace, monkeypatch):
    import threading

    from qs_ai.infrastructure.persistence.mysql import evaluation_contracts

    _, store, scope, sid, command, at = workspace
    owner = threading.get_ident()
    calls = []
    for module, name in (
        (evaluation_contracts, "semantic_assets"),
        (evaluation_contracts, "execution_policy"),
    ):
        original = getattr(module, name)

        def checked(*args, _original=original, _name=name, **kwargs):
            assert threading.get_ident() != owner
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(module, name, checked)
    created = await store.apply(scope, sid, command, at)
    await store.apply(
        scope,
        sid,
        PrepareSolution(
            command_id=uuid4(), expected_revision=created["revision"], reason="异步准备验证"
        ),
        at,
    )
    assert "semantic_assets" in calls
    assert "execution_policy" in calls


async def test_v2_prepare_freezes_zhipu_and_receipt_survives_catalog_removal(workspace):
    from dataclasses import asdict

    from qs_ai.application.governance.solution_models import selection
    from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
    from tests.test_model_route_v2 import route as zhipu_route
    from tests.test_model_selection_v2 import configuration

    tx, _, scope, sid, create, at = workspace
    store = MySQLSolutions(tx, Settings(models=configuration(), zhipu_api_key="synthetic"))
    first = await store.apply(scope, sid, create, at)
    save = edit(
        first,
        generation=asdict(selection(zhipu_route())),
        semantic=asdict(selection(zhipu_route())),
    )
    await store.apply(scope, sid, save, at)
    command = PrepareSolution(command_id=uuid4(), expected_revision=2, reason="多供应商测试")
    prepared = await store.apply(scope, sid, command, at)
    # Original command results survive current catalog and credential removal.
    assert await MySQLSolutions(tx, Settings()).apply(scope, sid, command, at) == prepared
    async with tx.open() as db:
        row = (
            (
                await db.execute(
                    select(tables.evaluation_runs).where(
                        tables.evaluation_runs.c.run_id == prepared["prepared"]["run_id"]
                    )
                )
            )
            .mappings()
            .one()
        )
        creation = json.loads(row["definition_json"])
        for semantic in (False, True):
            frozen = await run_model_route(db, creation, semantic=semantic)
            assert isinstance(frozen, ModelRouteV2)
            assert frozen.provider == "zhipu" and frozen.model == "glm-5.3"
            assert frozen.binding_revision == "v1"
        assert len(creation["slots"]) == 35
