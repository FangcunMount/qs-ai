"""The existing solution workspace prepares the MBTI template without approval."""

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import CreateSolution, PrepareSolution
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.config import Settings
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql import schema as tables
from qs_ai.infrastructure.persistence.mysql.solution_templates import (
    template_catalog,
    template_release,
)
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.qs_server.evaluation_suite import MBTI_ROOT
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mbti_initialization import (
    apply,
    counts,
)
from tests.integration.test_mbti_initialization import (
    initialized_dependencies as initialized_dependencies,
)
from tests.integration.test_solutions import edit

pytestmark = pytest.mark.integration


@pytest.fixture
async def workspace(initialized_dependencies):
    tx, actor, commit = initialized_dependencies
    await apply(tx, actor, commit)
    sid = uuid4()
    scope = DraftScope(912, 42)
    settings = Settings(
        evaluation={**Settings().evaluation.model_dump(), "candidate_mode_enabled": True}
    )
    store = MySQLSolutions(tx, settings)
    command = CreateSolution(
        command_id=uuid4(), title="MBTI 模板闭环", reason="验证首版", template_ref=MBTI_ROOT
    )
    try:
        yield tx, store, scope, sid, command, datetime.now(UTC)
    finally:
        async with tx.open() as db:
            run_id, draft_id = str(uuid5(sid, "evaluation")), str(uuid5(sid, "prompt"))
            selectors = (
                await db.scalars(
                    select(tables.configuration_publications.c.selector_key).where(
                        tables.configuration_publications.c.run_id == run_id
                    )
                )
            ).all()
            for table in (
                tables.configuration_publication_changes,
                tables.configuration_publication_pointers,
            ):
                await db.execute(delete(table).where(table.c.selector_key.in_(selectors)))
            for table in reversed(tables.metadata.sorted_tables):
                if "run_id" in table.c:
                    await db.execute(delete(table).where(table.c.run_id == run_id))
            for table in (tables.solution_commands, tables.solution_revisions):
                await db.execute(delete(table).where(table.c.solution_id == str(sid)))
            version = f"solution-{sid}"
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
            for table in (tables.profile_assets, tables.prompt_assets):
                await db.execute(delete(table).where(table.c.version == version))
            await db.execute(
                delete(tables.route_assets).where(
                    tables.route_assets.c.revision.in_(
                        [version + "-generation", version + "-semantic"]
                    )
                )
            )
            await db.commit()


async def test_template_create_save_prepare_and_receipts_without_publication(
    workspace, monkeypatch
):
    tx, store, scope, sid, command, at = workspace
    # Once initialized, preparing a template may not read an MBTI initializer file.
    from qs_ai.infrastructure.qs_server import mbti_assets

    def forbidden(*args, **kwargs):
        raise AssertionError("runtime file fallback")

    monkeypatch.setattr(mbti_assets, "load_mbti_root", forbidden)
    before = await counts(tx)
    listing = await store.list(scope)
    assert listing["templates"][0]["published"] is False
    assert listing["templates"][0]["template_ref"] == asdict(MBTI_ROOT)
    created = await store.apply(scope, sid, command, at)
    assert created["source"]["template_ref"] == asdict(MBTI_ROOT)
    assert created["prepared"] is None and created["source_reviews"] == []
    assert created["scene_contract_version"] == "mbti-single-assessment/v1"
    listing = await store.list(scope)
    assert listing["items"][0]["scene_contract_version"] == created["scene_contract_version"]
    assert created["policy"]["scene_contract_version"] == "mbti-single-assessment/v1"
    assert await store.apply(scope, sid, command, at) == created
    assert await MySQLSolutions(tx, Settings()).receipt(scope, command.command_id) == created
    with pytest.raises(NotFound):
        await store.get(DraftScope(913, 42), sid)
    with pytest.raises(NotFound):
        await store.receipt(DraftScope(912, 43), command.command_id)
    with pytest.raises(DraftConflict):
        await store.apply(scope, sid, replace(command, title="changed"), at)
    saved = await store.apply(scope, sid, edit(created), at)
    prepare = PrepareSolution(command_id=uuid4(), reason="完整测试不发布", expected_revision=2)
    prepared = await store.apply(scope, sid, prepare, at)
    assert prepared["prepared"]["plan"]["candidate_count"] == 35
    assert prepared["prepared"]["plan"]["preflight_case_count"] == 1
    assert prepared["prepared"]["release"]["input_schema"]["version"] == "ai-explanation-input/v2"
    assert (
        prepared["prepared"]["release"]["semantic_prompt"]
        == created["source_release"]["semantic_prompt"]
    )
    assert prepared["generation"] == saved["generation"]
    assert await store.apply(scope, sid, prepare, at) == prepared
    after = await counts(tx)
    assert after == [before[0], before[1] + 1, before[2]]
    async with tx.open() as db:
        raw = await db.scalar(
            select(tables.evaluation_runs.c.definition_json).where(
                tables.evaluation_runs.c.run_id == prepared["prepared"]["run_id"]
            )
        )
        assert "mbti-single-semantic-evaluator" in raw


async def test_absent_template_is_not_initialized_implicitly(initialized_dependencies):
    tx, *_ = initialized_dependencies
    async with tx.open() as db:
        assert await template_catalog(db, DraftScope(912, 42)) == []
        with pytest.raises(ValueError):
            await template_release(db, DraftScope(912, 42), MBTI_ROOT)
        with pytest.raises(NotFound):
            await template_release(db, DraftScope(912, 42), replace(MBTI_ROOT, version="guessed"))


async def test_template_damaged_prompt_cannot_adopt_new_content(workspace):
    tx, store, scope, sid, command, at = workspace
    async with tx.open() as db:
        await db.execute(
            update(tables.prompt_assets)
            .where(tables.prompt_assets.c.template_id == MBTI_ROOT.id)
            .values(fingerprint="sha256:" + "0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await store.apply(scope, sid, command, at)
    with pytest.raises(NotFound):
        await store.get(scope, sid)


@pytest.mark.parametrize("recover_receipt", [False, True])
async def test_complete_mbti_run_uses_existing_graph_and_frozen_assets(
    workspace, monkeypatch, recover_receipt, kit
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.application.interpretation.provider import ModelResponse
    from qs_ai.infrastructure.persistence.mysql.evaluation_management import (
        MySQLEvaluationManagement,
    )
    from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
    from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
    from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
    from tests.test_mbti_runtime import mbti_output

    tx, store, scope, sid, command, at = workspace
    await store.apply(scope, sid, command, at)
    prepared = await store.apply(
        scope,
        sid,
        PrepareSolution(command_id=uuid4(), expected_revision=1, reason="完整隔离评测"),
        at,
    )
    run_id = UUID(prepared["prepared"]["run_id"])
    manager = MySQLEvaluationManagement(tx)
    management_scope = ManagementScope(run_id, scope.organization_id, scope.operator_user_id)
    await manager.start(management_scope, 1, "仅隔离假供应商测试", at, confirm=True)

    class Gateway:
        def __init__(self):
            self.calls = []
            self.types = set()

        async def generate_messages(self, messages, route, schema, invocation_id):
            self.calls.append(invocation_id)
            data = json.loads(messages.data_json)
            if "candidate_output" in data:
                assert "MBTI" in messages.system_message + messages.task_message
                output = {
                    "schema_version": "ai-explanation-semantic-evaluation-output/v1",
                    "scores": {
                        k: 5
                        for k in (
                            "faithfulness",
                            "cross_dimension_quality",
                            "suggestion_actionability",
                            "audience_clarity",
                            "concision",
                        )
                    },
                    "rationale": "隔离假裁判，不代表质量通过",
                    "decisions": [
                        dict(
                            type=a["type"],
                            scope=a["scope"],
                            ordinal=a["ordinal"],
                            status="passed",
                            detail="隔离回执",
                        )
                        for a in data["assertions"]
                    ],
                }
            else:
                self.types.add(data["facts"]["model_result"]["type_code"])
                output = mbti_output()
                output["summary"] = (
                    "本次测评呈现 " + data["facts"]["model_result"]["type_code"] + " 偏好组合。"
                )
            raw = json.dumps(output, ensure_ascii=False)
            return ModelResponse(
                invocation_id, "synthetic:" + invocation_id, route.model, raw, raw, "none", 1, 2, 3
            )

    gateway = Gateway()
    worker = EvaluationWorker(
        tx,
        gateway,
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
        "mbti-test",
        enabled=True,
        candidate_limit=3,
        clock=lambda: at,
    )
    if recover_receipt:
        from qs_ai.infrastructure.workflows import evaluation

        async def projection_unavailable(*args, **kwargs):
            raise ConnectionError("isolated projection interruption")

        assert await worker.once()  # Preflight has no provider call.
        with monkeypatch.context() as patch:
            patch.setattr(evaluation, "finish_step", projection_unavailable)
            with pytest.raises(ConnectionError):
                await worker.once()  # Response commits before interrupted projection.
        assert len(gateway.calls) == 1
        worker.clock = lambda: at + timedelta(minutes=6)
        assert await worker.once()  # Reclaims original persisted receipt.
        assert len(gateway.calls) == 1
    for _ in range(75):
        if not await worker.once():
            break
    view = await manager.get(management_scope)
    assert view.status == "awaiting_review", view
    assert len(gateway.calls) == 70 and len(set(gateway.calls)) == 70
    assert len(gateway.types) == 6
    assert not await worker.once()
    assert len(gateway.calls) == 70
    async with tx.open() as db:
        row = (
            (
                await db.execute(
                    select(tables.evaluation_runs).where(
                        tables.evaluation_runs.c.run_id == str(run_id)
                    )
                )
            )
            .mappings()
            .one()
        )
        assert row["execution_mode"] == "candidate_v2"
        assert row["progress_json"]["preflight"]["status"] == "passed"
        assert row["definition_json"]
        receipt_ids = (
            await db.scalars(
                select(tables.evaluation_response_receipts.c.invocation_id).where(
                    tables.evaluation_response_receipts.c.run_id == str(run_id)
                )
            )
        ).all()
        assert len(receipt_ids) == 70
    if not recover_receipt:
        await synthetic_publication_and_generation(
            tx, manager, management_scope, view, prepared, at, kit
        )


async def test_existing_mbti_solution_cannot_switch_to_scale_suite(workspace):
    from qs_ai.bootstrap.import_evaluation_suites import baseline, install_baseline

    tx, store, scope, sid, command, at = workspace
    created = await store.apply(scope, sid, command, at)
    source, suite, contracts = baseline()
    actor = "cross-scene-" + str(uuid4())
    try:
        async with tx.open() as db:
            await install_baseline(db, source, suite, contracts, actor)
            await db.commit()
        with pytest.raises(ValueError, match="cannot change source scene"):
            await store.apply(
                scope, sid, edit(created, evaluation_suite=asdict(suite.reference)), at
            )
        assert await store.get(scope, sid) == created
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(tables.evaluation_suites).where(
                    tables.evaluation_suites.c.imported_by == actor
                )
            )
            await db.commit()


async def test_profile_registration_cannot_convert_mbti_source_to_scale(workspace):
    from qs_ai.application.governance.profile_registration import RegisterProfile
    from qs_ai.bootstrap.import_profiles import baseline_assets
    from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
    from qs_ai.infrastructure.persistence.mysql.profile_registrations import validate_source

    tx, _, scope, _, _, _ = workspace
    async with tx.open() as db:
        release = await template_release(db, scope, MBTI_ROOT)
        _, manifest = await generation_snapshot(db, release)
        command = RegisterProfile(
            uuid4(),
            manifest.profile,
            baseline_assets()[1][0].definition_json,
            manifest.prompt,
            manifest.generation_route,
            "拒绝跨场景派生",
        )
        with pytest.raises(ValueError, match="cannot change source scene"):
            await validate_source(db, command)


async def synthetic_publication_and_generation(tx, manager, scope, view, prepared, at, kit):
    """Disposable DB only. Fake reviewers exercise contracts, not real quality approval."""
    from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
    from qs_ai.application.execution.worker import ExecuteNext
    from qs_ai.application.governance.publication import (
        MovePublication,
        PublicationScope,
        PublishConfiguration,
    )
    from qs_ai.application.interpretation.preparation import prepare_explanation
    from qs_ai.application.interpretation.provider import ModelResponse
    from qs_ai.domain.evaluation.review import CandidateHumanReview
    from qs_ai.domain.governance.publication import ReleaseSelector
    from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
    from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
        MySQLExecutionConfigurations,
    )
    from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
    from qs_ai.infrastructure.persistence.mysql.solution_assets import release_from
    from tests.integration.test_execution_configurations import workflow
    from tests.integration.test_interpretation import expire
    from tests.probes.session_inspection import read_session
    from tests.test_mbti_runtime import mbti_case, mbti_output

    candidates = (await manager.list_candidates(scope)).candidates
    for role, reviewer in (("assessment_semantics", 42), ("safety_product", 43)):
        reviewer_scope = replace(scope, operator_user_id=reviewer)
        reviews = tuple(
            CandidateHumanReview(
                c.candidate_id,
                role,
                reviewer_scope.actor,
                "approve",
                at + timedelta(seconds=1),
                "仅隔离合成审核数据",
            )
            for c in candidates
        )
        view = await manager.review(reviewer_scope, view.version, reviews)
    preview = await manager.preview_gates(scope, view.version, at + timedelta(seconds=2))
    assert all(passed for _, passed in preview.gate_passes), preview.quality.reasons
    view = await manager.finalize(
        scope, view.version, True, "仅隔离门槛测试", at + timedelta(seconds=2), confirm=True
    )
    assert view.status == "approved"
    selector = ReleaseSelector(**prepared["policy"]["selector"])
    release = release_from(prepared["prepared"]["release"])
    publications = MySQLPublications(tx)
    publication = await publications.apply(
        PublicationScope(scope.organization_id, 42),
        PublishConfiguration(
            uuid4(),
            selector,
            0,
            None,
            "仅隔离发布测试",
            True,
            scope.run_id,
            view.version,
            release.fingerprint(),
        ),
        at + timedelta(seconds=3),
    )
    original = publication.change.current.active
    assert original is not None
    _, evidence, _ = mbti_case()
    from qs_ai.application.interpretation.eligibility import Eligibility
    from tests.integration.test_eligibility import readonly_check

    assert await readonly_check(tx, kit.actor, evidence.items) == Eligibility("available")
    request_id = str(uuid4())
    receipt = await kit.service.start_external(
        kit.actor, "7", ("42",), "MBTI 解读", request_id, evidence.items
    )
    assert receipt.status == "queued"
    assert (
        await kit.service.start_external(
            kit.actor, "7", ("42",), "MBTI 解读", request_id, evidence.items
        )
        == receipt
    )
    claim = await kit.store.claim(60)
    assert claim.session.workflow_version == "qs-published-snapshot-v2"
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(tables.evidence_sets).where(
                        tables.evidence_sets.c.session_id == receipt.session_id
                    )
                )
            )
            .mappings()
            .one()
        )
    bound = replace(evidence, id=row["id"], session_id=receipt.session_id)
    config = await MySQLExecutionConfigurations(kit.transactions).get(claim, bound)
    request = FrozenGeneration(
        prepare_explanation(claim.session, bound, config.release, config.package),
        config.route,
        config.schema,
        publication_id=config.publication_id,
        manifest_fingerprint=config.manifest_fingerprint,
    )

    class Model:
        calls = 0

        async def generate(self, source, route, schema, invocation_id):
            self.calls += 1
            assert source.release.input_policy.profile_id == "participant-mbti-single"
            raw = json.dumps(mbti_output(), ensure_ascii=False)
            return ModelResponse(
                invocation_id, "synthetic-user-result", route.model, raw, raw, "none", 1, 2, 3
            )

    model = Model()
    generated = await DurableGeneration(kit.store, model, JSONModelCallCodec()).execute(
        claim, request
    )
    await publications.apply(
        PublicationScope(scope.organization_id, 42),
        MovePublication(
            uuid4(), selector, 1, original.publication_id, "隔离暂停后恢复", True, None
        ),
        at + timedelta(seconds=4),
    )
    assert await readonly_check(tx, kit.actor, evidence.items) == Eligibility(
        "unavailable", "publication_paused"
    )
    await expire(kit, receipt.session_id)
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    result = await read_session(kit.service.uows, receipt.session_id)
    assert result.session.status == "completed", result.session.failure_code
    assert model.calls == 1
    async with tx.open() as db:
        binding = (
            (
                await db.execute(
                    select(tables.execution_configurations).where(
                        tables.execution_configurations.c.session_id == receipt.session_id
                    )
                )
            )
            .mappings()
            .one()
        )
        assert binding["publication_id"] == str(original.publication_id)
        raw = await db.scalar(
            select(tables.model_calls.c.request_json).where(
                tables.model_calls.c.run_id == receipt.run_id
            )
        )
        assert JSONModelCallCodec().decode_request(raw) == generated.request
        assert await db.scalar(
            select(tables.artifacts.c.id).where(tables.artifacts.c.session_id == receipt.session_id)
        )
        outputs = (
            await db.scalars(
                select(tables.result_outbox.c.payload).where(
                    tables.result_outbox.c.session_id == receipt.session_id
                )
            )
        ).all()
        assert any(output["status"] == "completed" for output in outputs)


async def test_initialized_template_alone_cannot_admit_mbti_generation(workspace, kit):
    from tests.probes.session_inspection import read_session
    from tests.test_mbti_runtime import mbti_case

    _, evidence, _ = mbti_case()
    from qs_ai.application.interpretation.eligibility import Eligibility
    from tests.integration.test_eligibility import readonly_check

    assert await readonly_check(workspace[0], kit.actor, evidence.items) == Eligibility(
        "unavailable", "publication_missing"
    )
    request_id = str(uuid4())
    receipt = await kit.service.start_external(
        kit.actor, "7", ("42",), "未发布 MBTI", request_id, evidence.items
    )
    view = await read_session(kit.service.uows, receipt.session_id)
    assert view.session.workflow_version == "qs-published-snapshot-v2"
    assert view.session.failure_code == "configuration_unavailable"
    assert await kit.store.claim(60) is None
    assert (
        await kit.service.start_external(
            kit.actor, "7", ("42",), "未发布 MBTI", request_id, evidence.items
        )
        == receipt
    )
