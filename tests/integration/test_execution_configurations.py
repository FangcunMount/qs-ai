"""Actual admission/worker/publication transactions; synthetic model and IAM inputs."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.execution.configuration import PublishedReportWorkflow
from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.governance.publication import MovePublication
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredWorkflow
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    MySQLExecutionConfigurations,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.observation import observe
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
    execution_configurations,
    idempotency,
    model_calls,
    result_outbox,
    sessions,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from tests.integration.test_generation import Gateway
from tests.integration.test_interpretation import expire
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import freeze_creation as freeze_creation
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run
from tests.test_input_binding import bound_case
from tests.test_output_validation import candidate

pytestmark = [
    pytest.mark.integration,
    pytest.mark.parametrize("freeze_creation", [V6_PUBLISHED], indirect=True),
]


@pytest.fixture
async def admitted(ready, kit):
    tx, scope, command, at = ready
    published = await MySQLPublications(tx).apply(scope, command, at)
    _, evidence, _ = bound_case()
    service = InterpretationService(
        MySQLUnitOfWorkFactory(kit.transactions), kit.source, use_publications=True
    )
    key = str(uuid4())
    receipt = await service.start_external(kit.actor, "7", ("42",), "解读", key, evidence.items)
    return kit, service, key, receipt, evidence, published, (tx, scope, command, at)


class Model(Gateway):
    async def generate(self, *args):
        response = await super().generate(*args)
        raw = json.dumps(candidate())
        return replace(response, raw_output=raw, validation_output=raw)


def workflow(kit, model):
    return PublishedReportWorkflow(
        MySQLExecutionConfigurations(kit.transactions),
        DurableGeneration(kit.store, model, JSONModelCallCodec()),
        UnconfiguredWorkflow(),
    )


async def test_acceptance_replay_and_execution_keep_original_publication_after_replace(admitted):
    kit, service, key, receipt, evidence, first, (tx, scope, command, at) = admitted
    second = await MySQLPublications(tx).apply(
        scope,
        replace(
            command,
            command_id=uuid4(),
            expected_version=1,
            expected_active_id=first.change.current.active.publication_id,
        ),
        at + timedelta(seconds=1),
    )
    assert second.change.current.active.publication_id != first.change.current.active.publication_id
    assert (
        await service.start_external(kit.actor, "7", ("42",), "解读", key, evidence.items)
        == receipt
    )
    model = Model()
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    view = await service.get(kit.actor, receipt.session_id)
    assert view.session.status == "completed", view.session.failure_code
    async with tx.open() as db:
        binding = (
            (
                await db.execute(
                    select(execution_configurations).where(
                        execution_configurations.c.session_id == receipt.session_id
                    )
                )
            )
            .mappings()
            .one()
        )
        request = json.loads(
            await db.scalar(
                select(model_calls.c.request_json).where(model_calls.c.run_id == receipt.run_id)
            )
        )
        assert binding["publication_id"] == str(first.change.current.active.publication_id)
        assert binding["pointer_version"] == 1
        assert request["publication_id"] == binding["publication_id"]
        assert (
            request["manifest_fingerprint"]
            == first.change.current.active.evidence.manifest.fingerprint()
        )
        assert await db.scalar(
            select(artifacts.c.id).where(artifacts.c.session_id == receipt.session_id)
        )
    assert model.calls == 1


async def test_observation_uses_bound_profile_and_acknowledged_completion(admitted):
    kit, service, key, receipt, evidence, first, (tx, scope, command, at) = admitted
    profile = first.change.current.active.evidence.manifest.profile.fingerprint
    begin = datetime(2026, 1, 1, tzinfo=UTC)
    end = begin + timedelta(days=1)
    async with tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(created_at=begin.replace(tzinfo=None))
        )
        await db.commit()
    before = await observe(tx, profile, begin, end)
    assert before["request_status_counts"] == {"queued": 1}
    assert before["completion_delivery_ms"]["p95"] is None
    assert before["backlog"]["queued_ready"] == 1
    assert before["acceptance_passed"] is False
    outside = await observe(tx, profile, begin - timedelta(days=1), begin)
    assert outside["request_status_counts"] == {}
    assert outside["backlog"]["queued_ready"] == 1

    assert await ExecuteNext(kit.store, kit.source, workflow(kit, Model())).once()
    assert (await service.get(kit.actor, receipt.session_id)).session.status == "completed"
    outbox = MySQLResultOutbox(tx)
    for event in await outbox.pending(20):
        await outbox.delivered(event.event_id)
    async with tx.open() as db:
        await db.execute(
            update(artifacts)
            .where(artifacts.c.session_id == receipt.session_id)
            .values(created_at=(begin + timedelta(seconds=2)).replace(tzinfo=None))
        )
        completion = result_outbox.c.payload["status"].as_string() == "completed"
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.session_id == receipt.session_id, completion)
            .values(
                created_at=(begin + timedelta(seconds=2)).replace(tzinfo=None),
                delivered_at=(begin + timedelta(seconds=5)).replace(tzinfo=None),
            )
        )
        await db.commit()
    # Changing the active publication does not change which old sessions are observed.
    await MySQLPublications(tx).apply(
        scope,
        MovePublication(
            uuid4(),
            command.selector,
            1,
            first.change.current.active.publication_id,
            "暂停新的准入",
            True,
            None,
        ),
        at + timedelta(seconds=1),
    )
    observed = await observe(tx, profile, begin, end)
    assert observed["request_status_counts"] == {"completed": 1}
    assert observed["provider_full_response_ms"]["p95"] == 4
    assert observed["request_to_artifact_ms"]["p95"] == 2000
    assert observed["completion_delivery_ms"]["p95"] == 3000
    assert observed["request_to_acknowledged_completion_ms"]["p95"] == 5000
    assert observed["backlog"]["pending_result_events"] == 0
    assert receipt.session_id not in json.dumps(observed)
    assert receipt.run_id not in json.dumps(observed)
    wrong_profile = await observe(tx, "sha256:" + "f" * 64, begin, end)
    assert wrong_profile["request_status_counts"] == {}
    assert wrong_profile["provider_full_response_ms"]["n"] == 0

    async with tx.open() as db:
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.session_id == receipt.session_id, completion)
            .values(created_at=None)
        )
        await db.commit()
    incomplete = await observe(tx, profile, begin, end)
    assert incomplete["completion_delivery_ms"]["n"] == 0
    assert incomplete["completion_delivery_ms"]["missing"] == 1
    assert incomplete["request_to_acknowledged_completion_ms"]["p95"] is None


async def test_disabled_pointer_does_not_rebind_accepted_task_or_allow_new_acceptance(admitted):
    kit, service, key, receipt, evidence, first, (tx, scope, command, at) = admitted
    stop = MovePublication(
        uuid4(), command.selector, 1, first.change.current.active.publication_id, "停用", True, None
    )
    await MySQLPublications(tx).apply(scope, stop, at + timedelta(seconds=1))
    rejected_key = str(uuid4())
    with pytest.raises(RuleViolation, match="configuration_unavailable"):
        await service.start_external(kit.actor, "7", ("42",), "解读", rejected_key, evidence.items)
    async with tx.open() as db:
        assert list(
            await db.scalars(
                select(sessions.c.id).where(sessions.c.owner_subject_id == kit.actor.subject_id)
            )
        ) == [receipt.session_id]
        assert (
            await db.scalar(select(idempotency.c.key).where(idempotency.c.key == rejected_key))
            is None
        )
    assert (
        await service.start_external(kit.actor, "7", ("42",), "解读", key, evidence.items)
        == receipt
    )
    model = Model()
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    assert (await service.get(kit.actor, receipt.session_id)).session.status == "completed"
    assert model.calls == 1


@pytest.mark.parametrize("damage", ["missing", "evidence", "pointer", "publication"])
async def test_invalid_binding_never_sends_or_accepts_artifact(admitted, damage):
    kit, service, _, receipt, _, _, _ = admitted
    async with kit.transactions.open() as db:
        where = execution_configurations.c.session_id == receipt.session_id
        if damage == "missing":
            await db.execute(delete(execution_configurations).where(where))
        else:
            field, value = {
                "evidence": ("evidence_fingerprint", "f" * 64),
                "pointer": ("pointer_version", 999),
                "publication": ("publication_sha256", "f" * 64),
            }[damage]
            await db.execute(update(execution_configurations).where(where).values(**{field: value}))
        await db.commit()
    model = Model()
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    assert (
        await service.get(kit.actor, receipt.session_id)
    ).session.failure_code == "configuration_invalid"
    assert model.calls == 0


async def test_recovery_reads_original_binding_and_durable_response(admitted):
    kit, service, _, receipt, _, first, (tx, scope, command, at) = admitted
    claim = await kit.store.claim(60)
    evidence = await kit.store.evidence(claim)
    model = Model()
    first_result = await workflow(kit, model).execute(claim, evidence)
    assert first_result.artifact is not None
    await MySQLPublications(tx).apply(
        scope,
        replace(
            command,
            command_id=uuid4(),
            expected_version=1,
            expected_active_id=first.change.current.active.publication_id,
        ),
        at + timedelta(seconds=1),
    )
    await expire(kit, receipt.session_id)
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    assert (await service.get(kit.actor, receipt.session_id)).session.status == "completed"
    assert model.calls == 1


@pytest.mark.parametrize("damage", ["publication", "route", "prompt", "schema", "input"])
async def test_dispatch_store_rejects_request_outside_accepted_configuration(admitted, damage):
    kit, _, _, receipt, _, _, _ = admitted
    claim = await kit.store.claim(60)
    evidence = await kit.store.evidence(claim)
    config = await MySQLExecutionConfigurations(kit.transactions).get(claim, evidence)
    prepared = prepare_explanation(claim.session, evidence, config.release, config.package)
    request = FrozenGeneration(
        prepared,
        config.route,
        config.schema,
        publication_id=config.publication_id,
        manifest_fingerprint=config.manifest_fingerprint,
    )
    if damage == "publication":
        request = replace(request, publication_id=str(uuid4()))
    elif damage == "route":
        request = replace(request, route=replace(request.route, model="other"))
    elif damage == "prompt":
        request = replace(
            request,
            prepared=replace(
                prepared, messages=replace(prepared.messages, system_message="other instructions")
            ),
        )
    elif damage == "schema":
        request = replace(request, schema={})
    else:
        request = replace(
            request,
            prepared=replace(
                prepared, assembled_input=replace(prepared.assembled_input, canonical_json="{}")
            ),
        )
    # A caller cannot bypass the stored session's policy by changing the claim marker.
    claim = replace(claim, session=replace(claim.session, workflow_version="qs-snapshot-v1"))
    with pytest.raises(ProviderFailure, match="configuration_invalid"):
        await kit.store.begin_model_call(claim, JSONModelCallCodec().encode_request(request))
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(model_calls.c.run_id).where(model_calls.c.run_id == receipt.run_id)
            )
            is None
        )


@pytest.mark.parametrize("damage", ["binding", "artifact"])
async def test_artifact_acceptance_rechecks_original_configuration(admitted, damage):
    kit, service, _, receipt, _, _, _ = admitted
    claim = await kit.store.claim(60)
    evidence = await kit.store.evidence(claim)
    model = Model()
    result = await workflow(kit, model).execute(claim, evidence)
    assert result.artifact is not None
    if damage == "binding":
        async with kit.transactions.open() as db:
            await db.execute(
                update(execution_configurations)
                .where(execution_configurations.c.session_id == receipt.session_id)
                .values(publication_sha256="f" * 64)
            )
            await db.commit()
    else:
        result = replace(result, artifact=replace(result.artifact, profile_version="other"))
    with pytest.raises(ValueError):
        await kit.store.finish(claim, result)
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(artifacts.c.id).where(artifacts.c.session_id == receipt.session_id)
            )
            is None
        )
    assert (await service.get(kit.actor, receipt.session_id)).session.status == "running"
    assert model.calls == 1


async def test_concurrent_pointer_change_does_not_mix_admission_snapshot(admitted, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import execution_configurations as implementation

    kit, service, _, _, evidence, first, (tx, scope, command, at) = admitted
    selected, proceed = asyncio.Event(), asyncio.Event()
    original = implementation.load_pointer

    async def pause_after_pointer_read(db, row):
        selected.set()
        await asyncio.wait_for(proceed.wait(), 10)
        return await original(db, row)

    monkeypatch.setattr(implementation, "load_pointer", pause_after_pointer_read)
    pending = asyncio.create_task(
        service.start_external(kit.actor, "7", ("42",), "并发接单", str(uuid4()), evidence.items)
    )
    try:
        await asyncio.wait_for(selected.wait(), 10)
        await MySQLPublications(tx).apply(
            scope,
            replace(
                command,
                command_id=uuid4(),
                expected_version=1,
                expected_active_id=first.change.current.active.publication_id,
            ),
            at + timedelta(seconds=1),
        )
    finally:
        proceed.set()
        receipt = await asyncio.wait_for(pending, 10)
    async with tx.open() as db:
        binding = (
            (
                await db.execute(
                    select(execution_configurations).where(
                        execution_configurations.c.session_id == receipt.session_id
                    )
                )
            )
            .mappings()
            .one()
        )
        assert binding["publication_id"] == str(first.change.current.active.publication_id)
        assert binding["pointer_version"] == 1


async def test_manual_retry_preserves_accepted_publication_and_completes_original_session(admitted):
    from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
    from qs_ai.application.execution.retry import ParticipantRetry, RetryParticipant
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.participant_retries import MySQLParticipantRetries
    from qs_ai.infrastructure.persistence.mysql.schema import result_outbox

    kit, service, key, accepted, _, first, (tx, scope, command, at) = admitted
    failed = Model(ProviderFailure("provider_timeout", result_unknown=True))
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, failed)).once()
    before = (await service.get(kit.actor, accepted.session_id)).session
    assert before.status == "blocked"
    async with tx.open() as db:
        original = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == accepted.run_id)))
            .mappings()
            .one()
        )
    await MySQLPublications(tx).apply(
        scope,
        replace(
            command,
            command_id=uuid4(),
            expected_version=1,
            expected_active_id=first.change.current.active.publication_id,
        ),
        at + timedelta(seconds=1),
    )
    retry = RetryParticipant(MySQLParticipantRetries(tx, ParticipantCapacityPolicy()), kit.source)
    receipt = await retry.execute(
        ParticipantRetry(
            DraftScope(1, 42),
            before.id,
            str(uuid4()),
            before.active_run_id,
            before.version,
            "确认未知调用风险后重试原发布",
            True,
            1,
            True,
        )
    )
    model = Model()
    assert await ExecuteNext(kit.store, kit.source, workflow(kit, model)).once()
    after = (await service.get(kit.actor, accepted.session_id)).session
    assert after.status == "completed" and after.active_run_id == receipt.run_id
    async with tx.open() as db:
        old = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == accepted.run_id)))
            .mappings()
            .one()
        )
        new = (
            (await db.execute(select(model_calls).where(model_calls.c.run_id == receipt.run_id)))
            .mappings()
            .one()
        )
        event = await db.scalar(
            select(result_outbox.c.payload).where(
                result_outbox.c.session_id == after.id, result_outbox.c.version == after.version
            )
        )
    assert old == original and old["status"] == "unknown"
    assert (
        new["request_json"] == old["request_json"] and new["invocation_id"] != old["invocation_id"]
    )
    assert json.loads(new["request_json"])["publication_id"] == str(
        first.change.current.active.publication_id
    )
    assert event["request_id"] == key and event["status"] == "completed" and event["artifact_json"]
    assert failed.calls == model.calls == 1
