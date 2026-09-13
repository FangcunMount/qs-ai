"""Retries preserve the original attempt and require current access and new capacity."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.retry import ParticipantRetry, RetryParticipant
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import AccessDenied, NotFound, WorkflowResult
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.leases import LeaseLost
from qs_ai.infrastructure.persistence.mysql.participant_retries import MySQLParticipantRetries
from qs_ai.infrastructure.persistence.mysql.schema import (
    model_calls,
    participant_retries,
    result_outbox,
    runs,
)
from tests.integration.test_generation import Gateway, request
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_participant_capacity import reservations, start

pytestmark = pytest.mark.integration


def service(kit, policy=None):
    return RetryParticipant(
        MySQLParticipantRetries(kit.transactions, policy or ParticipantCapacityPolicy()), kit.source
    )


async def blocked(kit, *, unknown=False, dispatch=True):
    accepted = await start(kit, kit.service)
    claim = await kit.store.claim(60)
    if dispatch:
        gateway = Gateway(ProviderFailure("provider_timeout", result_unknown=unknown))
        with pytest.raises(ProviderFailure):
            await DurableGeneration(kit.store, gateway, JSONModelCallCodec()).execute(
                claim, request()
            )
    await kit.store.finish(
        claim,
        WorkflowResult(
            "", failure_code="provider_result_unknown" if unknown else "provider_timeout"
        ),
    )
    state = await kit.service.get(kit.actor, accepted.session_id)
    command = ParticipantRetry(
        DraftScope(1, 42),
        accepted.session_id,
        str(uuid4()),
        claim.run_id,
        state.session.version,
        "重新执行原报告",
        True,
        1,
        unknown,
    )
    return command, claim


async def calls(kit, session_id):
    async with kit.transactions.open() as db:
        return (
            (
                await db.execute(
                    select(model_calls).where(
                        model_calls.c.run_id.in_(
                            select(runs.c.id).where(runs.c.session_id == session_id)
                        )
                    )
                )
            )
            .mappings()
            .all()
        )


async def test_retry_is_idempotent_and_keeps_original_model_request_and_result_correlation(kit):
    command, old_claim = await blocked(kit)
    previous_calls = await calls(kit, command.session_id)
    retry = service(kit)
    first, replay = await asyncio.gather(retry.execute(command), retry.execute(command))
    assert first == replay and first.status == "queued"
    assert first.run_id != command.expected_run_id and first.version == command.expected_version + 1
    assert await retry.receipt(command.scope, command.command_id) == first
    assert len(await reservations(kit)) == 2
    assert await calls(kit, command.session_id) == previous_calls
    async with kit.transactions.open() as db:
        audit = (
            (
                await db.execute(
                    select(participant_retries).where(participant_retries.c.run_id == first.run_id)
                )
            )
            .mappings()
            .one()
        )
        assert audit["source_run_id"] == command.expected_run_id
        assert audit["frozen_request_json"] == previous_calls[0]["request_json"]
        event = (
            await db.execute(
                select(result_outbox.c.payload).where(
                    result_outbox.c.session_id == command.session_id,
                    result_outbox.c.version == first.version,
                )
            )
        ).scalar_one()
        assert event["request_id"] == audit["request_id"] and event["status"] == "queued"
    with pytest.raises(LeaseLost):
        await kit.store.finish(old_claim, WorkflowResult("", failure_code="late_result"))
    new_claim = await kit.store.claim(60)
    assert new_claim.run_id == first.run_id
    gateway = Gateway()
    generation = DurableGeneration(kit.store, gateway, JSONModelCallCodec())
    changed = replace(request(), route=replace(request().route, model="changed-after-admission"))
    result = await generation.execute(new_claim, changed)
    assert result.request == request()
    assert result.response.invocation_id != previous_calls[0]["invocation_id"]
    assert await generation.execute(new_claim, changed) == result
    assert gateway.calls == 1
    assert previous_calls[0] in await calls(kit, command.session_id)


async def test_unknown_retry_requires_risk_confirmation_and_retains_unknown_history(kit):
    command, _ = await blocked(kit, unknown=True)
    before = await calls(kit, command.session_id)
    with pytest.raises(RuleViolation, match="participant_retry_unknown_risk_required"):
        await service(kit).execute(replace(command, accept_result_unknown_risk=False))
    assert len(await reservations(kit)) == 1
    result = await service(kit).execute(command)
    assert result.run_id != command.expected_run_id
    assert await calls(kit, command.session_id) == before
    assert before[0]["status"] == "unknown"


async def test_quota_denial_rolls_back_retry_and_does_not_change_the_failed_attempt(kit):
    command, _ = await blocked(kit, dispatch=False)
    with pytest.raises(RuleViolation, match="participant_daily_capacity_exceeded"):
        await service(kit, ParticipantCapacityPolicy(daily_user=1)).execute(command)
    assert len(await reservations(kit)) == 1
    state = await kit.service.get(kit.actor, command.session_id)
    assert state.session.status == "blocked" and state.session.version == command.expected_version
    with pytest.raises(NotFound):
        await service(kit).receipt(command.scope, command.command_id)
    result = await service(kit).execute(command)
    assert result.status == "queued"


async def test_current_participant_revocation_blocks_retry_and_receipt_replay(kit):
    command, _ = await blocked(kit, dispatch=False)
    retry = service(kit)
    kit.source.revoked = True
    with pytest.raises(AccessDenied):
        await retry.execute(command)
    assert len(await reservations(kit)) == 1
    kit.source.revoked = False
    result = await retry.execute(command)
    kit.source.revoked = True
    with pytest.raises(AccessDenied):
        await retry.execute(command)
    with pytest.raises(AccessDenied):
        await retry.receipt(command.scope, command.command_id)
    assert result.run_id != command.expected_run_id and len(await reservations(kit)) == 2


async def test_retry_scope_command_identity_and_stale_attempt_are_checked(kit):
    command, _ = await blocked(kit, dispatch=False)
    retry = service(kit)
    with pytest.raises(NotFound):
        await retry.execute(replace(command, scope=DraftScope(2, 42)))
    results = await asyncio.gather(
        retry.execute(command),
        retry.execute(replace(command, command_id=str(uuid4()))),
        return_exceptions=True,
    )
    assert sum(isinstance(r, RuleViolation) for r in results) == 1
    assert len(await reservations(kit)) == 2
    # Obtain the winning command by reading the immutable lineage, not assuming scheduling order.
    async with kit.transactions.open() as db:
        winner = await db.scalar(
            select(participant_retries.c.command_id).where(
                participant_retries.c.session_id == command.session_id
            )
        )
    with pytest.raises(RuleViolation, match="idempotency_conflict"):
        await retry.execute(replace(command, command_id=winner, reason="changed command"))
    with pytest.raises(NotFound):
        await retry.receipt(DraftScope(1, 99), winner)


async def test_retry_that_fails_before_dispatch_keeps_the_earliest_frozen_request(kit):
    command, _ = await blocked(kit)
    first = await service(kit).execute(command)
    claim = await kit.store.claim(60)
    await kit.store.finish(claim, WorkflowResult("", failure_code="access_revoked"))
    current = await kit.service.get(kit.actor, command.session_id)
    second = await service(kit).execute(
        replace(
            command,
            command_id=str(uuid4()),
            expected_run_id=first.run_id,
            expected_version=current.session.version,
        )
    )
    new_claim = await kit.store.claim(60)
    assert new_claim.run_id == second.run_id
    gateway = Gateway()
    changed = replace(request(), route=replace(request().route, model="changed-twice"))
    result = await DurableGeneration(kit.store, gateway, JSONModelCallCodec()).execute(
        new_claim, changed
    )
    assert result.request == request() and gateway.calls == 1
    assert len(await reservations(kit)) == 3
