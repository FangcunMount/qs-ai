"""Capacity is enforced through actual admission/claim/terminal transactions."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.interpretation.commands import CancelCommand
from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.schema import jobs, result_outbox, sessions
from qs_ai.infrastructure.persistence.mysql.schema import (
    participant_capacity_reservations as ledger,
)
from tests.integration.test_interpretation import expire
from tests.integration.test_interpretation import kit as kit
from tests.probes.session_inspection import read_session
from tests.test_input_binding import bound_case

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


def services(kit, policy):
    return (
        InterpretationService(MySQLUnitOfWorkFactory(kit.transactions, policy), kit.source),
        MySQLExecutionStore(kit.transactions, policy),
    )


async def start(kit, service, request_id=None):
    item = bound_case()[1].items[0]
    return await service.start_external(
        kit.actor, "7", ("42",), "容量验证", request_id or str(uuid4()), (item,)
    )


async def reservations(kit):
    async with kit.transactions.open() as db:
        return (
            (await db.execute(select(ledger).where(ledger.c.subject_id == kit.actor.subject_id)))
            .mappings()
            .all()
        )


@pytest.mark.parametrize("dimension", ["org", "user", "assessment"])
async def test_concurrent_admission_obeys_each_daily_dimension_atomically(kit, dimension):
    policy = replace(ParticipantCapacityPolicy(), **{"daily_" + dimension: 1})
    service, _ = services(kit, policy)
    results = await asyncio.gather(*(start(kit, service) for _ in range(3)), return_exceptions=True)
    assert all(not isinstance(v, Exception) for v in results)
    assert sum(v.status == "blocked" for v in results) == 2
    assert sum(v.status == "queued" for v in results) == 1
    assert len(await reservations(kit)) == 1
    async with kit.transactions.open() as db:
        assert (
            await db.execute(
                select(func.count())
                .select_from(sessions)
                .where(sessions.c.owner_subject_id == kit.actor.subject_id)
            )
        ).scalar_one() == 3
        payloads = (
            (
                await db.execute(
                    select(result_outbox.c.payload).where(
                        result_outbox.c.session_id.in_([v.session_id for v in results])
                    )
                )
            )
            .scalars()
            .all()
        )
        assert (
            sum(p["failure_code"] == "participant_daily_capacity_exceeded" for p in payloads) == 2
        )
        assert (
            await db.execute(
                select(func.count())
                .select_from(jobs)
                .where(jobs.c.session_id.in_([v.session_id for v in results]))
            )
        ).scalar_one() == 1


async def test_original_request_replay_reserves_one_call_and_cancel_does_not_refund(kit):
    service, _ = services(kit, ParticipantCapacityPolicy(daily_user=1))
    key = str(uuid4())
    original = await start(kit, service, key)
    assert await start(kit, service, key) == original
    await service.cancel(kit.actor, original.session_id, CancelCommand(original.version), "cancel")
    saved = await reservations(kit)
    assert len(saved) == 1 and not saved[0]["active"]
    denied_key = str(uuid4())
    denied = await start(kit, service, denied_key)
    assert denied.status == "blocked"
    assert await start(kit, service, denied_key) == denied
    assert await reservations(kit) == saved


@pytest.mark.parametrize("dimension", ["org", "user", "assessment"])
async def test_active_limit_defers_work_and_lease_recovery_reuses_slot(kit, dimension):
    policy = replace(
        ParticipantCapacityPolicy(active_org=3, active_user=3, active_assessment=3),
        **{"active_" + dimension: 1},
    )
    service, store = services(kit, policy)
    first = await start(kit, service)
    second = await start(kit, service)
    claim = await store.claim(60)
    assert claim.session.id == first.session_id
    assert await store.claim(60) is None
    queued = await read_session(service.uows, second.session_id)
    assert queued.session.status == "queued"
    before = await reservations(kit)
    assert sum(r["active"] for r in before) == 1
    await expire(kit, first.session_id)
    resumed = await store.claim(60)
    assert resumed.run_id == claim.run_id
    assert await reservations(kit) == before
    await store.finish(resumed, WorkflowResult("", failure_code="model_not_connected"))
    assert not any(r["active"] for r in await reservations(kit))
    async with kit.transactions.open() as db:
        await db.execute(
            update(jobs)
            .where(jobs.c.session_id == second.session_id)
            .values(available_at=func.utc_timestamp(6))
        )
        await db.commit()
    next_claim = await store.claim(60)
    assert next_claim.session.id == second.session_id
    assert sum(r["active"] for r in await reservations(kit)) == 1
    # A current cancellation releases the slot but preserves both daily receipts.
    await service.cancel(
        kit.actor, second.session_id, CancelCommand(next_claim.session.version), "cancel"
    )
    saved = await reservations(kit)
    assert len(saved) == 2 and not any(r["active"] for r in saved)


async def test_management_capacity_reads_scoped_limits_and_current_usage_without_mutation(kit):
    from dataclasses import asdict
    from datetime import UTC, datetime

    from qs_ai.application.execution.management import ParticipantCapacityQuery
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.infrastructure.persistence.mysql.participant_management import (
        MySQLParticipantCapacityReader,
    )

    policy = ParticipantCapacityPolicy()
    service, store = services(kit, policy)
    await start(kit, service)
    await store.claim(60)
    before = await reservations(kit)
    reader = MySQLParticipantCapacityReader(kit.transactions, policy)
    query = ParticipantCapacityQuery(DraftScope(1, 42), kit.actor.subject_id, "42")
    result = await reader.get(query, datetime.now(UTC))
    assert result.organization.daily_reserved == result.organization.active == 1
    assert result.subject.daily_remaining == 4
    assert result.assessment.daily_remaining == 2
    assert result.assessment.active_remaining == 0
    assert result.daily_reservations == result.active_reservations
    assert pb.ParticipantCapacitySnapshot(**asdict(result)).subject.identity == kit.actor.subject_id
    foreign = await reader.get(ParticipantCapacityQuery(DraftScope(2, 42)), datetime.now(UTC))
    assert foreign.organization.daily_reserved == foreign.organization.active == 0
    assert not foreign.daily_reservations and foreign.subject is None
    assert await reservations(kit) == before


async def test_online_quota_reaches_admission_without_restarting_service(kit):
    from datetime import UTC, datetime

    from sqlalchemy import delete

    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.application.governance.quotas import default_baseline
    from qs_ai.infrastructure.persistence.mysql.quotas import MySQLQuotas
    from qs_ai.infrastructure.persistence.mysql.schema import (
        organization_quota_commands,
        organization_quota_pointers,
        organization_quota_versions,
    )

    service, _ = services(kit, ParticipantCapacityPolicy())
    scope = DraftScope(int(kit.actor.org_id), 42)
    quotas = MySQLQuotas(kit.transactions, default_baseline())
    baseline = default_baseline().defaults
    lowered = replace(baseline, participant=replace(baseline.participant, daily_user=1))
    try:
        await quotas.apply(scope, uuid4(), 0, "验证在线额度", datetime.now(UTC), values=lowered)
        first = await start(kit, service)
        second = await start(kit, service)
        assert first.status == "queued"
        assert second.status == "blocked"
        rows = await reservations(kit)
        assert len(rows) == 1 and rows[0]["quota_snapshot"]["revision"] == 1
        await quotas.apply(scope, uuid4(), 1, "恢复额度", datetime.now(UTC), values=baseline)
        assert (await start(kit, service)).status == "queued"
        assert len(await reservations(kit)) == 2
    finally:
        async with kit.transactions.open() as db:
            for table in (
                organization_quota_commands,
                organization_quota_pointers,
                organization_quota_versions,
            ):
                await db.execute(
                    delete(table).where(table.c.organization_id == scope.organization_id)
                )
            await db.commit()
