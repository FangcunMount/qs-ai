"""Actual MySQL read planning; no Broker, original-root commit or model invocation claim."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select

from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.infrastructure.persistence.mysql.schema import (
    jobs,
    model_calls,
    result_outbox,
    runs,
    sessions,
)
from qs_ai.infrastructure.persistence.mysql.start_admission import ReadOnlyStartUnitOfWork
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from tests.integration.test_interpretation import kit as kit
from tests.test_input_binding import bound_case

pytestmark = pytest.mark.integration


async def counts(kit):
    async with kit.transactions.open() as db:
        return tuple(
            [
                await db.scalar(select(func.count()).select_from(table))
                for table in (sessions, runs, jobs, result_outbox, model_calls)
            ]
        )


async def planned_start(kit, expected_code):
    facts = StartPlanFacts(b"x" * 32, datetime.now(UTC), *(str(uuid4()) for _ in range(6)))
    request_id = str(uuid4())
    baseline = await counts(kit)
    captured = []
    engine = kit.transactions.database.engine.sync_engine

    def capture(connection, cursor, statement, parameters, context, executemany):
        captured.append((statement.lstrip().split()[0].upper(), "FOR UPDATE" in statement.upper()))

    async with kit.transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        uow = ReadOnlyStartUnitOfWork(db, facts)
        event.listen(engine, "before_cursor_execute", capture)
        try:
            receipt = await InterpretationService(
                uow, kit.source, new_identity=facts.identity
            ).start_external(
                kit.actor,
                "7",
                ("42",),
                "read-only original START",
                request_id,
                bound_case()[1].items,
            )
        finally:
            event.remove(engine, "before_cursor_execute", capture)
    assert captured and all(kind == "SELECT" and not locked for kind, locked in captured)
    assert await counts(kit) == baseline
    assert receipt.session_id == facts.session_id and receipt.run_id == facts.run_id
    assert uow.state.event_id == facts.state_event_id and uow.state.request_id == request_id
    if expected_code:
        assert receipt.status == "blocked" and receipt.version == 4
        assert uow.state.failure_code == expected_code and uow.configuration is None
    else:
        assert receipt.status == "queued" and receipt.version == 2
        assert uow.configuration is not None
        assert uow.configuration.session_id == facts.session_id
        assert uow.configuration.evidence_set_id == facts.evidence_id
    assert not uow.previous


@pytest.mark.usefixtures("published_configuration")
async def test_published_start_planning_only_reads_and_creates_no_business_or_publication_row(kit):
    await planned_start(kit, None)


async def test_missing_configuration_planning_retains_original_refusal_without_persisting_it(kit):
    await planned_start(kit, "configuration_unavailable")
