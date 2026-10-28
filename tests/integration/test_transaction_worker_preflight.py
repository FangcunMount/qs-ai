"""Actual worker/preflight/MySQL roots with database-only runtime; no half/Broker claim."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, Result, StoreChecker
from sqlalchemy import select, text

from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_dispatch import (
    PreflightStateDispatcher,
    PreflightTransactionPending,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    PreflightOperation,
    preflight_intent,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.integration.test_transaction_evaluation_admission import (
    host as host,
)
from tests.integration.test_transaction_evaluation_admission import (
    keys as keys,
)
from tests.integration.test_transaction_evaluation_admission import (
    kit as kit,
)
from tests.integration.test_transaction_evaluation_admission import (
    original as original,
)
from tests.integration.test_transaction_evaluation_admission import (
    requested as requested,
)
from tests.integration.test_transaction_evaluation_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_evaluation_admission import (
    setup_run as setup_run,
)
from tests.integration.test_transaction_evaluation_admission import (
    trigger_admin as trigger_admin,
)
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


class DatabaseOnlyStateRuntime:
    """No sender, half message or final Broker confirmation is provided here."""

    routing = legacy_routes()

    def __init__(self, host):
        self.host, self.store, self.calls, self.before = host, host.store, 0, None

    async def reserve(self, request, candidate):
        value = await self.store.reserve(request, candidate)
        if value.acquired:
            self.host.executions.append(value.execution)
        return value

    async def execute(self, execution, local):
        self.calls += 1
        if self.before is not None:
            await self.before()
        return Result(await local(execution), ())


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "archive_rejected",
        "client_reply_discarded",
        "proof_temporarily_unavailable",
        "source_changed",
    ],
)
async def test_actual_worker_commits_frozen_preflight_with_state_witness_and_no_publish_outbox(
    original, trigger_admin, monkeypatch, fault
):
    host, scope, management = original
    await management.start(
        scope, 1, "explicit worker preflight seed", datetime.now(UTC), confirm=True
    )
    runtime = DatabaseOnlyStateRuntime(host)
    dispatcher = PreflightStateDispatcher(
        host.tx,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )
    gateway = SimpleNamespace(
        generate_messages=AsyncMock(side_effect=AssertionError("Preflight cannot call a model"))
    )
    worker = EvaluationWorker(
        host.tx,
        gateway,
        Mock(),
        Mock(),
        "worker:state-native",
        enabled=True,
        preflight_dispatch=dispatcher,
    )
    previous_recorder = host.tx.database.state_events
    recorder = SimpleNamespace(
        flush=AsyncMock(side_effect=AssertionError("New state cannot use legacy commit hook"))
    )
    host.tx.database.state_events = recorder
    trigger = None
    committed = fault in ("none", "client_reply_discarded", "proof_temporarily_unavailable")
    try:
        if fault == "archive_rejected":
            trigger = "preflight_archive_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} "
                        "FOR EACH ROW SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated preflight archive refusal'"
                    )
                )
        elif fault in ("client_reply_discarded", "proof_temporarily_unavailable"):
            real_commit = EventSession.commit

            async def discard(db):
                await real_commit(db)
                raise ConnectionError("client discarded completed preflight commit reply")

            monkeypatch.setattr(EventSession, "commit", discard)
            if fault == "proof_temporarily_unavailable":
                monkeypatch.setattr(
                    host.store,
                    "evidence",
                    AsyncMock(side_effect=ConnectionError("isolated proof unavailable")),
                )
        elif fault == "source_changed":

            async def cancel():
                await management.cancel(
                    scope,
                    2,
                    "explicit concurrent cancel",
                    datetime.now(UTC),
                    discard=False,
                    confirm=True,
                )

            runtime.before = cancel
        if fault in ("archive_rejected", "source_changed", "proof_temporarily_unavailable"):
            with pytest.raises(PreflightTransactionPending):
                await worker.once()
        else:
            assert await worker.once()
        monkeypatch.undo()
        intent = preflight_intent(scope.run_id, 2, scope.organization_id)
        reserved = await host.store.lookup(intent.request)
        assert reserved is not None and runtime.calls == 1
        facts, value = read_evaluation_state_plan(
            intent, reserved.execution.plan, route_resolver=legacy_routes()
        )
        original = PreflightOperation.from_facts(facts)
        assert original.evidence.provider_call_count == 0
        async with host.tx.open() as db:
            row = (
                (
                    await db.execute(
                        select(evaluation_runs).where(evaluation_runs.c.run_id == intent.run_id)
                    )
                )
                .mappings()
                .one()
            )
            version = await db.scalar(
                select(evaluation_checkpoints.c.version).where(
                    evaluation_checkpoints.c.run_id == intent.run_id
                )
            )
            if committed:
                assert (
                    version == 3
                    and row["progress_json"]["preflight"]["evaluated_at"]
                    == original.evidence.evaluated_at.isoformat()
                )
            else:
                assert version == (3 if fault == "source_changed" else 2)
                assert "preflight" not in row["progress_json"]
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == value.envelope().message_id
                    )
                )
            ).all()
        assert await StoreChecker(host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        assert await host.archives.read_committed(value.envelope().message_id) == (
            value if committed else None
        )
        if committed:
            dispatcher._planner.candidate = AsyncMock(
                side_effect=AssertionError("Committed original hint cannot plan again")
            )
            await dispatcher(scope.run_id, 2, scope.organization_id)
            assert runtime.calls == 1
            dispatcher._planner.candidate.assert_not_called()
        gateway.generate_messages.assert_not_called()
        recorder.flush.assert_not_called()
    finally:
        host.tx.database.state_events = previous_recorder
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text("DROP TRIGGER " + trigger))
