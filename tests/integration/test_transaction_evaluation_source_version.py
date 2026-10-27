"""Real source-bound original root proof, not a complete terminal Sender or Broker."""

import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4, uuid5

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import insert, select

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import EventSession, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import plan_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from tests.integration.test_transaction_evaluation_admission import host as host
from tests.integration.test_transaction_evaluation_admission import keys as keys
from tests.integration.test_transaction_evaluation_admission import kit as kit
from tests.integration.test_transaction_evaluation_admission import original as original
from tests.integration.test_transaction_evaluation_admission import requested as requested
from tests.integration.test_transaction_evaluation_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_evaluation_admission import setup_run as setup_run
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration
_REASON = "explicit isolated authoritative source proof"


@pytest.mark.parametrize(
    "fault", ["none", "rollback", "source_changed", "client_reply_discarded", "multi_cas"]
)
async def test_explicit_source_context_keeps_stable_input_without_relaxing_original_cas(
    original,
    monkeypatch,
    fault,
):
    host, scope, store = original
    before = await store.get(scope)
    assert before.version == 1
    async with host.tx.open() as db:
        previous = (
            (
                await db.execute(
                    select(evaluation_sequences).where(
                        evaluation_sequences.c.run_id == str(scope.run_id)
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if previous is None:
            await db.execute(
                insert(evaluation_sequences).values(run_id=str(scope.run_id), sequence=1, version=1)
            )
        else:
            assert (previous["sequence"], previous["version"]) == (1, 1)
        await db.commit()
    # The operation identity remains at its original version0; authority for this
    # exact source1 is explicit host input, not granted by the context field.
    intent = EvaluationStateIntent(
        str(scope.organization_id),
        str(scope.run_id),
        "source-bound/start-cancel/0" if fault == "multi_cas" else "source-bound/cancel/0",
        0,
        hashlib.sha256(
            json.dumps([scope.actor, _REASON, 1, False, True, fault == "multi_cas"]).encode()
        ).digest(),
    )
    target_version = 3 if fault == "multi_cas" else 2
    at = datetime.now(UTC)
    async with host.tx.open() as db:
        state, progress = await plan_cancellation(
            db, scope, 1, _REASON, at, discard=False, confirm=True, lock=False
        )
        row = (
            (
                await db.execute(
                    select(evaluation_runs).where(evaluation_runs.c.run_id == str(scope.run_id))
                )
            )
            .mappings()
            .one()
        )
        facts = EvaluationStateFacts(
            intent, at, 1, 1, b"explicit original source1 authority", source_version=1
        )
        visible = evaluation_state(
            str(scope.run_id),
            {**row, "version": target_version, "progress_json": progress},
            facts.sequence,
        )
    assert facts.sequence == 2 and intent.expected_version == 0
    first = prepare(
        pb.EVALUATION_STATE,
        str(uuid5(UUID(intent.run_id), f"state:{visible.version}")),
        intent.run_id,
        pb.MessagingBody(evaluation_state=visible),
        organization_id=intent.organization_id,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
    )
    plan = freeze_evaluation_state_plan(facts, str(uuid4()), first, route_resolver=legacy_routes())
    owned = await host.store.reserve(intent.request, plan)
    assert owned.acquired
    host.executions.append(owned.execution)
    if fault == "source_changed":
        await store.start(scope, 1, "explicit competing original source", at, confirm=True)
    if fault == "client_reply_discarded":
        commit = EventSession.commit

        async def discard(db):
            await commit(db)
            raise ConnectionError("client discarded original source-bound commit reply")

        monkeypatch.setattr(EventSession, "commit", discard)
    calls = 0

    async def business(db, frozen):
        nonlocal calls
        calls += 1
        assert frozen == facts and frozen.source_version == 1
        management = MySQLEvaluationManagement(Transactions.borrowed(db))
        version = frozen.source_version
        if fault == "multi_cas":
            await management.start(scope, version, _REASON, at, confirm=True)
            version += 1
        await management.cancel(scope, version, _REASON, at, discard=False, confirm=True)
        if fault == "rollback":
            raise RuntimeError("explicit original root refusal")

    root = EvaluationStateRoot(
        host.tx,
        host.archives,
        host.store,
        intent,
        owned.execution.plan,
        business,
        route_resolver=legacy_routes(),
    )
    outcome = await root(owned.execution)
    committed = fault in ("none", "client_reply_discarded", "multi_cas")
    assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED) and calls == 1
    assert await StoreChecker(host.store).check(owned.execution.references[0]) == (
        Decision.COMMIT if committed else Decision.ROLLBACK
    )
    actual = await store.get(scope)
    assert (actual.status, actual.version) == (
        ("canceled", target_version)
        if committed
        else ("collecting", 2)
        if fault == "source_changed"
        else (before.status, before.version)
    )
    _, archive = read_evaluation_state_plan(intent, plan, route_resolver=legacy_routes())
    assert await host.archives.read_committed(first.envelope.message_id) == (
        archive if committed else None
    )
    async with host.tx.open() as db:
        sequence = (
            (
                await db.execute(
                    select(evaluation_sequences).where(
                        evaluation_sequences.c.run_id == intent.run_id
                    )
                )
            )
            .mappings()
            .one()
        )
        assert (sequence["sequence"], sequence["version"]) == (
            (2, target_version) if committed else (1, 1)
        )
