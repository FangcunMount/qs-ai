"""Original non-command cancellation/state roots; no Broker or production wiring claim."""

import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4, uuid5

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import insert, select, text

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import EventSession, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import plan_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
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
_REASON = "explicit original state cancellation"


async def planned(host, scope):
    # The stable input is the original explicit command, never a mutable current row.
    intent = EvaluationStateIntent(
        str(scope.organization_id),
        str(scope.run_id),
        "cancel/1",
        1,
        hashlib.sha256(json.dumps([scope.actor, _REASON, False, True]).encode()).digest(),
    )
    at = datetime.now(UTC)
    async with host.tx.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
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
        facts = EvaluationStateFacts(
            intent,
            at,
            previous["sequence"] if previous else 0,
            previous["version"] if previous else 0,
        )
        visible = evaluation_state(
            str(scope.run_id),
            {**row, "progress_json": progress, "version": state.version},
            facts.sequence,
        )
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
    winner = freeze_evaluation_state_plan(
        facts, str(uuid4()), first, route_resolver=legacy_routes()
    )
    reservation = await host.store.reserve(intent.request, winner)
    assert reservation.acquired
    host.executions.append(reservation.execution)
    return intent, reservation.execution


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "archive_rejected",
        "witness_rejected",
        "client_reply_discarded",
        "source_changed",
        "sequence_changed",
    ],
)
async def test_state_only_original_business_sequence_archive_and_witness_commit_together(
    original, trigger_admin, monkeypatch, fault
):
    host, scope, store = original
    before = await store.get(scope)
    intent, owned = await planned(host, scope)
    facts, value = read_evaluation_state_plan(intent, owned.plan, route_resolver=legacy_routes())
    trigger = None
    if fault in ("archive_rejected", "witness_rejected"):
        trigger = "state_fault_" + uuid4().hex
        sql = (
            f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated state archive rejection'"
            if fault == "archive_rejected"
            else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
            "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='isolated state witness rejection'; END IF; END"
        )
        async with trigger_admin.begin() as db:
            await db.execute(text(sql))
    elif fault == "client_reply_discarded":
        real_commit = EventSession.commit

        async def discard(db):
            await real_commit(db)
            raise ConnectionError("client discarded completed state root reply")

        monkeypatch.setattr(EventSession, "commit", discard)
    elif fault == "source_changed":
        await store.start(scope, 1, "authorized concurrent start", facts.at, confirm=True)
    elif fault == "sequence_changed":
        async with host.tx.open() as db, db.begin():
            await db.execute(
                insert(evaluation_sequences).values(run_id=intent.run_id, sequence=1, version=1)
            )

    calls = 0

    async def business(db, frozen):
        nonlocal calls
        calls += 1
        assert frozen == facts
        await MySQLEvaluationManagement(Transactions.borrowed(db)).cancel(
            scope, frozen.intent.expected_version, _REASON, frozen.at, discard=False, confirm=True
        )

    try:
        root = EvaluationStateRoot(
            host.tx,
            host.archives,
            host.store,
            intent,
            owned.plan,
            business,
            route_resolver=legacy_routes(),
        )
        outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert calls == 1
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        actual = await store.get(scope)
        assert (actual.status, actual.version) == (
            ("canceled", 2)
            if committed
            else ("collecting", 2)
            if fault == "source_changed"
            else (before.status, before.version)
        )
        archived = await host.archives.read_committed(value.envelope().message_id)
        assert archived == (value if committed else None)
        async with host.tx.open() as db:
            current = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == intent.run_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            assert (None if current is None else (current["sequence"], current["version"])) == (
                (1, 2) if committed else (1, 1) if fault == "sequence_changed" else None
            )
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == value.envelope().message_id
                    )
                )
            ).all()
            version = await db.scalar(
                select(evaluation_checkpoints.c.version).where(
                    evaluation_checkpoints.c.run_id == intent.run_id
                )
            )
            assert version == actual.version
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text("DROP TRIGGER " + trigger))
