"""Original correction roots and replay/MySQL evidence; no Broker/network fault claim."""

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, LocalOutcome, Result, StoreChecker
from sqlalchemy import delete, select, text

from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.correction_state import (
    CorrectionStatePlanner,
    CorrectionStateRoot,
    read_correction_plan,
)
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
    GovernanceStateDispatcher,
    GovernanceTransactionPending,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_review_corrections import intent
from tests.integration.test_evaluation_review_corrections import signatures as signatures
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


async def clean_sequence(host, scope):
    async with host.tx.open() as db, db.begin():
        await db.execute(
            delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(scope.run_id))
        )


@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "client_reply_discarded", "source_changed"]
)
async def test_original_correction_audit_sequence_archive_and_witness_commit_atomically(
    host, signatures, trigger_admin, monkeypatch, fault
):
    tx, scope, version, _, _, original = signatures
    args = intent(signatures)
    operation = CorrectionStateOperation(**args)
    before, evidence = await rows(tx, scope.run_id), await outputs(tx, scope.run_id)
    candidate = await CorrectionStatePlanner(
        host.tx, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    ).candidate(operation, operation.value.reviewed_at)
    assert candidate is not None
    reserved = await host.store.reserve(operation.intent.request, candidate)
    assert reserved.acquired
    owned = reserved.execution
    host.executions.append(owned)
    _, first = read_correction_plan(operation, owned.plan)
    trigger = None
    try:
        if fault == "archive_rejected":
            trigger = "correction_fault_" + uuid4().hex
            async with trigger_admin.begin() as db:
                await db.execute(
                    text(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                        "SIGNAL SQLSTATE '45000' "
                        "SET MESSAGE_TEXT='isolated correction archive rejection'"
                    )
                )
        if fault == "source_changed":
            await original.correct_review(**{**args, "command_id": str(uuid4())})
        later = replace(
            operation,
            value=replace(
                operation.value, reviewed_at=operation.value.reviewed_at + timedelta(seconds=10)
            ),
        )
        root = CorrectionStateRoot(host.tx, host.archives, host.store, later, owned.plan)
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                actual = EventSession.commit

                async def discarded(db):
                    await actual(db)
                    raise ConnectionError("client discarded completed correction commit reply")

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(owned)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(host.store).check(owned.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        current = await rows(tx, scope.run_id)
        if committed or fault == "source_changed":
            assert current[2]["version"] == version + 1
            record = current[0]["progress_json"]["review_corrections"][0]
            assert record["review"]["reviewed_at"] == operation.value.reviewed_at.isoformat()
            assert record["review"]["reason"] == operation.value.reason
            if committed:
                assert record["command_id"] == operation.command_id
        else:
            assert current == before
        assert await outputs(tx, scope.run_id) == evidence
        assert await host.archives.read_committed(first.envelope().message_id) == (
            first if committed else None
        )
        async with host.tx.open() as db:
            sequence = (
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
            assert (None if sequence is None else (sequence["sequence"], sequence["version"])) == (
                (1, version + 1) if committed else None
            )
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id == first.envelope().message_id
                    )
                )
            ).all()
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))
        await clean_sequence(host, scope)


class DatabaseOnlyRuntime:
    def __init__(self, host, *, reject=False):
        self.host, self.store, self.reject, self.calls = host, host.store, reject, 0

    async def reserve(self, request, candidate):
        value = await self.store.reserve(request, candidate)
        if value.acquired:
            self.host.executions.append(value.execution)
        return value

    async def execute(self, owned, local):
        if self.reject:
            from reliable_messaging.rocketmq_transaction import TransactionSendError

            raise TransactionSendError(
                "isolated_prepare_refused", Result(LocalOutcome.NOT_STARTED, ())
            )
        self.calls += 1
        return Result(await local(owned), ())


def dispatcher(host, runtime):
    return GovernanceStateDispatcher(
        host.tx,
        host.archives,
        runtime,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )


@pytest.mark.parametrize("legacy", [False, True])
async def test_correction_replay_after_finalization_keeps_first_identity_and_no_new_witness(
    host, signatures, legacy
):
    tx, scope, _, _, _, original = signatures
    args = intent(signatures)
    operation = CorrectionStateOperation(**args)
    runtime = DatabaseOnlyRuntime(host)
    send = dispatcher(host, runtime)
    try:
        corrected = (
            await original.correct_review(**args)
            if legacy
            else await send(operation, operation.value.reviewed_at)
        )
        previous = await host.store.lookup(operation.intent.request)
        assert (previous is None) == legacy
        finalized = await original.finalize(
            scope,
            corrected.version,
            True,
            "原审核后定稿",
            operation.value.reviewed_at + timedelta(seconds=1),
            confirm=True,
        )
        assert finalized.status == "approved"
        before, evidence = await rows(tx, scope.run_id), await outputs(tx, scope.run_id)
        again = replace(
            operation,
            value=replace(
                operation.value, reviewed_at=operation.value.reviewed_at + timedelta(seconds=5)
            ),
        )
        current = await send(again, again.value.reviewed_at)
        assert current == finalized and runtime.calls == (0 if legacy else 1)
        assert (
            await rows(tx, scope.run_id) == before and await outputs(tx, scope.run_id) == evidence
        )
        assert await host.store.lookup(operation.intent.request) == previous
        from qs_ai.application.evaluation.checkpoints import CheckpointConflict

        with pytest.raises(CheckpointConflict):
            changed = replace(operation, value=replace(operation.value, reason="不同授权输入"))
            await send(changed, changed.value.reviewed_at)
    finally:
        await clean_sequence(host, scope)


async def test_unprepared_original_correction_stays_pending_without_business_retry(
    host, signatures
):
    from reliable_messaging.rocketmq_transaction import TransactionSendError

    tx, scope, _, _, _, _ = signatures
    operation = CorrectionStateOperation(**intent(signatures))
    runtime = DatabaseOnlyRuntime(host, reject=True)
    send = dispatcher(host, runtime)
    before = await rows(tx, scope.run_id)
    with pytest.raises(TransactionSendError):
        await send(operation, operation.value.reviewed_at)
    owned = host.executions[-1]
    assert runtime.calls == 0 and await rows(tx, scope.run_id) == before
    assert await StoreChecker(host.store).check(owned.references[0]) == Decision.UNKNOWN
    with pytest.raises(GovernanceTransactionPending):
        await send(operation, operation.value.reviewed_at)
    assert (
        runtime.calls == 0 and len(host.executions) == 1 and await rows(tx, scope.run_id) == before
    )


@pytest.mark.parametrize("reject", [False, True])
async def test_actual_correction_handler_uses_original_root_only_after_preparation(
    host, signatures, reject
):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime
    from unittest.mock import Mock

    from qs_ai.application.evaluation.management import EvaluationManagementStore
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.infrastructure.persistence.mysql.evaluation_management import (
        MySQLEvaluationManagement,
    )
    from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
        TransactionalEvaluationManagement,
    )
    from qs_ai.transport.grpc.evaluation import EvaluationManagement
    from tests.test_grpc_commands import Aborted, Context

    tx, scope, version, _, _, _ = signatures
    args = intent(signatures)
    operation = CorrectionStateOperation(**args)
    runtime = DatabaseOnlyRuntime(host, reject=reject)
    send = dispatcher(host, runtime)
    new_id = Mock(side_effect=AssertionError("Existing command ID must be retained"))
    store = TransactionalEvaluationManagement(
        MySQLEvaluationManagement(host.tx), send, new_id=new_id
    )

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    request = pb.EvaluationReviewCorrectionCommand(
        scope=pb.EvaluationQuery(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
        ),
        command_id=operation.command_id,
        expected_version=version,
        role=operation.value.role,
        candidate_id=operation.value.candidate_id,
        decision=operation.value.decision,
        reason=operation.value.reason,
        confirm=True,
        previous_review_fingerprint=operation.previous_review_fingerprint,
        candidate_output_fingerprint=operation.candidate_output_fingerprint,
    )
    before = await rows(tx, scope.run_id)
    at = datetime.now(UTC)
    try:
        if reject:
            with pytest.raises(Aborted):
                await EvaluationManagement(container).CorrectReview(request, Context())
            assert runtime.calls == 0 and await rows(tx, scope.run_id) == before
            assert (
                await StoreChecker(host.store).check(host.executions[-1].references[0])
                == Decision.UNKNOWN
            )
        else:
            response = await EvaluationManagement(container).CorrectReview(request, Context())
            assert response.version == version + 1 and runtime.calls == 1
            frozen, _ = read_correction_plan(operation, host.executions[-1].plan)
            first, _ = CorrectionStateOperation.from_facts(frozen)
            assert at <= first.value.reviewed_at <= datetime.now(UTC)
            current = await rows(tx, scope.run_id)
            assert (
                current[0]["progress_json"]["review_corrections"][0]["review"]["reviewed_at"]
                == first.value.reviewed_at.isoformat()
            )
            assert (
                await StoreChecker(host.store).check(host.executions[-1].references[0])
                == Decision.COMMIT
            )
        new_id.assert_not_called()
    finally:
        await clean_sequence(host, scope)
