"""Actual evaluation business/sequence/archive/witness roots; no Broker or real model calls."""

import asyncio
import hashlib
import os
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import delete, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, inbox, outbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_capacity_reservations,
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
)
from qs_ai.infrastructure.workflow_transport.evaluation_planner import EvaluationAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS, authenticate
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transaction_admission import make_evaluation_root
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_runs import create
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import admission, command
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_start_admission import reserve, rows

pytestmark = pytest.mark.integration


@pytest.fixture
async def original(host, requested):
    tx, scope, store = requested
    try:
        yield host, scope, store
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )
            await db.commit()


@pytest.fixture
async def trigger_admin(host):
    # Fault installation is isolated administrative DDL; business retains the
    # original application account and original pool throughout the test.
    configured = os.environ.get("QS_MQ_FAULT_DDL_MYSQL_URL")
    if not configured:
        pytest.fail("Required isolated fault DDL administrator missing")
    url = make_url(configured)
    primary = host.tx.database.engine.url
    if (
        url.drivername != "mysql+asyncmy"
        or url.host not in {"127.0.0.1", "localhost", "::1"}
        or (url.host, url.port or 3306, url.database)
        != (primary.host, primary.port or 3306, primary.database)
    ):
        pytest.fail("Fault administrator must target the same disposable primary")
    engine = create_async_engine(url, hide_parameters=True)
    try:
        yield engine
    finally:
        await engine.dispose()


def incoming(host, scope, kind=pb.EVALUATION_START, version=1, *, original_occurred_at=""):
    body = pb.MessagingBody()
    request = body.evaluation_start if kind == pb.EVALUATION_START else body.evaluation_cancel
    request.scope.CopyFrom(
        workflow.EvaluationQuery(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
        )
    )
    request.expected_version, request.reason, request.confirm = (
        version,
        "M3 original evaluation admission",
        True,
    )
    if kind == pb.EVALUATION_CANCEL:
        request.discard = False
    prepared = command(
        host.keys, kind, body, str(scope.run_id), original_occurred_at=original_occurred_at
    )
    header = authenticate(
        prepared.wire,
        COMMANDS,
        decrypt_keys={"ai.encrypt": host.keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", host.keys["qs.sign"])},
    )
    value = FrozenCommandInput.from_authenticated(header, prepared.body, prepared.wire)
    host.messages.append(value)
    return value


def planner(host):
    return EvaluationAdmissionPlanner(
        host.tx, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    )


def root(host, message, owned):
    return make_evaluation_root(
        host.tx,
        host.archives,
        host.store,
        MessagingStore(),
        message,
        owned.plan,
        admission(host.kit.source),
    )


@pytest.mark.parametrize("kind", [pb.EVALUATION_START, pb.EVALUATION_CANCEL])
async def test_actual_evaluation_business_first_wire_sequence_and_witness_commit_together(
    original, kind
):
    host, scope, store = original
    message = incoming(host, scope, kind)
    candidate = await planner(host).candidate(message)
    assert len(candidate.branches) == 2
    owned = (await reserve(host, message, candidate)).execution
    assert await root(host, message, owned)(owned) == LocalOutcome.COMMITTED
    view = await store.get(scope)
    assert (view.version, view.status) == (
        2,
        "collecting" if kind == pb.EVALUATION_START else "canceled",
    )
    sequence = await rows(
        host, evaluation_sequences, evaluation_sequences.c.run_id == str(scope.run_id)
    )
    assert len(sequence) == 1 and (sequence[0]["sequence"], sequence[0]["version"]) == (1, 2)
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
    for a in read_admission_plan(message, owned.plan):
        stored = await host.archives.read_committed(a.envelope().message_id)
        assert (
            stored is not None
            and hashlib.sha256(stored.wire).digest() == hashlib.sha256(a.wire).digest()
        )
        assert not await rows(host, outbox, outbox.c.message_id == a.envelope().message_id)
    assert (await rows(host, inbox, inbox.c.message_id == message.envelope().message_id))[0][
        "decision"
    ] == "accepted"


async def test_changed_original_checkpoint_cannot_commit_frozen_start_or_witness(original):
    host, scope, store = original
    message = incoming(host, scope)
    owned = (await reserve(host, message, await planner(host).candidate(message))).execution
    await store.cancel(
        scope, 1, "concurrent original cancellation", datetime.now(UTC), discard=False, confirm=True
    )
    assert await root(host, message, owned)(owned) == LocalOutcome.ABORTED
    assert (await store.get(scope)).status == "canceled"
    assert not await rows(host, inbox, inbox.c.message_id == message.envelope().message_id)
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.ROLLBACK


async def test_changed_event_sequence_aborts_business_and_every_archive(original):
    host, scope, store = original
    message = incoming(host, scope)
    owned = (await reserve(host, message, await planner(host).candidate(message))).execution
    async with host.tx.open() as db:
        await db.execute(
            insert(evaluation_sequences).values(run_id=str(scope.run_id), sequence=7, version=1)
        )
        await db.commit()
    assert await root(host, message, owned)(owned) == LocalOutcome.ABORTED
    assert (await store.get(scope)).status == "requested"
    assert not await rows(
        host,
        evaluation_capacity_reservations,
        evaluation_capacity_reservations.c.run_id == str(scope.run_id),
    )
    for a in read_admission_plan(message, owned.plan):
        assert await host.archives.read_committed(a.envelope().message_id) is None


@pytest.mark.parametrize("fault", ["archive", "witness", "client_discarded_reply"])
async def test_evaluation_root_failure_keeps_original_commit_truth(
    original, trigger_admin, monkeypatch, fault
):
    host, scope, store = original
    message = incoming(host, scope)
    owned = (await reserve(host, message, await planner(host).candidate(message))).execution
    trigger = "eval_" + uuid4().hex
    from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive

    try:
        if fault != "client_discarded_reply":
            async with trigger_admin.begin() as db:
                statement = (
                    f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} "
                    "FOR EACH ROW SIGNAL SQLSTATE '45000' "
                    "SET MESSAGE_TEXT='isolated evaluation archive rejection'"
                    if fault == "archive"
                    else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness "
                    "FOR EACH ROW BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                    "SET MESSAGE_TEXT='isolated evaluation witness rejection'; END IF; END"
                )
                await db.execute(text(statement))
                await db.commit()
        else:
            original_commit = EventSession.commit

            async def discard_reply(db):
                await original_commit(db)
                # Client deliberately discards a completed reply; not a packet failpoint.
                raise ConnectionError("client discarded evaluation root commit reply")

            monkeypatch.setattr(EventSession, "commit", discard_reply)
        outcome = await root(host, message, owned)(owned)
        committed = fault == "client_discarded_reply"
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert (await store.get(scope)).status == ("collecting" if committed else "requested")
        for reference in owned.references:
            assert await StoreChecker(host.store).check(reference) == (
                Decision.COMMIT if committed else Decision.ROLLBACK
            )
        for a in read_admission_plan(message, owned.plan):
            assert (
                await host.archives.read_committed(a.envelope().message_id) is not None
            ) is committed
        assert (
            bool(
                await rows(
                    host, evaluation_sequences, evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )
            is committed
        )
    finally:
        monkeypatch.undo()
        if fault != "client_discarded_reply":
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
                await db.commit()


async def test_two_guarded_roots_share_one_original_organization_capacity_slot(original, setup_run):
    host, scope, store = original
    _, _, release = setup_run
    second = replace(scope, run_id=uuid4())
    try:
        async with host.tx.open() as db:
            await create(db, second.run_id, release)
            await db.commit()
        messages = [incoming(host, target) for target in (scope, second)]
        # Both read the original empty capacity before either business root runs.
        candidates = [await planner(host).candidate(message) for message in messages]
        assert all(len(candidate.branches) == 2 for candidate in candidates)
        owned = [
            (await reserve(host, message, candidate)).execution
            for message, candidate in zip(messages, candidates, strict=True)
        ]
        outcomes = await asyncio.gather(
            *(
                root(host, message, execution)(execution)
                for message, execution in zip(messages, owned, strict=True)
            )
        )
        assert sorted(outcome.value for outcome in outcomes) == sorted(
            [LocalOutcome.COMMITTED.value, LocalOutcome.ABORTED.value]
        )
        statuses = [(await store.get(target)).status for target in (scope, second)]
        assert sorted(statuses) == ["collecting", "requested"]
        reservations = []
        for target in (scope, second):
            reservations += await rows(
                host,
                evaluation_capacity_reservations,
                evaluation_capacity_reservations.c.run_id == str(target.run_id),
            )
        assert len(reservations) == 1
        for execution, outcome in zip(owned, outcomes, strict=True):
            for reference in execution.references:
                assert await StoreChecker(host.store).check(reference) == (
                    Decision.COMMIT if outcome == LocalOutcome.COMMITTED else Decision.ROLLBACK
                )
    finally:
        async with host.tx.open() as db:
            for table in (
                evaluation_sequences,
                evaluation_capacity_reservations,
                evaluation_dispatches,
                evaluation_checkpoints,
                evaluation_runs,
            ):
                await db.execute(delete(table).where(table.c.run_id == str(second.run_id)))
            await db.commit()


async def test_dispatched_evaluation_cancel_keeps_unknown_work_and_only_commits_drain_intent(
    host, dispatched
):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.infrastructure.persistence.mysql.evaluation_management import (
        MySQLEvaluationManagement,
    )

    _, run_id, *_ = dispatched
    scope = ManagementScope(run_id, 1, 42)
    store = MySQLEvaluationManagement(host.tx)
    before = (
        await rows(host, evaluation_checkpoints, evaluation_checkpoints.c.run_id == str(run_id))
    )[0]
    assert before["version"] == 5 and before["checkpoint_json"]["phase"] == "dispatching"
    try:
        message = incoming(host, scope, pb.EVALUATION_CANCEL, version=5)
        candidate = await planner(host).candidate(message)
        assert len(candidate.branches) == 2
        expected = pb.MessagingBody.FromString(
            read_admission_plan(message, candidate)[0].body
        ).command_receipt.evaluation_receipt
        assert (
            expected.status == "collecting" and expected.cancel_draining and expected.version == 6
        )
        owned = (await reserve(host, message, candidate)).execution
        assert await root(host, message, owned)(owned) == LocalOutcome.COMMITTED
        after = (
            await rows(host, evaluation_checkpoints, evaluation_checkpoints.c.run_id == str(run_id))
        )[0]
        assert after["version"] == 6
        assert (
            hashlib.sha256(str(after["checkpoint_json"]).encode()).digest()
            == hashlib.sha256(str(before["checkpoint_json"]).encode()).digest()
        )
        view = await store.get(scope)
        assert view.cancel_draining and view.status == "collecting" and not view.cancellation_json
        for reference in owned.references:
            assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
    finally:
        async with host.tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )
            await db.commit()
