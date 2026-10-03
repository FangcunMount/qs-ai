"""Real original admission, persistence and rollback; no real model or Broker calls."""

from dataclasses import asdict
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.publication import MovePublication
from qs_ai.application.governance.solution_models import DEFAULT_EDITABLE_MODELS
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging import (
    MessagingStore,
    inbox,
    outbox,
    quarantine,
)
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    idempotency,
    jobs,
    model_calls,
    result_outbox,
    runs,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import (
    StateEventRecorder,
    evaluation_sequences,
)
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_mq_storage import received, receiver
from tests.probes.p1_runtime import SyntheticEvidence
from tests.test_input_binding import bound_case

pytestmark = pytest.mark.integration


def admission(source):
    return WorkflowCommandAdmission(
        source,
        ParticipantCapacityPolicy(),
        EvaluationCapacityPolicy(),
        DEFAULT_EDITABLE_MODELS,
        None,
        EvaluationRuntimeLimits(),
    )


def command(keys, kind, body, aggregate, command_id=None):
    return prepare(
        kind,
        command_id or str(uuid4()),
        aggregate,
        body,
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )


@pytest.fixture
async def mq_env(kit, keys):
    tx = kit.transactions
    rec = StateEventRecorder(MessagingStore(), keys["ai.sign"], keys["qs.encrypt"])
    tx.database.state_events = rec
    try:
        yield rec
    finally:
        tx.database.state_events = None
        async with tx.open() as db:
            for table in (inbox, outbox, quarantine, evaluation_sequences):
                await db.execute(delete(table))
            await db.commit()


def start_message(kit, keys):
    identity = str(uuid4())
    item = bound_case()[1].items[0]
    return command(
        keys,
        pb.START,
        pb.MessagingBody(
            start=workflow.StartCommand(
                request_id=identity,
                actor=workflow.Actor(**asdict(kit.actor)),
                testee_id="7",
                assessment_ids=["42"],
                goal="MQ事务验收",
                evidence=[workflow.EvidenceItem(**asdict(item))],
            )
        ),
        identity,
        identity,
    )


async def saved(tx, table):
    async with tx.open() as db:
        return list((await db.execute(select(table))).mappings())


async def locked_result(db, event_id):
    return (
        (
            await db.execute(
                select(result_outbox).where(result_outbox.c.event_id == event_id).with_for_update()
            )
        )
        .mappings()
        .one()
    )


@pytest.mark.usefixtures("published_configuration")
async def test_start_change_and_duplicate_preserve_first_effect_receipt_and_wire(kit, keys, mq_env):
    tx = kit.transactions
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))
    message = start_message(kit, keys)
    await transport.receive_command(received(message))
    first = await saved(tx, outbox)
    assert len(first) == 2  # exact state event + acceptance receipt
    receipt = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt.workflow_receipt
        for r in first
        if r["kind"] == pb.COMMAND_RECEIPT
    )
    decision = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt
        for r in first
        if r["kind"] == pb.COMMAND_RECEIPT
    )
    assert decision.decision == pb.ACCEPTED and decision.code == ""
    assert receipt.status == "queued"
    original = next(
        r for r in await saved(tx, result_outbox) if r["session_id"] == receipt.session_id
    )
    state = next(r for r in first if r["kind"] == pb.INTERPRETATION_STATE)
    assert state["message_id"] == original["event_id"] and original["mq_owned"]
    assert await MySQLResultOutbox(tx).pending(100) == []
    await transport.receive_command(received(message))
    assert await saved(tx, outbox) == first
    identity = str(uuid4())
    cancel = command(
        keys,
        pb.CHANGE,
        pb.MessagingBody(
            change=workflow.ChangeCommand(
                command_id=identity,
                actor=workflow.Actor(**asdict(kit.actor)),
                session_id=receipt.session_id,
                expected_version=receipt.version,
                action="cancel",
            )
        ),
        message.envelope.aggregate_key,
        identity,
    )
    await transport.receive_command(received(cancel))
    after = await saved(tx, outbox)
    await transport.receive_command(received(cancel))
    assert await saved(tx, outbox) == after
    assert len(after) == 4
    assert len(await saved(tx, inbox)) == 2


async def test_unavailable_configuration_keeps_original_refusal_and_zero_dispatch(
    kit, keys, mq_env
):
    tx = kit.transactions
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))
    message = start_message(kit, keys)
    before_jobs, before_calls = await saved(tx, jobs), await saved(tx, model_calls)
    await transport.receive_command(received(message))
    first = await saved(tx, outbox)
    decision = next(
        pb.MessagingBody.FromString(row["body"]).command_receipt
        for row in first
        if row["kind"] == pb.COMMAND_RECEIPT
    )
    assert (decision.decision, decision.code, decision.grpc_status_code) == (
        pb.REJECTED,
        "configuration_unavailable",
        9,
    )
    receipt = decision.workflow_receipt
    assert receipt.status == "blocked"
    original = next(
        row for row in await saved(tx, result_outbox) if row["session_id"] == receipt.session_id
    )
    assert original["payload"]["failure_code"] == decision.code and original["mq_owned"]
    assert len([r for r in await saved(tx, runs) if r["session_id"] == receipt.session_id]) == 1
    before = [await saved(tx, table) for table in (sessions, runs, idempotency, result_outbox)]
    await transport.receive_command(received(message))
    assert await saved(tx, outbox) == first
    assert [
        await saved(tx, table) for table in (sessions, runs, idempotency, result_outbox)
    ] == before
    assert await saved(tx, jobs) == before_jobs and await saved(tx, model_calls) == before_calls
    assert len(await saved(tx, inbox)) == 1


async def test_start_refusal_replay_survives_publication_recovery(
    published_configuration, ready, kit, keys, mq_env
):
    tx, scope, command_value, at = ready
    active = published_configuration.change.current.active.publication_id
    await MySQLPublications(tx).apply(
        scope,
        MovePublication(uuid4(), command_value.selector, 1, active, "隔离停用", True, None),
        at + timedelta(seconds=1),
    )
    message = start_message(kit, keys)
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))
    await transport.receive_command(received(message))
    first = await saved(tx, outbox)
    rejected = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt
        for r in first
        if r["kind"] == pb.COMMAND_RECEIPT
    )
    assert rejected.decision == pb.REJECTED and rejected.code == "configuration_unavailable"
    await MySQLPublications(tx).apply(
        scope,
        MovePublication(uuid4(), command_value.selector, 2, None, "隔离恢复", True, active),
        at + timedelta(seconds=2),
    )
    before = [await saved(tx, table) for table in (sessions, runs, jobs, model_calls, idempotency)]
    # Also replay the original service idempotency receipt with no transport Inbox
    # shortcut: publication changes cannot turn its first refusal into acceptance.
    async with tx.open() as db:
        await db.begin()

        from qs_ai.infrastructure.workflow_transport.messaging import parse_body

        result = await admission(kit.source).admit(
            db, message.envelope, parse_body(message.envelope, message.body)
        )
        assert result.decision == pb.REJECTED and result.code == rejected.code
        assert result.workflow_receipt == rejected.workflow_receipt
        await db.commit()
    await transport.receive_command(received(message))
    assert await saved(tx, outbox) == first
    assert [
        await saved(tx, table) for table in (sessions, runs, jobs, model_calls, idempotency)
    ] == before
    # Restored publication applies only to a new original request identity.
    fresh = start_message(kit, keys)
    await transport.receive_command(received(fresh))
    accepted = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt
        for r in await saved(tx, outbox)
        if r["kind"] == pb.COMMAND_RECEIPT
        and pb.MessagingBody.FromString(r["body"]).command_receipt.command_id
        == fresh.envelope.message_id
    )
    assert accepted.decision == pb.ACCEPTED and accepted.workflow_receipt.status == "queued"
    assert await saved(tx, model_calls) == before[3]


async def test_refusal_receipt_failure_rolls_back_original_business_and_inbox(kit, keys, mq_env):
    tx = kit.transactions
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))
    tables = (sessions, runs, jobs, model_calls, idempotency, result_outbox, inbox, outbox)
    before = [await saved(tx, table) for table in tables]

    async def fail(*args):
        raise RuntimeError("injected refusal receipt persistence failure")

    transport._record_decision = fail
    with pytest.raises(RuntimeError, match="injected refusal receipt persistence failure"):
        await transport.receive_command(received(start_message(kit, keys)))
    assert [await saved(tx, table) for table in tables] == before
    assert len(await saved(tx, quarantine)) == 1  # technical attempt, no false business decision


@pytest.mark.usefixtures("published_configuration")
async def test_failure_after_real_effect_rolls_back_business_inbox_and_state(kit, keys, mq_env):
    tx = kit.transactions
    before = await saved(tx, sessions)
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))

    async def fail(*args):
        raise RuntimeError("injected completion failure")

    transport._record_decision = fail
    with pytest.raises(RuntimeError):
        await transport.receive_command(received(start_message(kit, keys)))
    assert await saved(tx, sessions) == before
    assert await saved(tx, inbox) == [] and await saved(tx, outbox) == []
    assert len(await saved(tx, quarantine)) == 1  # bounded technical counter, not business


@pytest.mark.usefixtures("published_configuration")
async def test_rejected_cas_rolls_back_local_command_reservation_but_commits_inbox(
    kit, keys, mq_env
):
    tx = kit.transactions
    transport = receiver(tx, MessagingStore(), keys, admission(kit.source))
    start = start_message(kit, keys)
    await transport.receive_command(received(start))
    first = await saved(tx, idempotency)
    accepted = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt.workflow_receipt
        for r in await saved(tx, outbox)
        if r["kind"] == pb.COMMAND_RECEIPT
    )
    identity = str(uuid4())
    invalid = command(
        keys,
        pb.CHANGE,
        pb.MessagingBody(
            change=workflow.ChangeCommand(
                command_id=identity,
                actor=workflow.Actor(**asdict(kit.actor)),
                session_id=accepted.session_id,
                expected_version=999,
                action="cancel",
            )
        ),
        start.envelope.aggregate_key,
        identity,
    )
    await transport.receive_command(received(invalid))
    assert await saved(tx, idempotency) == first
    decision = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt
        for r in await saved(tx, outbox)
        if r["kind"] == pb.COMMAND_RECEIPT
        and pb.MessagingBody.FromString(r["body"]).command_receipt.command_id == identity
    )
    assert decision.decision == pb.REJECTED and decision.grpc_status_code == 10
    assert len(await saved(tx, inbox)) == 2


async def test_evaluation_start_cancel_replay_and_final_projection(requested, kit, keys, mq_env):
    tx, scope, store = requested
    tx.database.state_events = mq_env
    try:
        transport = receiver(tx, MessagingStore(), keys, admission(SyntheticEvidence()))
        body = pb.MessagingBody(
            evaluation_start=workflow.EvaluationStartCommand(
                scope=workflow.EvaluationQuery(
                    run_id=str(scope.run_id), organization_id=1, operator_user_id=42
                ),
                expected_version=1,
                reason="MQ启动",
                confirm=True,
            )
        )
        start = command(keys, pb.EVALUATION_START, body, str(scope.run_id))
        await transport.receive_command(received(start))
        events = [r for r in await saved(tx, outbox) if r["kind"] == pb.EVALUATION_STATE]
        state = pb.MessagingBody.FromString(events[0]["body"]).evaluation_state
        assert (state.status, state.version, state.event_sequence) == ("collecting", 2, 1)
        first = await saved(tx, outbox)
        await transport.receive_command(received(start))
        assert await saved(tx, outbox) == first
        cancel = command(
            keys,
            pb.EVALUATION_CANCEL,
            pb.MessagingBody(
                evaluation_cancel=workflow.EvaluationCancelCommand(
                    scope=body.evaluation_start.scope,
                    expected_version=2,
                    reason="MQ取消",
                    confirm=True,
                    discard=False,
                )
            ),
            str(scope.run_id),
        )
        await transport.receive_command(received(cancel))
        events = [r for r in await saved(tx, outbox) if r["kind"] == pb.EVALUATION_STATE]
        states = sorted(
            (pb.MessagingBody.FromString(r["body"]).evaluation_state for r in events),
            key=lambda r: r.event_sequence,
        )
        assert [(r.status, r.version, r.event_sequence) for r in states] == [
            ("collecting", 2, 1),
            ("canceled", 3, 2),
        ]
        assert (await store.get(scope)).status == "canceled"
    finally:
        tx.database.state_events = None


@pytest.mark.usefixtures("published_configuration")
async def test_participant_retry_retains_frozen_request_and_replay(kit, keys, mq_env):
    from tests.integration.test_participant_retries import blocked

    original, claim = await blocked(kit)
    body = pb.MessagingBody(
        participant_retry=workflow.ParticipantRetryCommand(
            scope=workflow.PublicationScope(organization_id=1, operator_user_id=42),
            session_id=original.session_id,
            command_id=original.command_id,
            expected_run_id=original.expected_run_id,
            expected_version=original.expected_version,
            reason=original.reason,
            confirm=True,
            expected_provider_invocations=1,
            accept_result_unknown_risk=False,
        )
    )
    rows = await saved(kit.transactions, result_outbox)
    request_id = next(
        r["payload"]["request_id"] for r in rows if r["session_id"] == original.session_id
    )
    message = command(keys, pb.PARTICIPANT_RETRY, body, request_id, original.command_id)
    transport = receiver(kit.transactions, MessagingStore(), keys, admission(kit.source))
    await transport.receive_command(received(message))
    first = await saved(kit.transactions, outbox)
    receipt = next(
        pb.MessagingBody.FromString(r["body"]).command_receipt.workflow_receipt
        for r in first
        if r["kind"] == pb.COMMAND_RECEIPT
    )
    assert receipt.status == "queued" and receipt.run_id != claim.run_id
    await transport.receive_command(received(message))
    assert await saved(kit.transactions, outbox) == first


async def test_failed_evaluation_event_rolls_back_projection_and_sequence(
    requested, kit, keys, mq_env, monkeypatch
):
    tx, scope, store = requested
    tx.database.state_events = mq_env
    try:
        transport = receiver(tx, MessagingStore(), keys, admission(SyntheticEvidence()))
        body = pb.MessagingBody(
            evaluation_start=workflow.EvaluationStartCommand(
                scope=workflow.EvaluationQuery(
                    run_id=str(scope.run_id), organization_id=1, operator_user_id=42
                ),
                expected_version=1,
                reason="事件原子性",
                confirm=True,
            )
        )
        message = command(keys, pb.EVALUATION_START, body, str(scope.run_id))
        original = mq_env.store.stage

        async def fail_event(db, prepared, **kwargs):
            if prepared.envelope.kind == pb.EVALUATION_STATE:
                raise RuntimeError("injected state event failure")
            return await original(db, prepared, **kwargs)

        monkeypatch.setattr(mq_env.store, "stage", fail_event)
        with pytest.raises(RuntimeError):
            await transport.receive_command(received(message))
        assert (await store.get(scope)).version == 1
        assert await saved(tx, evaluation_sequences) == []
        assert await saved(tx, inbox) == [] and await saved(tx, outbox) == []
        monkeypatch.setattr(mq_env.store, "stage", original)
        await transport.receive_command(received(message))
        assert (await store.get(scope)).version == 2
        assert (await saved(tx, evaluation_sequences))[0]["sequence"] == 1
    finally:
        tx.database.state_events = None


@pytest.mark.usefixtures("published_configuration")
async def test_legacy_handoff_preserves_original_identity_and_never_marks_delivered(
    kit, keys, mq_env
):
    # Explicitly model an existing legacy result, before MQ mode is enabled.
    kit.transactions.database.state_events = None
    message = start_message(kit, keys)
    request = message.envelope.message_id
    await kit.service.start_external(
        kit.actor, "7", ("42",), "迁移验收", request, bound_case()[1].items
    )
    rows = await saved(kit.transactions, result_outbox)
    legacy = next(r for r in rows if r["payload"]["request_id"] == request)
    kit.transactions.database.state_events = mq_env
    async with kit.transactions.open() as db:
        await db.begin()
        assert await mq_env.record_legacy_interpretation(
            db, await locked_result(db, legacy["event_id"])
        )
        await db.commit()
    event = (await saved(kit.transactions, outbox))[0]
    assert event["message_id"] == legacy["event_id"]
    assert event["aggregate_key"] == request
    assert pb.MessagingBody.FromString(event["body"]).interpretation_state == workflow.StateEvent(
        **legacy["payload"]
    )
    assert not next(
        r
        for r in await saved(kit.transactions, result_outbox)
        if r["event_id"] == legacy["event_id"]
    )["delivered"]
    first = event["wire"]
    async with kit.transactions.open() as db:
        await db.begin()
        assert not await mq_env.record_legacy_interpretation(
            db, await locked_result(db, legacy["event_id"])
        )
        await db.commit()
    assert (await saved(kit.transactions, outbox))[0]["wire"] == first
    assert await MySQLResultOutbox(kit.transactions).pending(100) == []


@pytest.mark.usefixtures("published_configuration")
async def test_real_nsq_bootstrap_original_admission_and_shutdown(kit, keys, tmp_path):
    import asyncio
    import os
    from urllib.parse import urlencode

    from dishka import Provider, Scope, provide
    from reliable_messaging.wire import FAILED_CHANNEL, failed_topic
    from tornado.httpclient import AsyncHTTPClient, HTTPRequest

    from qs_ai.application.interpretation.ports import EvidenceSource
    from qs_ai.bootstrap.container import create_container
    from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
    from qs_ai.bootstrap.messaging import MessagingRuntime
    from qs_ai.config import Settings
    from qs_ai.infrastructure.workflow_transport.messaging import ACKS, CHANNELS, COMMANDS
    from tests.integration.test_delivery import certificates

    address, origin = os.environ.get("QS_MQ_NSQ_TCP_ADDRESS"), os.environ.get("QS_MQ_NSQ_HTTP_URL")
    if not address or not origin:
        pytest.fail("Required dedicated disposable NSQ TCP/HTTP configuration is missing")
    certificates(tmp_path)
    paths = {}
    for name, key in keys.items():
        path = tmp_path / (name + ".json")
        path.write_text(key.export_private() if name.startswith("ai.") else key.export_public())
        paths[name] = str(path)
    settings = Settings(
        database_url=kit.dsn.replace("mysql://", "mysql+asyncmy://", 1),
        generation={"enabled": False},
        evaluation={"enabled": False},
        grpc={
            "access_address": "localhost:1",
            "ca_file": str(tmp_path / "ca.pem"),
            "cert_file": str(tmp_path / "ai.pem"),
            "key_file": str(tmp_path / "ai.key"),
        },
        messaging={
            "enabled": True,
            "nsqd": {address: origin},
            "signing_key_file": paths["ai.sign"],
            "decrypt_key_files": {"ai.encrypt": paths["ai.encrypt"]},
            "qs_signer_files": {"qs.sign": paths["qs.sign"]},
            "qs_recipient_key_file": paths["qs.encrypt"],
        },
    )

    class FakeAuthorization(Provider):
        @provide(scope=Scope.APP, provides=EvidenceSource, override=True)
        def source(self) -> EvidenceSource:
            return SyntheticEvidence()

    http = AsyncHTTPClient(force_instance=True)
    try:
        # Provision is a test-host operation, outside runtime startup/preflight.
        for topic in (COMMANDS, ACKS):
            for t, c in (
                (topic, CHANNELS[topic]),
                (failed_topic(topic, CHANNELS[topic]), FAILED_CHANNEL),
            ):
                await http.fetch(
                    HTTPRequest(
                        origin + "/topic/create?" + urlencode({"topic": t}),
                        method="POST",
                        body=b"",
                        request_timeout=5,
                    )
                )
                await http.fetch(
                    HTTPRequest(
                        origin + "/channel/create?" + urlencode({"topic": t, "channel": c}),
                        method="POST",
                        body=b"",
                        request_timeout=5,
                    )
                )
        container = create_container(settings, FakeAuthorization())
        runtime = await MessagingRuntime.create(
            container,
            settings,
            (tmp_path / "ca.pem").read_bytes(),
            (tmp_path / "ai.pem").read_bytes(),
            (tmp_path / "ai.key").read_bytes(),
        )
        stop, state = asyncio.Event(), RuntimeState()
        task = asyncio.create_task(supervise(runtime.components(state, 1), state, stop))
        try:
            async with asyncio.timeout(10):
                while not state.ready:  # noqa: ASYNC110 - observe startup state
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            msg = start_message(kit, keys)
            await runtime.publishers[address].publish(COMMANDS, msg.wire)
            async with asyncio.timeout(10):
                while not await saved(kit.transactions, inbox):  # noqa: ASYNC110 - poll durable evidence
                    await asyncio.sleep(0.02)
            evidence = await saved(kit.transactions, outbox)
            assert len(evidence) == 2
            assert any(
                pb.MessagingBody.FromString(r["body"]).command_receipt.workflow_receipt.status
                == "queued"
                for r in evidence
                if r["kind"] == pb.COMMAND_RECEIPT
            )
            # Duplicate physical delivery must skip original CAS and preserve exact first wire.
            await runtime.publishers[address].publish(COMMANDS, msg.wire)
            await runtime.step()
            assert {r["message_id"]: r["wire"] for r in await saved(kit.transactions, outbox)} == {
                r["message_id"]: r["wire"] for r in evidence
            }
        finally:
            stop.set()
            await asyncio.wait_for(task, 10)
            await runtime.close()
            await container.close()
    finally:
        http.close()
        async with kit.transactions.open() as db:
            for table in (inbox, outbox, quarantine, evaluation_sequences):
                await db.execute(delete(table))
            await db.commit()


@pytest.mark.usefixtures("published_configuration")
@pytest.mark.parametrize(
    "confirmation",
    ["stored", "wrong_hash", "held", "rollback", "source_conflict", "unowned", "storage_error"],
)
async def test_legacy_result_settles_only_with_atomic_original_business_ack(
    kit, keys, mq_env, confirmation
):
    from reliable_messaging.durable import MessageConflict
    from sqlalchemy import update

    kit.transactions.database.state_events = None
    request = str(uuid4())
    await kit.service.start_external(
        kit.actor, "7", ("42",), "历史结果确认事务验收", request, bound_case()[1].items
    )
    legacy = next(
        r
        for r in await saved(kit.transactions, result_outbox)
        if r["payload"]["request_id"] == request
    )
    kit.transactions.database.state_events = mq_env
    async with kit.transactions.open() as db:
        await db.begin()
        await mq_env.record_legacy_interpretation(db, await locked_result(db, legacy["event_id"]))
        await db.commit()
    event = next(
        r for r in await saved(kit.transactions, outbox) if r["message_id"] == legacy["event_id"]
    )
    ack = prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        str(uuid4()),
        request,
        pb.MessagingBody(
            event_acknowledgement=pb.MessagingEventAcknowledgement(
                event_id=legacy["event_id"],
                event_body_sha256="0" * 64
                if confirmation == "wrong_hash"
                else event["body_sha256"],
                event_kind=pb.INTERPRETATION_STATE,
                outcome=pb.MessagingEventAcknowledgement.TECHNICALLY_HELD
                if confirmation == "held"
                else pb.MessagingEventAcknowledgement.STORED,
            )
        ),
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )
    if confirmation in ("source_conflict", "unowned"):
        async with kit.transactions.open() as db:
            await db.begin()
            await db.execute(
                update(result_outbox)
                .where(result_outbox.c.event_id == legacy["event_id"])
                .values(
                    **(
                        {"payload": {**legacy["payload"], "failure_code": "changed"}}
                        if confirmation == "source_conflict"
                        else {"mq_owned": False}
                    )
                )
            )
            await db.commit()
    async with kit.transactions.open() as db:
        await db.begin()
        if confirmation in ("wrong_hash", "source_conflict", "unowned"):
            with pytest.raises(MessageConflict):
                await mq_env.store.confirm_event(db, ack.envelope, ack.body)
            await db.rollback()
        elif confirmation == "storage_error":
            original_execute = db.execute

            async def unavailable(statement, *args, **kwargs):
                if getattr(statement, "table", None) is result_outbox:
                    raise OSError("isolated original result storage failure")
                return await original_execute(statement, *args, **kwargs)

            db.execute = unavailable
            with pytest.raises(OSError):
                await mq_env.store.confirm_event(db, ack.envelope, ack.body)
            db.execute = original_execute
            await db.rollback()
        else:
            await mq_env.store.confirm_event(db, ack.envelope, ack.body)
            if confirmation == "rollback":
                await db.rollback()
            else:
                await db.commit()
    final = next(
        r
        for r in await saved(kit.transactions, result_outbox)
        if r["event_id"] == legacy["event_id"]
    )
    current = next(
        r for r in await saved(kit.transactions, outbox) if r["message_id"] == legacy["event_id"]
    )
    assert bool(final["delivered"]) == (confirmation == "stored")
    assert (current["stage"] == "confirmed") == (confirmation == "stored")
    assert final["created_at"] == legacy["created_at"]
    assert current["wire"] == event["wire"]
    if confirmation == "stored":
        assert final["delivered_at"] is not None
        async with kit.transactions.open() as db:
            await db.begin()
            await mq_env.store.confirm_event(db, ack.envelope, ack.body)
            await db.commit()
        duplicate = next(
            r
            for r in await saved(kit.transactions, result_outbox)
            if r["event_id"] == legacy["event_id"]
        )
        assert duplicate["delivered_at"] == final["delivered_at"]


@pytest.mark.usefixtures("published_configuration")
async def test_legacy_handoff_and_ack_share_lock_order_without_deadlock(kit, keys, mq_env):
    import asyncio

    kit.transactions.database.state_events = None
    request = str(uuid4())
    await kit.service.start_external(
        kit.actor, "7", ("42",), "历史移交锁顺序验收", request, bound_case()[1].items
    )
    legacy = next(
        r
        for r in await saved(kit.transactions, result_outbox)
        if r["payload"]["request_id"] == request
    )
    kit.transactions.database.state_events = mq_env
    async with kit.transactions.open() as db:
        await db.begin()
        await mq_env.record_legacy_interpretation(db, await locked_result(db, legacy["event_id"]))
        await db.commit()
    event = next(
        r for r in await saved(kit.transactions, outbox) if r["message_id"] == legacy["event_id"]
    )
    ack = prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        str(uuid4()),
        request,
        pb.MessagingBody(
            event_acknowledgement=pb.MessagingEventAcknowledgement(
                event_id=legacy["event_id"],
                event_body_sha256=event["body_sha256"],
                event_kind=pb.INTERPRETATION_STATE,
                outcome=pb.MessagingEventAcknowledgement.STORED,
            )
        ),
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )

    async def confirm():
        async with kit.transactions.open() as db:
            await db.begin()
            await mq_env.store.confirm_event(db, ack.envelope, ack.body)
            await db.commit()

    async with kit.transactions.open() as db:
        await db.begin()
        old = (
            (
                await db.execute(
                    select(result_outbox)
                    .where(result_outbox.c.event_id == legacy["event_id"])
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        task = asyncio.create_task(confirm())
        try:
            # Give ACK enough time to reach its first lock. It must not hold the
            # new row while waiting for this original row.
            await asyncio.sleep(0.2)
            assert not task.done()
            await asyncio.wait_for(mq_env.record_legacy_interpretation(db, old), 3)
            await db.commit()
            await asyncio.wait_for(task, 3)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    final = next(
        r
        for r in await saved(kit.transactions, result_outbox)
        if r["event_id"] == legacy["event_id"]
    )
    assert final["delivered"] and final["delivered_at"] is not None
    assert (
        next(
            r
            for r in await saved(kit.transactions, outbox)
            if r["message_id"] == legacy["event_id"]
        )["wire"]
        == event["wire"]
    )
