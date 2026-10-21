"""Actual participant retry root; synthetic provider history, no Broker or production calls."""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import select, update

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, inbox, outbox
from qs_ai.infrastructure.persistence.mysql.retry_admission import audit_digest
from qs_ai.infrastructure.persistence.mysql.schema import (
    jobs,
    participant_retries,
    result_outbox,
    runs,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS, authenticate
from qs_ai.infrastructure.workflow_transport.retry_planner import ParticipantRetryPlanner
from qs_ai.infrastructure.workflow_transport.transaction_admission import make_retry_root
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import admission, command
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_participant_retries import blocked, calls
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_start_admission import reserve, rows

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


def calls_digest(values):
    raw = json.dumps([dict(v) for v in values], sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).digest()


async def incoming(host, value):
    async with host.tx.open() as db:
        original_request = await db.scalar(
            select(sessions.c.request_id).where(sessions.c.id == value.session_id)
        )
    prepared = command(
        host.keys,
        pb.PARTICIPANT_RETRY,
        pb.MessagingBody(
            participant_retry=workflow.ParticipantRetryCommand(
                scope=workflow.PublicationScope(
                    organization_id=value.scope.organization_id,
                    operator_user_id=value.scope.operator_user_id,
                ),
                session_id=value.session_id,
                command_id=value.command_id,
                expected_run_id=value.expected_run_id,
                expected_version=value.expected_version,
                reason=value.reason,
                confirm=value.confirm,
                expected_provider_invocations=value.expected_provider_invocations,
                accept_result_unknown_risk=value.accept_result_unknown_risk,
            )
        ),
        original_request,
        command_id=value.command_id,
    )
    header = authenticate(
        prepared.wire,
        COMMANDS,
        decrypt_keys={"ai.encrypt": host.keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", host.keys["qs.sign"])},
    )
    result = FrozenCommandInput.from_authenticated(header, prepared.body, prepared.wire)
    host.messages.append(result)
    return result


async def planned(host, value):
    message = await incoming(host, value)
    source = SimpleNamespace(authorize=AsyncMock(wraps=host.kit.source.authorize))
    candidate = await ParticipantRetryPlanner(
        host.tx, source, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    ).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    source.authorize.side_effect = AssertionError("no remote authorization in original root")
    local = make_retry_root(
        host.tx, host.archives, host.store, MessagingStore(), message, owned.plan, admission(source)
    )
    return SimpleNamespace(**locals())


async def test_retry_commits_exact_first_source_audit_history_and_witness_without_model_redo(host):
    value, claim = await blocked(host.kit)
    previous_calls = await calls(host.kit, value.session_id)
    f = await planned(host, value)
    facts = InterpretationMutationFacts.from_plan(f.message, f.owned.plan)
    assert await f.local(f.owned) == LocalOutcome.COMMITTED
    assert f.source.authorize.await_count == 1
    for reference in f.owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
    current = (await rows(host, sessions, sessions.c.id == value.session_id))[0]
    assert (
        current["status"] == "queued"
        and current["active_run_id"] == facts.run_id
        and current["version"] == value.expected_version + 1
    )
    audit = (await rows(host, participant_retries, participant_retries.c.run_id == facts.run_id))[0]
    expected = {
        k: audit[k]
        for k in (
            "organization_id",
            "command_id",
            "session_id",
            "request_id",
            "source_run_id",
            "run_id",
            "operator_user_id",
            "expected_version",
            "reason",
            "accepted_unknown_risk",
            "source_failure_code",
            "frozen_request_json",
            "receipt",
        )
    }
    assert audit_digest(expected) == facts.audit_digest
    assert (
        hashlib.sha256(audit["frozen_request_json"].encode()).digest()
        == hashlib.sha256(previous_calls[0]["request_json"].encode()).digest()
    )
    history = (await rows(host, result_outbox, result_outbox.c.event_id == facts.state_event_id))[0]
    assert history["mq_owned"] and not history["delivered"] and history["attempts"] == 0
    assert len(await rows(host, jobs, jobs.c.id == facts.job_id)) == 1
    assert (await rows(host, runs, runs.c.id == value.expected_run_id))[0]["status"] == "blocked"
    assert calls_digest(await calls(host.kit, value.session_id)) == calls_digest(previous_calls)
    assert (await rows(host, inbox, inbox.c.message_id == f.message.envelope().message_id))[0][
        "decision"
    ] == "accepted"
    for archived in read_admission_plan(f.message, f.owned.plan):
        stored = await host.archives.read_committed(archived.envelope().message_id)
        assert stored is not None
        assert hashlib.sha256(stored.wire).digest() == hashlib.sha256(archived.wire).digest()
        assert not await rows(host, outbox, outbox.c.message_id == archived.envelope().message_id)


async def test_changed_original_failure_audit_aborts_whole_retry_before_any_new_fact_commits(host):
    value, _ = await blocked(host.kit)
    previous_calls = await calls(host.kit, value.session_id)
    f = await planned(host, value)
    facts = InterpretationMutationFacts.from_plan(f.message, f.owned.plan)
    async with host.tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == value.session_id)
            .values(failure_code="provider_error")
        )
        await db.commit()
    assert await f.local(f.owned) == LocalOutcome.ABORTED
    for reference in f.owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.ROLLBACK
    assert not await rows(host, participant_retries, participant_retries.c.run_id == facts.run_id)
    assert not await rows(host, jobs, jobs.c.id == facts.job_id)
    assert not await rows(host, result_outbox, result_outbox.c.event_id == facts.state_event_id)
    assert not await rows(host, inbox, inbox.c.message_id == f.message.envelope().message_id)
    current = (await rows(host, sessions, sessions.c.id == value.session_id))[0]
    assert current["version"] == value.expected_version and current["status"] == "blocked"
    assert calls_digest(await calls(host.kit, value.session_id)) == calls_digest(previous_calls)


async def test_unknown_result_risk_refusal_commits_only_rejection_and_does_not_retry_model(host):
    value, _ = await blocked(host.kit, unknown=True)
    value = replace(value, command_id=str(uuid4()), accept_result_unknown_risk=False)
    previous_calls = await calls(host.kit, value.session_id)
    f = await planned(host, value)
    assert await f.local(f.owned) == LocalOutcome.COMMITTED
    assert len(f.owned.references) == 1
    assert await StoreChecker(host.store).check(f.owned.references[0]) == Decision.COMMIT
    assert (await rows(host, inbox, inbox.c.message_id == f.message.envelope().message_id))[0][
        "decision"
    ] == "rejected"
    assert (await rows(host, sessions, sessions.c.id == value.session_id))[0][
        "version"
    ] == value.expected_version
    assert calls_digest(await calls(host.kit, value.session_id)) == calls_digest(previous_calls)
