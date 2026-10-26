"""Original explicit exception and witness share one root; no real Broker/model claim."""

import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    BindingError,
    Decision,
    LocalOutcome,
    Result,
    StoreChecker,
)
from sqlalchemy import delete, text

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.contract_recovery_sender import ContractRecoverySender
from qs_ai.infrastructure.workflow_transport.contract_recovery_state import (
    ContractRecoveryInput,
    ContractRecoveryPlanner,
    apply_contract_recovery_state,
    read_contract_recovery_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_semantic_contract_recovery import AT, blocked, originals
from tests.integration.test_transaction_evaluation_admission import trigger_admin as trigger_admin
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


@pytest.fixture
async def original(host, ready):
    tx, run_id, *_ = ready
    state, value = await blocked(ready)
    before, records = await rows(tx, run_id), await originals(ready)
    release = json.loads(before[0]["definition_json"])["release_fingerprint"]
    incoming = ContractRecoveryInput(run_id, 1, state.version, release, value, True)
    try:
        yield SimpleNamespace(**locals())
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(evaluation_sequences.c.run_id == str(run_id))
            )
            await db.commit()


def planner(p):
    return ContractRecoveryPlanner(
        p.host.tx, signing_key=p.host.keys["ai.sign"], recipient_key=p.host.keys["qs.encrypt"]
    )


@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
async def test_original_failed_evidence_exception_audit_and_witness_commit_or_rollback_together(
    original, trigger_admin, monkeypatch, fault
):
    p = original
    winner = await planner(p).candidate(p.incoming)
    assert await rows(p.tx, p.run_id) == p.before and await originals(p.ready) == p.records
    reserved = await p.host.store.reserve(p.incoming.intent.request, winner)
    assert reserved.acquired
    p.host.executions.append(reserved.execution)
    _, first, _, frozen = read_contract_recovery_plan(p.incoming, winner)
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        p.incoming.intent,
        winner,
        apply_contract_recovery_state,
        isolation_level="REPEATABLE READ",
    )
    trigger = None
    try:
        if fault in ("archive_rejected", "witness_rejected"):
            trigger = "contract_exception_fault_" + uuid4().hex
            statement = (
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON {archive.name} FOR EACH ROW "
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated contract archive refusal'"
                if fault == "archive_rejected"
                else f"CREATE TRIGGER {trigger} BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='isolated contract witness refusal'; END IF; END"
            )
            async with trigger_admin.begin() as db:
                await db.execute(text(statement))
        with monkeypatch.context() as patch:
            if fault == "client_reply_discarded":
                actual_commit = EventSession.commit

                async def discarded(db):
                    await actual_commit(db)
                    raise ConnectionError(
                        "client discarded completed policy exception commit reply"
                    )

                patch.setattr(EventSession, "commit", discarded)
            outcome = await root(reserved.execution)
        committed = fault in ("none", "client_reply_discarded")
        assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
        assert await StoreChecker(p.host.store).check(reserved.execution.references[0]) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
        assert await originals(p.ready) == p.records
        if committed:
            run, _, checkpoint = await rows(p.tx, p.run_id)
            assert checkpoint["version"] == p.state.version + 1
            audit = run["progress_json"]["semantic_contract_recoveries"][-1]
            assert audit["resolved_at"] == first.value.resolved_at.isoformat()
            assert (
                audit["actor"] == p.value.actor
                and audit["output_fingerprint"] == p.value.output_fingerprint
            )
        else:
            assert await rows(p.tx, p.run_id) == p.before
        assert await p.host.archives.read_committed(frozen.envelope().message_id) == (
            frozen if committed else None
        )
        with pytest.raises(BindingError):
            await root(reserved.execution)
    finally:
        if trigger is not None:
            async with trigger_admin.begin() as db:
                await db.execute(text(f"DROP TRIGGER {trigger}"))


@pytest.mark.parametrize("reject", [False, True])
async def test_sender_requires_preparation_and_replays_first_operator_result_after_later_state(
    original, reject
):
    p = original

    class Runtime:
        store = p.host.store
        calls = 0

        async def reserve(self, request, candidate):
            value = await self.store.reserve(request, candidate)
            if value.acquired:
                p.host.executions.append(value.execution)
            return value

        async def execute(self, owned, local):
            if reject:
                raise TransactionSendError(
                    "isolated_contract_half_rejected", Result(LocalOutcome.NOT_STARTED, ())
                )
            self.calls += 1
            raise TransactionSendError(
                "isolated_contract_final_unknown", Result(await local(owned), ())
            )

    runtime = Runtime()
    sender = ContractRecoverySender(
        p.host.tx,
        p.host.archives,
        runtime,
        signing_key=p.host.keys["ai.sign"],
        recipient_key=p.host.keys["qs.encrypt"],
    )
    if reject:
        with pytest.raises(TransactionSendError):
            await sender(p.incoming)
        assert runtime.calls == 0 and await rows(p.tx, p.run_id) == p.before
    else:
        result = await sender(p.incoming)
        assert result == p.incoming.response() and runtime.calls == 1
        await MySQLEvaluationManagement(p.tx).cancel(
            ManagementScope(p.run_id, 1, 42),
            p.state.version + 1,
            "original later cancellation",
            AT + timedelta(seconds=5),
            discard=False,
            confirm=True,
        )
        current = await rows(p.tx, p.run_id)
        later = replace(
            p.incoming,
            value=replace(p.value, resolved_at=p.value.resolved_at + timedelta(seconds=10)),
        )
        replay = await sender(later)
        assert replay == result and replay["result_version"] == p.state.version + 1
        assert (await rows(p.tx, p.run_id)) == current and runtime.calls == 1
        assert await originals(p.ready) == p.records
        with pytest.raises(ValueError):
            await sender(
                replace(later, value=replace(later.value, reason="different authorization"))
            )
        assert runtime.calls == 1


async def test_later_original_cancellation_cannot_be_overridden_by_prepared_exception(original):
    p = original
    winner = await planner(p).candidate(p.incoming)
    reserved = await p.host.store.reserve(p.incoming.intent.request, winner)
    p.host.executions.append(reserved.execution)
    await MySQLEvaluationManagement(p.tx).cancel(
        ManagementScope(p.run_id, 1, 42),
        p.state.version,
        "original operator cancellation wins",
        AT + timedelta(seconds=2),
        discard=False,
        confirm=True,
    )
    expected = await rows(p.tx, p.run_id)
    root = EvaluationStateRoot(
        p.host.tx,
        p.host.archives,
        p.host.store,
        p.incoming.intent,
        winner,
        apply_contract_recovery_state,
        isolation_level="REPEATABLE READ",
    )
    assert await root(reserved.execution) == LocalOutcome.ABORTED
    assert await rows(p.tx, p.run_id) == expected and await originals(p.ready) == p.records
    assert (
        await StoreChecker(p.host.store).check(reserved.execution.references[0])
        == Decision.ROLLBACK
    )


@pytest.mark.parametrize("apply", [False, True])
async def test_maintenance_cli_real_primary_preview_or_sdk_original_root_without_transport_claim(
    original, monkeypatch, apply
):
    import os
    from argparse import Namespace
    from types import SimpleNamespace

    from pydantic import SecretStr
    from reliable_messaging.transaction_mysql import MySQLTransactionStore

    from qs_ai.bootstrap import recover_semantic_contract as cli

    p = original
    events = []
    options = SimpleNamespace(
        transactions={"ai.mysql": object()},
        enabled=True,
        publish_enabled=True,
        signing_key_file="isolated-signing",
        qs_recipient_key_file="isolated-recipient",
    )
    settings = SimpleNamespace(
        database_url=SecretStr(
            os.environ["QS_AI_TEST_MYSQL_DSN"].replace("mysql://", "mysql+asyncmy://", 1)
        ),
        messaging=options,
    )
    args = Namespace(
        run_id=p.run_id,
        organization_id=1,
        expected_version=p.state.version,
        execution_id=p.value.execution_id,
        expected_release_fingerprint=p.release,
        reason=p.value.reason,
        apply=apply,
        expected_output_fingerprint=p.value.output_fingerprint,
        acknowledge_policy_exception_and_cost=True,
    )
    actual_database = cli.Database

    def database(url):
        value = actual_database(url)
        actual_close = value.close

        async def close():
            events.append("database_close")
            await actual_close()

        value.close = close
        return value

    def factory(_options, sessions):
        class Runtime:
            store = MySQLTransactionStore(sessions, p.host.store.binding)

            async def start(self):
                events.append("sdk_start")
                await self.store.validate()

            async def reserve(self, request, candidate):
                value = await self.store.reserve(request, candidate)
                if value.acquired:
                    p.host.executions.append(value.execution)
                return value

            async def execute(self, owned, local):
                events.append("sdk_execute")
                raise TransactionSendError(
                    "isolated_cli_final_unknown", Result(await local(owned), ())
                )

            async def close(self, *, grace_seconds):
                events.append("sdk_close")

        return Runtime()

    monkeypatch.setattr(cli, "Settings", lambda: settings)
    monkeypatch.setattr(cli, "Database", database)
    monkeypatch.setattr(cli, "make_transaction_sender", factory)
    monkeypatch.setattr(
        cli, "read_key", lambda path, private: p.host.keys["ai.sign" if private else "qs.encrypt"]
    )
    monkeypatch.setattr(cli, "datetime", SimpleNamespace(now=lambda tz: p.value.resolved_at))
    result = await cli.run(args)
    assert result["mode"] == ("applied" if apply else "preview_rolled_back")
    assert await originals(p.ready) == p.records
    if apply:
        assert events == ["sdk_start", "sdk_execute", "sdk_close", "database_close"]
        owned = p.host.executions[-1]
        assert await StoreChecker(p.host.store).check(owned.references[0]) == Decision.COMMIT
        assert (await rows(p.tx, p.run_id))[2]["version"] == p.state.version + 1
    else:
        assert events == ["database_close"] and not p.host.executions
        assert await rows(p.tx, p.run_id) == p.before
