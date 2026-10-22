"""Actual default DI and released SDK construction, without network or installed schema."""

from unittest.mock import Mock

import pytest
from jwcrypto import jwk
from sqlalchemy.ext.asyncio import AsyncEngine

from qs_ai.application.evaluation.management import EvaluationManagementStore
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.application.governance.solutions import SolutionStore
from qs_ai.application.interpretation.ports import ExecutionStore
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_requests import (
    TransactionalEvaluationRequests,
)
from qs_ai.infrastructure.workflow_transport.transactional_interpretation_execution import (
    TransactionalInterpretationExecutionStore,
)
from qs_ai.infrastructure.workflow_transport.transactional_solutions import TransactionalSolutions
from tests.test_evaluation_container import configured
from tests.test_transaction_messaging import options


def transaction_settings(tmp_path):
    value = options(tmp_path)
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")
    private, public = tmp_path / "ai-sign.json", tmp_path / "qs-recipient.json"
    private.write_text(signing.export_private())
    public.write_text(recipient.export_public())
    private.chmod(0o600)
    public.chmod(0o600)
    # Isolated construction fixture only; no JOSE delivery or production key use.
    value.update(
        enabled=True,
        signing_key_file=str(private),
        qs_recipient_key_file=str(public),
        decrypt_key_files={"ai.sign": str(private)},
        qs_signer_files={"qs.encrypt": str(public)},
    )
    return configured(messaging=value)


async def test_default_transaction_container_shares_one_sender_but_borrows_each_original_request(
    tmp_path, monkeypatch
):
    settings = transaction_settings(tmp_path)
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", settings.messaging.rocketmq.ca_file)
    connect = Mock(side_effect=AssertionError("DI cannot connect or install schema"))
    monkeypatch.setattr(AsyncEngine, "connect", connect)
    events = []
    sdk_close, sdk_wait, database_close = (
        TransactionRuntime.close,
        TransactionRuntime.wait,
        Database.close,
    )

    async def close_sdk(self, **kwargs):
        events.append("sender_closing")
        await sdk_close(self, **kwargs)
        events.append("sender_closed")

    async def wait_sdk(self):
        await sdk_wait(self)
        events.append("sender_waited")

    async def close_database(self):
        events.append("database_closing")
        await database_close(self)

    monkeypatch.setattr(TransactionRuntime, "close", close_sdk)
    monkeypatch.setattr(TransactionRuntime, "wait", wait_sdk)
    monkeypatch.setattr(Database, "close", close_database)
    container = create_container(settings, EvaluationProvider())
    try:
        host = await container.get(TransactionServices | None)
        assert host is not None and not host.runtime.stats()["ready"]
        assert "fixture-only" not in repr(host) and "PRIVATE" not in repr(host)
        workers = []
        transactions = []
        for _ in range(2):
            async with container() as request:
                assert await request.get(TransactionServices | None) is host
                for port, expected in (
                    (ExecutionStore, TransactionalInterpretationExecutionStore),
                    (EvaluationManagementStore, TransactionalEvaluationManagement),
                    (EvaluationRequests, TransactionalEvaluationRequests),
                    (SolutionStore, TransactionalSolutions),
                ):
                    assert isinstance(await request.get(port), expected)
                worker = await request.get(EvaluationWorker)
                workers.append(worker)
                transactions.append(worker.transactions)
                assert worker.owner == host.worker_owner
                assert worker._serial_runtime.transactions is worker.transactions
                assert worker._candidate_runtime.transactions is worker.transactions
        assert transactions[0] is not transactions[1]
        assert transactions[0].database is transactions[1].database
        assert workers[0].capacity is workers[1].capacity
        assert workers[0].recovery_cursor is workers[1].recovery_cursor
        connect.assert_not_called()
    finally:
        await container.close()
    assert events[0] == "sender_closing" and events[-1] == "database_closing"
    assert events[-2] in ("sender_closed", "sender_waited")


@pytest.mark.parametrize("fault", ["disabled", "wrong_private_key"])
async def test_configured_transaction_container_refuses_bad_binding_before_legacy_fallback(
    tmp_path, monkeypatch, fault
):
    settings = transaction_settings(tmp_path)
    value = settings.messaging.model_dump()
    if fault == "disabled":
        value["enabled"] = False
    else:
        value["signing_key_file"] = settings.messaging.qs_recipient_key_file
    settings = configured(messaging=value)
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", settings.messaging.rocketmq.ca_file)
    connect = Mock(side_effect=AssertionError("Invalid config cannot connect"))
    monkeypatch.setattr(AsyncEngine, "connect", connect)
    container = create_container(settings, EvaluationProvider())
    try:
        async with container() as request:
            with pytest.raises(ValueError):
                await request.get(ExecutionStore)
        connect.assert_not_called()
    finally:
        await container.close()
