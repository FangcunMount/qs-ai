"""Frozen admission decisions do not create a second publication responsibility."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict
from sqlalchemy.dialects import mysql

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql import messaging
from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    freeze_admission_plan,
    read_admission_plan,
)
from tests.test_admission_plan import fixture


@pytest.mark.parametrize("decision", [pb.ACCEPTED, pb.REJECTED, pb.HELD])
@pytest.mark.parametrize("transactional", [False, True])
async def test_original_inbox_decision_reuses_first_receipt_without_transaction_outbox(
    monkeypatch, decision, transactional
):
    incoming, receipt, _ = fixture(decision)
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2),))
    archive = read_admission_plan(incoming, plan)[0]
    root = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(messaging, "bind", lambda _: root)
    db = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)))
    store = messaging.MessagingStore()
    store.stage = AsyncMock()
    if transactional:
        await store.decide_transaction(db, incoming.envelope(), archive, organization_id="7")
        store.stage.assert_not_called()
    else:
        await store.decide(db, incoming.envelope(), receipt, organization_id="7", sequence=2)
        store.stage.assert_awaited_once_with(db, receipt, organization_id="7", sequence=2)
    root.validate.assert_awaited_once_with()
    assert db.execute.await_count == 1
    sql = db.execute.await_args.args[0].compile(dialect=mysql.dialect())
    assert (
        sql.params["decision"]
        == {pb.ACCEPTED: "accepted", pb.REJECTED: "rejected", pb.HELD: "held"}[decision]
    )
    assert sql.params["receipt_id"] == receipt.envelope.message_id
    assert incoming.envelope().message_id in sql.params.values()
    assert "processing" in sql.params.values()
    assert "messaging_outbox" not in str(sql)


@pytest.mark.parametrize("conflict", ["command", "aggregate", "correlation", "organization"])
async def test_frozen_decision_conflicts_are_refused_before_inbox_update(monkeypatch, conflict):
    incoming, receipt, _ = fixture()
    if conflict == "aggregate":
        receipt.envelope.aggregate_key = str(uuid4())
    elif conflict == "correlation":
        receipt.envelope.correlation_command_id = str(uuid4())
    # Freeze one valid byte archive without asserting the admission-specific binding.
    from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

    archive = FrozenArchive(
        receipt.envelope.SerializeToString(deterministic=True),
        receipt.body,
        receipt.wire,
        8 if conflict == "organization" else 7,
        2,
        False,
        True,
    )
    command = incoming.envelope()
    if conflict == "command":
        command.message_id = str(uuid4())
    db = SimpleNamespace(execute=AsyncMock())
    root = Mock(side_effect=AssertionError("conflict must not borrow or write"))
    monkeypatch.setattr(messaging, "bind", root)
    with pytest.raises((MessageConflict, MessagingContractError)):
        await messaging.MessagingStore().decide_transaction(
            db, command, archive, organization_id="7"
        )
    db.execute.assert_not_called()
    root.assert_not_called()


async def test_frozen_decision_requires_the_original_processing_reservation(monkeypatch):
    incoming, receipt, _ = fixture()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2),))
    archive = read_admission_plan(incoming, plan)[0]
    root = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(messaging, "bind", lambda _: root)
    db = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=0)))
    with pytest.raises((MessageConflict, MessagingContractError)):
        await messaging.MessagingStore().decide_transaction(
            db, incoming.envelope(), archive, organization_id="7"
        )
    root.validate.assert_awaited_once_with()
