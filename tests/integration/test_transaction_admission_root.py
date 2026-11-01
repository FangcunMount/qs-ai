"""Actual host roots/MySQL proofs; no Broker, business admission or packet-fault claim."""

import hashlib
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from jwcrypto import jwk
from reliable_messaging.transaction import Binding, Decision, LocalOutcome, StoreChecker
from reliable_messaging.transaction_mysql import MySQLTransactionStore

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import Database, EventSession, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, inbox, outbox
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.transaction_admission import AdmissionRoot
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
)
from tests.integration.test_transaction_schema import installed as installed
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


def frozen_held(context=b""):
    identity = str(uuid4())
    qs_sign = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.sign")
    ai_sign = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    ai_key = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.encrypt")
    qs_key = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")
    command = prepare(
        pb.START,
        identity,
        identity,
        pb.MessagingBody(
            start=workflow.StartCommand(
                request_id=identity,
                actor=workflow.Actor(org_id="7", subject_id="9"),
                testee_id="19",
                assessment_ids=["42"],
                goal="isolated held-root proof",
            )
        ),
        organization_id="7",
        original_occurred_at="2026-10-21T00:00:00Z",
        signing_key=qs_sign,
        recipient_key=ai_key,
    )
    incoming = FrozenCommandInput.from_authenticated(
        command.envelope, command.body, command.wire, routes=legacy_routes()
    )
    receipt = prepare(
        pb.COMMAND_RECEIPT,
        str(uuid4()),
        identity,
        build_receipt_body(
            command.envelope, AdmissionDecision("7", 1, pb.HELD, "technical_budget_exhausted", 14)
        ),
        organization_id="7",
        original_occurred_at="2026-10-21T00:00:00Z",
        signing_key=ai_sign,
        recipient_key=qs_key,
        correlation=identity,
    )
    return (
        incoming,
        receipt,
        freeze_admission_plan(incoming, str(uuid4()), ((receipt, 1),), context=context),
    )


@pytest.mark.parametrize(
    "fault", ["none", "archive_rejected", "witness_rejected", "client_reply_discarded"]
)
@pytest.mark.parametrize("context", [b"", b"isolated-v2-local-proof"])
async def test_held_root_uses_actual_inbox_archive_and_witness_atomically(
    installed, monkeypatch, fault, context
):
    sessions = installed
    binding = Binding("ai.mysql", "owned-primary", "m3-1")
    store = MySQLTransactionStore(sessions, binding)
    archives = TransactionArchive(sessions, binding)
    async with sessions() as db, db.begin():
        await db.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','owned-primary','m3-1',1)")
        )
        if fault == "archive_rejected":
            await db.execute(
                sa.text(
                    f"CREATE TRIGGER refuse_archive BEFORE INSERT ON {archive.name} "
                    "FOR EACH ROW SIGNAL SQLSTATE '45000' "
                    "SET MESSAGE_TEXT='isolated archive rejection'"
                )
            )
        elif fault == "witness_rejected":
            await db.execute(
                sa.text(
                    "CREATE TRIGGER refuse_witness BEFORE UPDATE ON rm_tx_witness FOR EACH ROW "
                    "BEGIN IF NEW.state='committed' THEN SIGNAL SQLSTATE '45000' "
                    "SET MESSAGE_TEXT='isolated witness rejection'; END IF; END"
                )
            )
    if fault == "client_reply_discarded":
        original_commit = EventSession.commit

        async def discard_reply(db):
            await original_commit(db)
            # A client-side discarded reply, explicitly not a network failpoint.
            raise ConnectionError("client discarded completed commit reply")

        monkeypatch.setattr(EventSession, "commit", discard_reply)
    incoming, receipt, plan = frozen_held(
        context
    )  # all IDs, time, signatures and cipher before root
    reservation = await store.reserve(incoming.request, plan)
    assert reservation.acquired
    database = Database(None)
    database.engine = sessions.kw["bind"]  # fixture owns and disposes this exact engine
    business = AsyncMock(side_effect=AssertionError("HELD must not execute business"))
    local = AdmissionRoot(
        Transactions(database),
        archives,
        store,
        MessagingStore(),
        incoming,
        reservation_token=str(uuid4()),
        business=business,
    )
    outcome = await local(reservation.execution)
    committed = fault in ("none", "client_reply_discarded")
    assert outcome == (LocalOutcome.COMMITTED if committed else LocalOutcome.ABORTED)
    business.assert_not_called()
    for reference in reservation.execution.references:
        assert await StoreChecker(store).check(reference) == (
            Decision.COMMIT if committed else Decision.ROLLBACK
        )
    async with sessions() as db:
        row = (
            (
                await db.execute(
                    sa.select(inbox).where(inbox.c.message_id == incoming.envelope().message_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if committed:
            assert row is not None
            assert row["decision"] == "held" and row["receipt_id"] == receipt.envelope.message_id
        else:
            assert row is None
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(outbox)
                .where(outbox.c.message_id == receipt.envelope.message_id)
            )
            == 0
        )
    stored = await archives.read_committed(receipt.envelope.message_id)
    if committed:
        assert stored is not None
        assert hashlib.sha256(stored.wire).hexdigest() == hashlib.sha256(receipt.wire).hexdigest()
        assert hashlib.sha256(stored.body).hexdigest() == hashlib.sha256(receipt.body).hexdigest()
        assert stored.context == context
    else:
        assert stored is None
