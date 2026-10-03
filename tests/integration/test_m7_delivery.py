"""M7: actual protobuf/gRPC and local SQL persistence, without the Go harness or models."""

from dataclasses import asdict
from uuid import uuid4

import grpc
import pytest
from sqlalchemy import Column, LargeBinary, MetaData, String, Table, func, select, update
from sqlalchemy.dialects.mysql import insert

from qs_ai.application.integration.events import DeliverResults
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, result_outbox
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver
from tests.integration.test_interpretation import kit  # noqa: F401
from tests.test_input_binding import bound_case

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


async def test_original_grpc_id_payload_and_defaults_survive_lost_or_mismatched_receipt(kit):  # noqa: F811
    metadata = MetaData()
    receipts = Table(
        "m7_result_receipts",
        metadata,
        Column("event_id", String(128), primary_key=True),
        Column("wire", LargeBinary),
    )
    tx = kit.transactions
    engine = tx.database.engine
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    server = grpc.aio.server()

    class Receiver(rpc.ResultsServicer):
        mode = "lost"
        deliveries = 0

        async def Accept(self, request, context):
            self.deliveries += 1
            wire = request.SerializeToString(deterministic=True)
            async with tx.open() as db:
                statement = insert(receipts).values(event_id=request.event_id, wire=wire)
                await db.execute(statement.on_duplicate_key_update(event_id=receipts.c.event_id))
                assert await db.scalar(select(receipts.c.wire)) == wire
                await db.commit()
            if self.mode == "lost":
                await context.abort(grpc.StatusCode.UNAVAILABLE, "injected lost durable receipt")
            return pb.Acknowledgement(
                event_id="mismatched" if self.mode == "mismatch" else request.event_id
            )

    receiver = Receiver()
    rpc.add_ResultsServicer_to_server(receiver, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        receipt = await kit.service.start_external(
            kit.actor,
            "7",
            ("42",),
            "M7 synthetic notification",
            str(uuid4()),
            bound_case()[1].items,
        )
        store = MySQLResultOutbox(tx)
        [event] = await store.pending(20)
        original = pb.StateEvent(**asdict(event)).SerializeToString(deterministic=True)
        async with tx.open() as db:
            before = (await db.execute(select(result_outbox))).mappings().one()
            model_count = await db.scalar(select(func.count()).select_from(model_calls))
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            deliver = DeliverResults(store, GRPCResultReceiver(channel))
            for mode in ("lost", "mismatch", "normal"):
                receiver.mode = mode
                assert await deliver.once() == (1 if mode == "normal" else 0)
                async with tx.open() as db:
                    row = (await db.execute(select(result_outbox))).mappings().one()
                    assert row["event_id"] == before["event_id"] == event.event_id
                    assert row["payload"] == before["payload"]
                    assert row["created_at"] == before["created_at"]
                    assert row["delivered"] == (mode == "normal")
                    assert row["attempts"] == (1 if mode == "lost" else 2)
                    assert await db.scalar(select(receipts.c.wire)) == original
                    assert (
                        await db.scalar(select(func.count()).select_from(model_calls))
                        == model_count
                    )
                    # Only bring this synthetic result due; never change a task/frozen config.
                    await db.execute(
                        update(result_outbox)
                        .where(result_outbox.c.event_id == event.event_id)
                        .values(available_at=func.utc_timestamp(6))
                    )
                    await db.commit()
            assert await deliver.once() == 0
        assert receiver.deliveries == 3
        restored = pb.StateEvent.FromString(original)
        assert restored.request_id == event.request_id
        assert restored.session_id == receipt.session_id
        assert restored.question_id == "" and restored.can_skip is False
    finally:
        await server.stop(0)
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
