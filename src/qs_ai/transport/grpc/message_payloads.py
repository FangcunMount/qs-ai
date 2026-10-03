"""Read-only, exact-reference body access. Registration belongs to host bootstrap."""

from typing import Any

import grpc
from grpc import aio
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.workflow_transport.messaging import MAX_BODY, valid_hash, valid_id
from qs_ai.transport.grpc.identity import require_qs_workload


class MessagePayloads(rpc.MessagePayloadsServicer):
    def __init__(self, transactions: Transactions, store: MessagingStore) -> None:
        self.transactions, self.store = transactions, store

    async def Get(
        self,
        request: pb.MessagePayloadReference,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.MessagePayload:
        await require_qs_workload(context)
        if (
            request.ByteSize() > 8192
            or not valid_id(request.message_id)
            or not valid_hash(request.body_sha256)
            or not 0 < request.body_length <= MAX_BODY
        ):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid payload reference")
        try:
            async with self.transactions.open() as db:
                await db.begin()
                body = await self.store.payload(db, request, "qs-server")
            return pb.MessagePayload(reference=request, body=body)
        except MessageConflict:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Original payload unavailable")
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Payload storage unavailable")
        raise AssertionError("abort must raise")
