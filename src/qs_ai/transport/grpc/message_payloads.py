"""Read-only, exact-reference body access. Registration belongs to host bootstrap."""

from typing import Any

import grpc
from grpc import aio
from reliable_messaging.durable import MessageConflict

from qs_ai.application.messaging_payloads import PayloadAccess
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.transport.grpc.identity import require_qs_workload


class MessagePayloads(rpc.MessagePayloadsServicer):
    def __init__(self, access: PayloadAccess) -> None:
        self.access = access

    async def Get(
        self,
        request: pb.MessagePayloadReference,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.MessagePayload:
        try:
            await require_qs_workload(context)
        except Exception:
            await self.access.record_failure("payload_serve_workload_denied")
            raise
        if not self.access.valid_reference(request):
            await self.access.record_failure("payload_serve_reference_mismatch")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid payload reference")
        try:
            body = await self.access.payload(request, "qs-server")
            return pb.MessagePayload(reference=request, body=body)
        except MessageConflict:
            await self.access.record_failure("payload_serve_reference_mismatch")
            await context.abort(grpc.StatusCode.NOT_FOUND, "Original payload unavailable")
        except Exception:
            await self.access.record_failure("payload_serve_storage_unavailable")
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Payload storage unavailable")
        raise AssertionError("abort must raise")
