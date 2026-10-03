"""Borrow preconfigured mTLS stubs; never resolve URLs supplied by messages."""

import asyncio
import hashlib
from collections.abc import Mapping
from typing import Any

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.messaging import (
    MAX_BODY,
    MessagingContractError,
    parse_body,
    route,
    validate_header,
)


class PayloadResolver:
    def __init__(self, stubs: Mapping[str, Any], *, timeout: float = 5) -> None:
        if set(stubs) != {"qs-server"} or not 0 < timeout <= 5:
            raise ValueError("only the preconfigured QS mTLS endpoint is supported")
        self.stubs, self.timeout = dict(stubs), timeout  # borrowed clients

    async def body(self, authenticated: pb.MessagingEnvelope) -> bytes:
        validate_header(authenticated, route(authenticated.kind)[2])
        if authenticated.WhichOneof("body") == "inline_body":
            raw = bytes(authenticated.inline_body)
        else:
            reference = authenticated.payload_reference
            if reference.producer != "qs-server" or authenticated.kind == pb.EVENT_ACKNOWLEDGEMENT:
                raise MessagingContractError(
                    "unexpected payload source or acknowledgement reference"
                )
            async with asyncio.timeout(self.timeout):
                response = await self.stubs[reference.producer].Get(reference, timeout=self.timeout)
            if (
                response.reference != reference
                or len(response.body) > MAX_BODY
                or len(response.body) != reference.body_length
                or hashlib.sha256(response.body).hexdigest() != reference.body_sha256
            ):
                raise MessagingContractError("original payload response mismatch")
            raw = bytes(response.body)
        body = parse_body(authenticated, raw)
        if authenticated.HasField("payload_reference"):
            field = body.WhichOneof("value")
            value = getattr(body, field)
            org = (
                value.actor.org_id
                if field in ("start", "change")
                else str(value.scope.organization_id)
            )
            if org != authenticated.payload_reference.organization_id:
                raise MessagingContractError("payload organization does not match original command")
        return raw
