import io
import json
import logging

import grpc
import pytest
from grpc import aio

from qs_ai.infrastructure.observability.structured import StructuredHandler
from qs_ai.transport.grpc.diagnostics import Diagnostics


async def test_real_grpc_success_and_abort_preserve_contract_and_hide_body():
    stream = io.StringIO()
    handler = StructuredHandler(stream, environment="local", release="test")
    logger = logging.getLogger("qs_ai.structured")
    before = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    server = aio.server(interceptors=[Diagnostics()])

    async def call(request, context):
        if request == b"deny":
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "SECRET_BODY")
        return request

    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "test.Service",
                {
                    "Call": grpc.unary_unary_rpc_method_handler(call),
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            rpc = channel.unary_unary("/test.Service/Call")
            rid = "75d9b24b-2864-4318-b015-bc6e5df9f419"
            assert (
                await rpc(b"SECRET_RESPONSE", metadata=(("x-correlation-id", rid),))
                == b"SECRET_RESPONSE"
            )
            with pytest.raises(aio.AioRpcError) as error:
                await rpc(b"deny", metadata=(("x-request-id", "bad id!"),))
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        await server.stop(0)
        logger.removeHandler(handler)
        logger.setLevel(before)
        handler.shutdown()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert rows[0]["correlation_id"] == rid
    assert rows[0]["status"] == "OK"
    assert rows[1]["status"] == "PERMISSION_DENIED"
    assert rows[1]["correlation_id"] != "bad id!"
    assert "SECRET" not in stream.getvalue()
