import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.execution.runtime import RuntimeReader
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.transport.grpc.runtime import RuntimeManagement
from tests.test_grpc_commands import Aborted, Context


@pytest.fixture
def handler():
    reader = AsyncMock(spec=RuntimeReader)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is RuntimeReader
                return reader

        yield Scope()

    return RuntimeManagement(container), reader


def query():
    return pb.RuntimeQuery(
        scope=pb.PublicationScope(organization_id=7, operator_user_id=42),
        session_ids=[str(uuid4())],
    )


async def test_batch_is_one_read_with_trusted_scope(handler):
    service, reader = handler
    request = query()
    request.session_ids.extend(str(uuid4()) for _ in range(49))
    reader.summaries.return_value = {
        "items": [],
        "unavailable_session_ids": list(request.session_ids),
    }
    response = await service.BatchGet(request, Context())
    assert response.schema_version == "qs-ai-runtime-batch/v1"
    assert json.loads(response.data_json) == reader.summaries.return_value
    scope, ids = reader.summaries.await_args.args
    assert (scope.organization_id, scope.operator_user_id) == (7, 42)
    assert ids == tuple(request.session_ids)
    reader.summaries.assert_awaited_once()
    reader.detail.assert_not_awaited()


@pytest.mark.parametrize("case", ["empty", "duplicate", "many", "uuid", "zero", "scope", "peer"])
async def test_invalid_query_never_reaches_reader(handler, case):
    service, reader = handler
    request, context = query(), Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if case == "empty":
        del request.session_ids[:]
    elif case == "duplicate":
        request.session_ids.append(request.session_ids[0])
    elif case == "many":
        request.session_ids.extend(str(uuid4()) for _ in range(50))
    elif case == "uuid":
        request.session_ids[0] = "invalid"
    elif case == "zero":
        request.session_ids[0] = "00000000-0000-0000-0000-000000000000"
    elif case == "scope":
        request.scope.organization_id = 0
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as failure:
        await service.BatchGet(request, context)
    assert failure.value.args[0] == expected
    reader.summaries.assert_not_awaited()
    reader.detail.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (NotFound(), grpc.StatusCode.NOT_FOUND),
        (RuntimeError("secret DSN"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_detail_failure_does_not_expose_driver_body(handler, error, code):
    service, reader = handler
    reader.detail.side_effect = error
    with pytest.raises(Aborted) as failure:
        await service.Get(query(), Context())
    assert failure.value.args[0] == code
    assert "secret" not in str(failure.value)


async def test_detail_rejects_multiple_sessions(handler):
    service, reader = handler
    request = query()
    request.session_ids.append(str(uuid4()))
    with pytest.raises(Aborted):
        await service.Get(request, Context())
    reader.detail.assert_not_awaited()
