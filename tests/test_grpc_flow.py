import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.governance.flow import FlowReader
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.transport.grpc.flow import FlowManagement
from tests.test_grpc_commands import Aborted, Context


@pytest.fixture
def handler():
    reader = AsyncMock(spec=FlowReader)
    reader.solution.return_value = {"nodes": []}
    reader.publication.return_value = {"nodes": []}

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is FlowReader
                return reader

        yield Scope()

    return FlowManagement(container), reader


def query():
    return pb.FlowQuery(
        scope=pb.PublicationScope(organization_id=1, operator_user_id=42), solution_id=str(uuid4())
    )


async def test_only_qs_workload_can_read_flow(handler):
    service, reader = handler
    context = Context()
    context.auth_context = lambda: {}
    with pytest.raises(Aborted) as denied:
        await service.GetSolution(query(), context)
    assert denied.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    reader.solution.assert_not_awaited()


async def test_flow_requires_valid_single_scoped_identity(handler):
    service, reader = handler
    invalid = query()
    invalid.publication_id = str(uuid4())
    with pytest.raises(Aborted):
        await service.GetSolution(invalid, Context())
    reader.solution.assert_not_awaited()
    response = await service.GetSolution(query(), Context())
    assert response.schema_version == "qs-ai-flow/v1"
    assert json.loads(response.data_json) == {"nodes": []}
    assert reader.solution.await_args.args[0].organization_id == 1


async def test_broken_snapshot_never_returns_prompt_body(handler):
    service, reader = handler
    reader.solution.side_effect = ValueError("private Prompt")
    with pytest.raises(Aborted) as broken:
        await service.GetSolution(query(), Context())
    assert broken.value.args[0] == grpc.StatusCode.FAILED_PRECONDITION
    assert "private" not in str(broken.value)
