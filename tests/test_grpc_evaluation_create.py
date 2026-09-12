from contextlib import asynccontextmanager
from dataclasses import asdict
from unittest.mock import AsyncMock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_grpc_commands import Aborted, Context


@pytest.fixture
async def creation(complete_release):
    store = AsyncMock(spec=EvaluationRequests)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationRequests
                return store

        yield Scope()

    request = pb.EvaluationCreateCommand(
        scope=pb.EvaluationQuery(run_id=str(uuid4()), organization_id=1, operator_user_id=42),
        release=pb.EvaluationRelease(**asdict(complete_release)),
        reason="创建评测",
        confirm=True,
    )
    store.create.return_value = EvaluationView(request.scope.run_id, 1, "requested", 0, "[]")
    return EvaluationManagement(container), store, request


async def test_creation_freezes_all_refs_and_uses_trusted_identity_and_server_time(
    creation, complete_release
):
    service, store, request = creation
    reply = await service.Create(request, Context())
    scope, release, reason, at = store.create.await_args.args
    assert release == complete_release and scope.actor == "user:42"
    assert reason == "创建评测" and at.utcoffset().total_seconds() == 0
    assert reply.status == "requested" and reply.run_id == request.scope.run_id
    assert store.create.await_args.kwargs == {"confirm": True}


@pytest.mark.parametrize("invalid", ["scope", "confirm", "release", "workload"])
async def test_invalid_creation_never_reaches_store(creation, invalid):
    service, store, request = creation
    context = Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if invalid == "scope":
        request.scope.organization_id = 0
    elif invalid == "confirm":
        request.confirm = False
    elif invalid == "release":
        request.release.ClearField("semantic_route")
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.Create(request, context)
    assert error.value.args[0] == expected
    store.create.assert_not_awaited()
