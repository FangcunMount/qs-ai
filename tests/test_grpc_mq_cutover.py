"""Real mTLS bootstrap routing; substitutes never open transactions or invoke models."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from qs_ai.bootstrap.grpc_server import create_grpc_server
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.commands import Commands
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from qs_ai.transport.grpc.mq_cutover import MQ_REQUIRED
from qs_ai.transport.grpc.participant import ParticipantManagement
from qs_ai.transport.grpc.solutions import SolutionManagement
from tests.integration.test_delivery import certificates
from tests.test_single_server import port

WRITES = [
    (Commands, rpc.CommandsStub, "Start", pb.StartCommand, pb.Receipt),
    (Commands, rpc.CommandsStub, "Change", pb.ChangeCommand, pb.Receipt),
    (
        ParticipantManagement,
        rpc.ParticipantManagementStub,
        "Retry",
        pb.ParticipantRetryCommand,
        pb.Receipt,
    ),
    (
        EvaluationManagement,
        rpc.EvaluationManagementStub,
        "Start",
        pb.EvaluationStartCommand,
        pb.EvaluationState,
    ),
    (
        EvaluationManagement,
        rpc.EvaluationManagementStub,
        "Cancel",
        pb.EvaluationCancelCommand,
        pb.EvaluationState,
    ),
]
RETAINED = [
    (Commands, rpc.CommandsStub, "CheckEligibility", pb.EligibilityQuery, pb.EligibilityStatus),
    (
        ParticipantManagement,
        rpc.ParticipantManagementStub,
        "GetExecution",
        pb.ParticipantExecutionQuery,
        pb.ParticipantExecution,
    ),
    (
        ParticipantManagement,
        rpc.ParticipantManagementStub,
        "GetRetryReceipt",
        pb.ParticipantRetryReceiptQuery,
        pb.Receipt,
    ),
    (
        EvaluationManagement,
        rpc.EvaluationManagementStub,
        "Get",
        pb.EvaluationQuery,
        pb.EvaluationState,
    ),
    (
        EvaluationManagement,
        rpc.EvaluationManagementStub,
        "Create",
        pb.EvaluationCreateCommand,
        pb.EvaluationState,
    ),
    (
        EvaluationManagement,
        rpc.EvaluationManagementStub,
        "Prepare",
        pb.EvaluationPlanQuery,
        pb.EvaluationPlan,
    ),
    (SolutionManagement, rpc.SolutionManagementStub, "Get", pb.SolutionQuery, pb.SolutionResponse),
    (
        SolutionManagement,
        rpc.SolutionManagementStub,
        "GetModels",
        pb.SolutionQuery,
        pb.SolutionResponse,
    ),
    (
        SolutionManagement,
        rpc.SolutionManagementStub,
        "Prepare",
        pb.SolutionWrite,
        pb.SolutionResponse,
    ),
]


@pytest.fixture
async def endpoint(tmp_path, monkeypatch):
    certificates(tmp_path)
    handlers = {}
    for service, _, method, _, response in WRITES + RETAINED:
        handler = AsyncMock(return_value=response())
        monkeypatch.setattr(service, method, handler)
        handlers[service, method] = handler
    container = MagicMock(side_effect=AssertionError("No transaction scope permitted"))
    ca, cert, key = [(tmp_path / f).read_bytes() for f in ("ca.pem", "ai.pem", "ai.key")]

    @asynccontextmanager
    async def start(enabled, identity="qs"):
        bound = port()
        options = {
            "enabled": enabled,
            "nsqd": {"localhost:4150": "http://localhost:4151"},
            "signing_key_file": "/unused/sign.jwk",
            "decrypt_key_files": {"ai.encrypt": "/unused/decrypt.jwk"},
            "qs_signer_files": {"qs.sign": "/unused/qs-sign.jwk"},
            "qs_recipient_key_file": "/unused/qs-encrypt.jwk",
        }
        server = create_grpc_server(
            container,
            Settings(
                grpc={"bind_address": f"localhost:{bound}", "governance_enabled": True},
                messaging=options,
            ),
            ca,
            cert,
            key,
        )
        await server.start()
        try:
            credentials = grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            )
            async with grpc.aio.secure_channel(f"localhost:{bound}", credentials) as channel:
                yield channel
        finally:
            await server.stop(0)

    yield SimpleNamespace(start=start, handlers=handlers, container=container)
    container.assert_not_called()


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("case", WRITES, ids=[f"{v[0].__name__}.{v[2]}" for v in WRITES])
async def test_five_execution_writes_cannot_bypass_mq_admission(endpoint, enabled, case):
    service, stub, method, request, response = case
    handler = endpoint.handlers[service, method]
    async with endpoint.start(enabled) as channel:
        call = getattr(stub(channel), method)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await call(request(), timeout=3)
        assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert error.value.details() == MQ_REQUIRED
        handler.assert_not_awaited()
        for old_service, _, old_method, *_ in WRITES:
            endpoint.handlers[old_service, old_method].assert_not_awaited()


@pytest.mark.parametrize("case", WRITES, ids=[f"{v[0].__name__}.{v[2]}" for v in WRITES])
async def test_mq_cutover_keeps_workload_authorization_before_mode_error(endpoint, case):
    service, stub, method, request, _ = case
    async with endpoint.start(True, "other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await getattr(stub(channel), method)(request(), timeout=3)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    endpoint.handlers[service, method].assert_not_awaited()


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("case", RETAINED, ids=[f"{v[0].__name__}.{v[2]}" for v in RETAINED])
async def test_queries_and_governance_preparation_stay_registered(endpoint, enabled, case):
    service, stub, method, request, response = case
    async with endpoint.start(enabled) as channel:
        assert await getattr(stub(channel), method)(request(), timeout=3) == response()
    endpoint.handlers[service, method].assert_awaited_once()
    for old_service, _, old_method, *_ in WRITES:
        endpoint.handlers[old_service, old_method].assert_not_awaited()
