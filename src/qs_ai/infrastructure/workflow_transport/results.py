from dataclasses import asdict

from grpc import aio

from qs_ai.application.integration.events import StateEvent
from qs_ai.application.interpretation.ports import DependencyUnavailable
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc


class GRPCResultReceiver:
    def __init__(self, channel: aio.Channel, timeout_seconds: float = 5) -> None:
        self.timeout_seconds = timeout_seconds
        self.stub = rpc.ResultsStub(channel)

    async def accept(self, event: StateEvent) -> None:
        response = await self.stub.Accept(
            pb.StateEvent(**asdict(event)), timeout=self.timeout_seconds
        )
        if response.event_id != event.event_id:
            raise DependencyUnavailable("Unmatched delivery acknowledgement")


class UnconfiguredReceiver:
    async def accept(self, event: StateEvent) -> None:
        raise DependencyUnavailable("Result receiver is not configured")
