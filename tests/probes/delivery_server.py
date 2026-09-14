"""Real internal server with test-only dependencies and one lost acknowledgement."""

import asyncio
import sys
from pathlib import Path

import grpc
from dishka import Provider, Scope, provide
from grpc import aio

from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap.worker import worker_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredEvidenceSource
from qs_ai.transport.grpc.commands import Commands


class Fixtures(Provider):
    @provide(scope=Scope.APP, provides=EvidenceSource, override=True)
    def source(self) -> EvidenceSource:
        return UnconfiguredEvidenceSource()


class LostAcknowledgement(Commands):
    lost = False

    async def Start(self, request, context):
        result = await super().Start(request, context)
        if not self.lost:
            self.lost = True
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Injected lost acknowledgement")
        return result


async def main():
    ca, cert, key = await asyncio.gather(
        *(asyncio.to_thread(Path(path).read_bytes) for path in sys.argv[1:])
    )
    async with worker_container(Settings(), Fixtures()) as container:
        server = aio.server()
        rpc.add_CommandsServicer_to_server(LostAcknowledgement(container), server)
        port = server.add_secure_port(
            "localhost:0",
            grpc.ssl_server_credentials(
                [(key, cert)], root_certificates=ca, require_client_auth=True
            ),
        )
        await server.start()
        print(port, flush=True)
        try:
            await server.wait_for_termination()
        finally:
            await server.stop(0)


if __name__ == "__main__":
    asyncio.run(main())
