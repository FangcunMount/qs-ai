"""Explicit internal mTLS entry; no development identity or workflow fallback."""

import argparse
import asyncio
from pathlib import Path

import grpc
from dishka import Provider, Scope, provide
from grpc import aio

from qs_ai.application.integration.events import DeliverResults, ResultReceiver
from qs_ai.bootstrap.worker import worker_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver
from qs_ai.transport.grpc.commands import Commands


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["serve", "deliver"])
    parser.add_argument("--address", required=True, help="Bind address or QS callback target")
    parser.add_argument("--ca", required=True)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()
    ca, cert, key = await asyncio.gather(
        *(asyncio.to_thread(Path(path).read_bytes) for path in (args.ca, args.cert, args.key))
    )
    if args.mode == "serve":
        async with worker_container(Settings()) as container:
            server = aio.server(options=(("grpc.max_receive_message_length", 65536),))
            rpc.add_CommandsServicer_to_server(Commands(container), server)
            credentials = grpc.ssl_server_credentials(
                [(key, cert)], root_certificates=ca, require_client_auth=True
            )
            if server.add_secure_port(args.address, credentials) == 0:
                raise RuntimeError("Unable to bind internal service")
            await server.start()
            try:
                await server.wait_for_termination()
            finally:
                await server.stop(5)
    else:
        async with mtls_channel(args.address, ca, key, cert) as channel:

            class ReceiverProvider(Provider):
                @provide(scope=Scope.APP, provides=ResultReceiver, override=True)
                def receiver(self) -> ResultReceiver:
                    return GRPCResultReceiver(channel)

            async with worker_container(Settings(), ReceiverProvider()) as delivery_container:
                async with delivery_container() as operation:
                    count = await (await operation.get(DeliverResults)).once()
                    print(f"Delivered {count} result events")


if __name__ == "__main__":
    asyncio.run(main())
