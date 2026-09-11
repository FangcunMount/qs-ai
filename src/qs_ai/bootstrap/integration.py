"""Explicit internal mTLS entry; no development identity or workflow fallback."""

import argparse
import asyncio
from pathlib import Path

import grpc
from dishka import Provider, Scope, provide
from grpc import aio

from qs_ai.application.integration.events import DeliverResults, ResultReceiver
from qs_ai.bootstrap.daemon import serve_loop
from qs_ai.bootstrap.worker import worker_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver
from qs_ai.transport.grpc.commands import Commands


async def main() -> None:
    settings = Settings()
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["serve", "deliver"])
    parser.add_argument(
        "--continuous", action="store_true", help="Continuously deliver result events"
    )
    parser.add_argument("--address", help="Override bind address or QS callback target")
    parser.add_argument("--ca", default=settings.grpc.ca_file)
    parser.add_argument("--cert", default=settings.grpc.cert_file)
    parser.add_argument("--key", default=settings.grpc.key_file)
    args = parser.parse_args()
    if args.continuous and args.mode != "deliver":
        parser.error("--continuous applies only to result delivery")
    args.address = args.address or (
        settings.grpc.bind_address if args.mode == "serve" else settings.grpc.result_address
    )
    if not all((args.address, args.ca, args.cert, args.key)):
        parser.error("Address and TLS CA, certificate and key paths must be configured")
    ca, cert, key = await asyncio.gather(
        *(asyncio.to_thread(Path(path).read_bytes) for path in (args.ca, args.cert, args.key))
    )
    if args.mode == "serve":
        async with worker_container(settings) as container:
            server = aio.server(
                options=(("grpc.max_receive_message_length", settings.grpc.max_receive_bytes),)
            )
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
                await server.stop(settings.grpc.shutdown_grace_seconds)
    else:
        async with mtls_channel(args.address, ca, key, cert) as channel:

            class ReceiverProvider(Provider):
                @provide(scope=Scope.APP, provides=ResultReceiver, override=True)
                def receiver(self) -> ResultReceiver:
                    return GRPCResultReceiver(channel, settings.grpc.request_timeout_seconds)

            async with worker_container(settings, ReceiverProvider()) as delivery_container:
                if args.continuous:

                    async def attempt() -> int:
                        async with delivery_container() as operation:
                            return await (await operation.get(DeliverResults)).once(
                                settings.delivery.batch_size
                            )

                    await serve_loop(
                        attempt,
                        **settings.delivery.model_dump(exclude={"batch_size", "max_retry_seconds"}),
                    )
                    return
                async with delivery_container() as operation:
                    count = await (await operation.get(DeliverResults)).once(
                        settings.delivery.batch_size
                    )
                    print(f"Delivered {count} result events")


if __name__ == "__main__":
    asyncio.run(main())
