"""Non-mutating TLS/workload probe: invalid Change never opens a business transaction."""

import argparse
import asyncio
import json
import ssl
from pathlib import Path

import grpc

from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc


async def probe(
    address: str, ca: bytes, cert: bytes | None, key: bytes | None, expected: str
) -> None:
    credentials = grpc.ssl_channel_credentials(ca, key, cert)
    async with grpc.aio.secure_channel(address, credentials) as channel:
        try:
            await rpc.CommandsStub(channel).Change(
                pb.ChangeCommand(actor=pb.Actor(org_id="1", subject_id="transport-probe")),
                timeout=5,
            )
        except grpc.aio.AioRpcError as error:
            actual = error.code().name
            if actual != expected:
                raise RuntimeError(f"Expected {expected}, received {actual}") from None
            print(json.dumps({"probe": "passed", "expected_status": actual}))
            return
        raise RuntimeError("Invalid probe command unexpectedly accepted")


def main() -> None:
    settings = Settings()
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="qs-ai-grpc:50061")
    parser.add_argument("--ca", default=settings.grpc.ca_file)
    parser.add_argument("--cert", default=settings.grpc.cert_file)
    parser.add_argument("--key", default=settings.grpc.key_file)
    parser.add_argument("--anonymous", action="store_true")
    parser.add_argument("--check-files", action="store_true")
    parser.add_argument(
        "--expect",
        choices=["INVALID_ARGUMENT", "PERMISSION_DENIED", "UNAVAILABLE"],
        default="PERMISSION_DENIED",
    )
    args = parser.parse_args()
    if not args.ca or (not args.anonymous and not (args.cert and args.key)):
        parser.error("TLS file paths are required")
    if args.check_files:
        context = ssl.create_default_context(cafile=args.ca)
        context.load_cert_chain(args.cert, args.key)
        print(json.dumps({"tls_files": "readable_and_key_matches"}))
        return
    asyncio.run(
        probe(
            args.address,
            Path(args.ca).read_bytes(),
            None if args.anonymous else Path(args.cert).read_bytes(),
            None if args.anonymous else Path(args.key).read_bytes(),
            args.expect,
        )
    )


if __name__ == "__main__":
    main()
