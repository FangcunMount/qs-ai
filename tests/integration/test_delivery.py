import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy import delete, select, text, update

from qs_ai.application.integration.events import DeliverResults
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import external_requests, result_outbox
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver
from tests.integration.test_interpretation import kit  # noqa: F401

pytestmark = [
    pytest.mark.integration,
    pytest.mark.interop,
    pytest.mark.usefixtures("published_configuration"),
]


def certificates(path):
    root_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])

    def issue(name, key, ca=False):
        return (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
            .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(root_key, hashes.SHA256())
        )

    (path / "ca.pem").write_bytes(
        issue(issuer, root_key, True).public_bytes(serialization.Encoding.PEM)
    )
    for short, common in [
        ("qs", "qs-apiserver.svc"),
        ("ai", "qs-ai.svc"),
        ("other", "untrusted.svc"),
    ]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])
        (path / f"{short}.pem").write_bytes(
            issue(name, key).public_bytes(serialization.Encoding.PEM)
        )
        (path / f"{short}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )


async def stop(process):
    if process.returncode is None:
        process.kill()
    await process.communicate()


async def test_go_python_durable_round_trip(kit, tmp_path):  # noqa: F811
    binary = os.getenv("QS_AI_BRIDGE_BIN")
    qs_dsn = os.getenv("QS_AI_TEST_QS_MYSQL_DSN")
    if not binary or not qs_dsn or not os.getenv("QS_AI_TEST_QS_GO_DSN"):
        pytest.skip("Requires Go bridge binary, its migration and disposable QS MySQL DSN")
    certificates(tmp_path)
    ca, ai_cert, ai_key, qs_cert, qs_key = [
        tmp_path / name for name in ("ca.pem", "ai.pem", "ai.key", "qs.pem", "qs.key")
    ]
    database = Database(qs_dsn)
    transactions = Transactions(database)
    # Schema is provisioned once by the explicit local interop setup.
    request_id = str(uuid4())
    start = {
        "request_id": request_id,
        "actor": asdict(kit.actor),
        "testee_id": "7",
        "assessment_ids": ["42"],
        "goal": "Synthetic delivery verification",
    }
    from tests.test_input_binding import bound_case

    start["evidence"] = [asdict(item) for item in bound_case()[1].items]
    input_path = tmp_path / "command.json"
    input_path.write_text(json.dumps(start))
    env = {**os.environ, "QS_AI_BRIDGE_DSN": os.environ["QS_AI_TEST_QS_GO_DSN"]}

    async def go(*args):
        process = await asyncio.create_subprocess_exec(
            binary, *args, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            output, error = await asyncio.wait_for(process.communicate(), 15)
            assert process.returncode == 0, error.decode()
            return output.decode().strip()
        finally:
            if process.returncode is None:
                await stop(process)

    ai = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.probes.delivery_server",
        str(ca),
        str(ai_cert),
        str(ai_key),
        env={
            **os.environ,
            "QS_AI_DATABASE_URL": kit.dsn.replace("mysql://", "mysql+asyncmy://", 1),
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    receiver = None
    try:
        ai_port = int(await asyncio.wait_for(ai.stdout.readline(), 10))
        receiver = await asyncio.create_subprocess_exec(
            binary,
            "-mode",
            "receive",
            "-address",
            "localhost:0",
            "-ca",
            str(ca),
            "-cert",
            str(qs_cert),
            "-key",
            str(qs_key),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        address = (
            (await asyncio.wait_for(receiver.stdout.readline(), 10)).decode().strip().split()[-1]
        )
        target = "localhost:" + address.rsplit(":", 1)[-1]
        await go("-mode", "stage-start", "-input", str(input_path))
        await go("-mode", "stage-start", "-input", str(input_path))
        relay_args = (
            "-mode",
            "relay",
            "-address",
            f"localhost:{ai_port}",
            "-ca",
            str(ca),
            "-cert",
            str(qs_cert),
            "-key",
            str(qs_key),
        )
        assert await go(*relay_args) == "0"  # AI committed; response intentionally lost.
        async with kit.transactions.open() as db:
            session_id = await db.scalar(
                select(external_requests.c.session_id).where(
                    external_requests.c.request_id == request_id
                )
            )
            assert session_id is not None
            assert (
                len(
                    (
                        await db.scalars(
                            select(external_requests.c.session_id).where(
                                external_requests.c.request_id == request_id
                            )
                        )
                    ).all()
                )
                == 1
            )
        async with kit.transactions.open() as db:
            from qs_ai.infrastructure.persistence.mysql.schema import evidence_sets

            frozen = (
                (
                    await db.execute(
                        select(evidence_sets).where(evidence_sets.c.session_id == session_id)
                    )
                )
                .mappings()
                .one()
            )
            assert frozen["items"] == json.loads(json.dumps(start["evidence"]))
        outbox = MySQLResultOutbox(kit.transactions)
        async with mtls_channel(
            target, ca.read_bytes(), ai_key.read_bytes(), ai_cert.read_bytes()
        ) as channel:
            remote = GRPCResultReceiver(channel)
            # Result arrives before Start acknowledgement; receiver binds the session safely.
            assert await DeliverResults(outbox, remote).once() == 1
            first = json.loads(await go("-mode", "projection", "-request-id", request_id))
            assert first["session_id"] == session_id
            async with transactions.open() as db:
                await db.execute(
                    text(
                        "UPDATE ai_bridge_commands SET available_at=UTC_TIMESTAMP(6) "
                        "WHERE request_id=:id"
                    ),
                    {"id": request_id},
                )
                await db.commit()
            assert await go(*relay_args) == "1"
            assert await kit.worker().once()
            events = [e for e in await outbox.pending(20) if e.session_id == session_id]
            events.sort(key=lambda e: e.version, reverse=True)
            for event in events:  # Explicitly deliver the newest before the older version.
                await remote.accept(event)
                await outbox.delivered(event.event_id)
            view = json.loads(await go("-mode", "projection", "-request-id", request_id))
            assert view["status"] == "awaiting_answer"
            change = {
                "command_id": str(uuid4()),
                "session_id": session_id,
                "actor": asdict(kit.actor),
                "action": "answer",
                "expected_version": view["version"],
                "question_id": view["question_id"],
                "answer": "Father",
                "skip": False,
            }
            input_path.write_text(json.dumps(change))
            await go("-mode", "stage-change", "-request-id", request_id, "-input", str(input_path))
            assert await go(*relay_args) == "1"
            assert await kit.worker().once()

            class LostResultAck:
                async def accept(self, event):
                    await remote.accept(event)
                    raise TimeoutError("Injected lost result acknowledgement")

            assert await DeliverResults(outbox, LostResultAck()).once() == 0
            final = json.loads(await go("-mode", "projection", "-request-id", request_id))
            assert final["status"] == "blocked"
            assert final["failure_code"] == "model_not_connected"
            # Restart the real Go receiver, then replay the already committed events.
            await stop(receiver)
            receiver = await asyncio.create_subprocess_exec(
                binary,
                "-mode",
                "receive",
                "-address",
                target,
                "-ca",
                str(ca),
                "-cert",
                str(qs_cert),
                "-key",
                str(qs_key),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(receiver.stdout.readline(), 10)
            async with kit.transactions.open() as db:
                await db.execute(
                    update(result_outbox)
                    .where(result_outbox.c.session_id == session_id)
                    .values(available_at=text("UTC_TIMESTAMP(6)"))
                )
                await db.commit()
            assert await DeliverResults(outbox, remote).once() >= 1
            assert json.loads(await go("-mode", "projection", "-request-id", request_id)) == final
            wrong = pb.StateEvent(**asdict(events[0]))
            wrong.actor.subject_id = "wrong-owner"
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await rpc.ResultsStub(channel).Accept(wrong, timeout=5)
            assert error.value.code() == grpc.StatusCode.ABORTED
        # A certificate signed by the CA with the wrong workload name is still rejected.
        async with mtls_channel(
            target,
            ca.read_bytes(),
            (tmp_path / "other.key").read_bytes(),
            (tmp_path / "other.pem").read_bytes(),
        ) as channel:
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await rpc.ResultsStub(channel).Accept(pb.StateEvent(**asdict(events[0])), timeout=5)
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
        async with mtls_channel(
            f"localhost:{ai_port}",
            ca.read_bytes(),
            (tmp_path / "other.key").read_bytes(),
            (tmp_path / "other.pem").read_bytes(),
        ) as channel:
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await rpc.CommandsStub(channel).Start(pb.StartCommand(**start), timeout=5)
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        await stop(ai)
        if receiver is not None:
            await stop(receiver)
        async with transactions.open() as db:
            for table in ("ai_bridge_events", "ai_bridge_commands", "ai_bridge_requests"):
                await db.execute(
                    text(f"DELETE FROM {table} WHERE request_id=:id"), {"id": request_id}
                )
            await db.commit()
        await database.close()
        async with kit.transactions.open() as db:
            ids = select(external_requests.c.session_id).where(
                external_requests.c.request_id == request_id
            )
            await db.execute(delete(result_outbox).where(result_outbox.c.session_id.in_(ids)))
            await db.execute(
                delete(external_requests).where(external_requests.c.request_id == request_id)
            )
            await db.commit()
