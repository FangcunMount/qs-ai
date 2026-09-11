import asyncio
import json
import os
from dataclasses import asdict

import pytest
from sqlalchemy import select, text, update

from qs_ai.application.integration.events import DeliverResults
from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox
from qs_ai.infrastructure.persistence.mysql.schema import external_requests, result_outbox
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver
from tests.integration.test_artifact_acceptance import ready
from tests.integration.test_delivery import certificates, stop
from tests.integration.test_interpretation import kit as kit

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_complete_artifact_survives_go_receiver_restart_and_lost_ack(kit, tmp_path):
    binary = os.getenv("QS_AI_ARTIFACT_BRIDGE_BIN")
    dsn = os.getenv("QS_AI_TEST_QS_GO_DSN")
    if not binary or not dsn or not os.getenv("QS_AI_TEST_QS_MYSQL_DSN"):
        pytest.skip("Requires artifact-capable Go bridge and disposable QS database")
    certificates(tmp_path)
    env = {**os.environ, "QS_AI_BRIDGE_DSN": dsn}

    async def go(*args):
        process = await asyncio.create_subprocess_exec(
            binary, *args, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            output, error = await asyncio.wait_for(process.communicate(), 15)
            assert process.returncode == 0, error.decode()
            return output.decode()
        finally:
            if process.returncode is None:
                await stop(process)

    claim, artifact = await ready(kit)
    evidence = await kit.store.evidence(claim)
    async with kit.transactions.open() as db:
        request_id = await db.scalar(
            select(external_requests.c.request_id).where(
                external_requests.c.session_id == claim.session.id
            )
        )
    command = tmp_path / "start.json"
    command.write_text(
        json.dumps(
            {
                "request_id": request_id,
                "actor": asdict(claim.session.actor),
                "testee_id": "7",
                "assessment_ids": ["42"],
                "goal": "synthetic artifact delivery",
                "evidence": [asdict(item) for item in evidence.items],
            }
        )
    )
    await go("-mode", "stage-start", "-input", str(command))
    await kit.store.finish(claim, WorkflowResult("", artifact=artifact))
    receiver = None

    async def receive(address):
        process = await asyncio.create_subprocess_exec(
            binary,
            "-mode",
            "receive",
            "-address",
            address,
            "-ca",
            str(tmp_path / "ca.pem"),
            "-cert",
            str(tmp_path / "qs.pem"),
            "-key",
            str(tmp_path / "qs.key"),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            line = await asyncio.wait_for(process.stdout.readline(), 10)
            assert line.startswith(b"LISTENING")
            return process, "localhost:" + line.decode().strip().rsplit(":", 1)[-1]
        except BaseException:
            await stop(process)
            raise

    try:
        receiver, target = await receive("localhost:0")
        outbox = MySQLResultOutbox(kit.transactions)
        events = sorted(await outbox.pending(20), key=lambda event: event.version, reverse=True)
        async with mtls_channel(
            target,
            (tmp_path / "ca.pem").read_bytes(),
            (tmp_path / "ai.key").read_bytes(),
            (tmp_path / "ai.pem").read_bytes(),
        ) as channel:
            remote = GRPCResultReceiver(channel)
            # Completion precedes the older running and queued snapshots.
            for event in events:
                await remote.accept(event)
            before = json.loads(await go("-mode", "projection", "-request-id", request_id))
            assert before["status"] == "completed"
            assert json.loads(before["artifact_json"]) == asdict(artifact)

            class LostAck:
                async def accept(self, event):
                    await remote.accept(event)
                    raise TimeoutError("Injected acknowledgement loss")

            assert await DeliverResults(outbox, LostAck()).once() == 0
            await stop(receiver)
            receiver, _ = await receive(target)
            async with kit.transactions.open() as db:
                await db.execute(
                    update(result_outbox)
                    .where(result_outbox.c.session_id == claim.session.id)
                    .values(available_at=text("UTC_TIMESTAMP(6)"))
                )
                await db.commit()
            assert await DeliverResults(outbox, remote).once() == len(events)
            assert json.loads(await go("-mode", "projection", "-request-id", request_id)) == before
    finally:
        if receiver is not None:
            await stop(receiver)
        # Only this test's synthetic QS request is removed, never shared data.
        import asyncmy
        from sqlalchemy.engine import make_url

        url = make_url(os.environ["QS_AI_TEST_QS_MYSQL_DSN"])
        connection = await asyncmy.connect(
            host=url.host,
            port=url.port or 3306,
            user=url.username,
            password=url.password,
            db=url.database,
        )
        try:
            async with connection.cursor() as cursor:
                for table in ("ai_bridge_events", "ai_bridge_commands", "ai_bridge_requests"):
                    await cursor.execute(f"DELETE FROM {table} WHERE request_id=%s", (request_id,))
            await connection.commit()
        finally:
            connection.close()
