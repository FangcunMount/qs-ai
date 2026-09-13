"""Go current authority -> mTLS Python -> real participant reservation ledger."""

import asyncio
import json
import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.participant import ParticipantManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_participant_capacity import reservations, start

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_participant_capacity_preserves_filters_and_current_admin_boundary(
    kit, go_management, tmp_path
):
    await start(kit, kit.service)
    await kit.store.claim(60)
    before = await reservations(kit)
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_ParticipantManagementServicer_to_server(ParticipantManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    async def call(certificate="qs", **changes):
        request = {
            "Action": "participant-capacity",
            "OrgID": 1,
            "Allowed": True,
            "ParticipantCapacity": {"subject_id": kit.actor.subject_id, "assessment_id": "42"},
        }
        request.update(changes)
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / f"{certificate}.pem"),
            str(tmp_path / f"{certificate}.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(json.dumps(request).encode()), 15
            )
            assert process.returncode == 0
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        result = await call()
        assert result["Code"] == "OK"
        value = result["State"]
        assert value["subject"]["identity"] == kit.actor.subject_id
        assert value["assessment"]["daily_reserved"] == value["organization"]["active"] == 1
        assert value["daily_reservations"][0]["run_id"] == before[0]["run_id"]
        assert value["active_reservations"] == value["daily_reservations"]
        assert (await call(AuditOnly=True))["Denied"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(OrgID=2))["State"]["organization"]["daily_reserved"] == 0
        assert (await call(ParticipantCapacity={"assessment_id": "0"}))["Invalid"]
        assert await reservations(kit) == before
    finally:
        await server.stop(0)
        await container.close()


async def test_go_participant_retry_current_authority_unknown_confirmation_and_receipt(
    kit, go_management, tmp_path
):
    from dataclasses import asdict

    from dishka import Provider, Scope, provide

    from qs_ai.application.interpretation.ports import EvidenceSource
    from tests.integration.test_participant_retries import blocked, calls

    command, _ = await blocked(kit, unknown=True)
    original_calls = await calls(kit, command.session_id)

    class CurrentAccess(Provider):
        @provide(scope=Scope.APP, override=True)
        def source(self) -> EvidenceSource:
            return kit.source

    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        ),
        CurrentAccess(),
    )
    server = grpc.aio.server()
    rpc.add_ParticipantManagementServicer_to_server(ParticipantManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    payload = asdict(command)
    payload.pop("scope")
    payload.pop("session_id")

    async def call(action, certificate="qs", **changes):
        value = {
            "Action": action,
            "OrgID": 1,
            "Allowed": True,
            "SessionID": command.session_id,
            "CommandID": command.command_id,
            "ParticipantRetry": payload,
        }
        value.update(changes)
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / f"{certificate}.pem"),
            str(tmp_path / f"{certificate}.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(json.dumps(value).encode()), 15)
            assert process.returncode == 0
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        before = await call("participant-get")
        assert before["Code"] == "OK"
        state = before["State"]
        assert state["can_retry"] and state["unknown_result_risk"]
        assert (
            state["run_id"] == command.expected_run_id
            and state["version"] == command.expected_version
        )
        assert (await call("participant-get", OrgID=2))["Code"] == "NotFound"
        assert (await call("participant-retry", Allowed=False))["Denied"]
        assert (await call("participant-retry", AuditOnly=True))["Denied"]
        assert (await call("participant-retry", certificate="other"))["Code"] == "PermissionDenied"
        assert (
            await call(
                "participant-retry",
                ParticipantRetry={**payload, "accept_result_unknown_risk": False},
            )
        )["Code"] == "Aborted"
        kit.source.revoked = True
        assert (await call("participant-retry"))["Code"] == "PermissionDenied"
        kit.source.revoked = False
        accepted = await call("participant-retry")
        assert (
            accepted["Code"] == "OK"
            and accepted["State"]["version"] == command.expected_version + 1
        )
        assert accepted["State"]["run_id"] != command.expected_run_id
        assert (await call("participant-retry"))["State"] == accepted["State"]
        assert (await call("participant-receipt"))["State"] == accepted["State"]
        assert (await call("participant-receipt", UserID=99))["Code"] == "NotFound"
        after = (await call("participant-get"))["State"]
        assert not after["can_retry"] and after["source_run_id"] == command.expected_run_id
        kit.source.revoked = True
        assert (await call("participant-receipt"))["Code"] == "PermissionDenied"
        assert await calls(kit, command.session_id) == original_calls
        assert len(await reservations(kit)) == 2
    finally:
        await server.stop(0)
        await container.close()
