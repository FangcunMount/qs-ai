"""Actual mTLS -> DI -> MySQL publication lifecycle with synthetic evaluated output."""

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import asdict, replace
from pathlib import Path
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.governance.publication import PublicationScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.transport.grpc.publication import PublicationManagement, selector_message
from tests.integration.test_delivery import certificates
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import freeze_creation as freeze_creation
from tests.integration.test_publications import inventory
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.fixture
async def publication_server(ready, tmp_path):
    tx, scope, command, _ = ready
    certificates(tmp_path)
    assert tx.database.engine is not None
    container = create_container(
        Settings(database_url=tx.database.engine.url.render_as_string(hide_password=False))
    )
    server = grpc.aio.server()
    rpc.add_PublicationManagementServicer_to_server(PublicationManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials(
            [(key, cert)],
            root_certificates=ca,
            require_client_auth=True,
        ),
    )
    await server.start()
    channels = []

    def client(identity="qs"):
        credential = grpc.ssl_channel_credentials(
            ca,
            (tmp_path / f"{identity}.key").read_bytes(),
            (tmp_path / f"{identity}.pem").read_bytes(),
        )
        channel = grpc.aio.secure_channel(f"localhost:{port}", credential)
        channels.append(channel)
        return rpc.PublicationManagementStub(channel)

    try:
        yield tx, scope, command, client, port
    finally:
        for channel in channels:
            await channel.close()
        await server.stop(0)
        await container.close()


@pytest.fixture
async def running(publication_server):
    return publication_server[:4]


@pytest.fixture
async def go_publication(tmp_path):
    source = os.getenv("QS_AI_GOVERNANCE_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires isolated QS governance checkout and Go")
    root = Path(source)
    with tempfile.TemporaryDirectory(
        prefix="qs_ai_publication_", dir=root / "scripts"
    ) as directory:
        program = Path(directory) / "main.go"
        shutil.copyfile(
            Path(__file__).parents[1] / "fixtures/go_publication_management.go", program
        )
        binary = tmp_path / "publication"
        process = await asyncio.create_subprocess_exec(
            "go",
            "build",
            "-o",
            str(binary),
            str(program),
            cwd=root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, error = await asyncio.wait_for(process.communicate(), 120)
            assert process.returncode == 0, error.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
    return binary


@pytest.fixture
async def go_client(publication_server, go_publication, tmp_path):
    _, scope, _, _, port = publication_server

    async def invoke(action, identity="qs", **overrides):
        request = {
            "Action": action,
            "OrgID": scope.organization_id,
            "UserID": scope.operator_user_id,
            "Allowed": True,
            **overrides,
        }
        process = await asyncio.create_subprocess_exec(
            str(go_publication),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / f"{identity}.pem"),
            str(tmp_path / f"{identity}.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, error = await asyncio.wait_for(
                process.communicate(json.dumps(request, ensure_ascii=False).encode()), 20
            )
            assert process.returncode == 0, error.decode()
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    return invoke


def go_publish(command):
    return {
        "command_id": str(command.command_id),
        "reason": command.reason,
        "confirm": True,
        "expected": {
            "selector": asdict(command.selector),
            "version": 0,
            "active_publication_id": "",
        },
        "run_id": str(command.run_id),
        "run_version": command.run_version,
        "release_fingerprint": command.release_fingerprint,
    }


@pytest.mark.interop
async def test_go_publish_read_receipt_replace_rollback_disable(publication_server, go_client):
    tx, _, command, _, _ = publication_server
    request = go_publish(command)
    first = await go_client("publish", Publish=request)
    assert first["Code"] == "OK" and not first["Conflict"], first
    original = first["State"]
    assert original["current"]["version"] == 1
    before = await inventory(tx)
    # Every call starts a fresh Go process/connection. Receipt reads never replay mutations.
    recovered = await go_client("receipt", CommandID=request["command_id"], AuditOnly=True)
    assert recovered["State"] == original and recovered["Code"] == "OK"
    assert (await go_client("publish", Publish=request))["State"] == original
    assert await inventory(tx) == before
    selector = request["expected"]["selector"]
    assert (await go_client("get", Selector=selector, AuditOnly=True))["State"] == original[
        "current"
    ]
    second_request = {
        **request,
        "command_id": str(uuid4()),
        "expected": {
            "selector": selector,
            "version": 1,
            "active_publication_id": original["current"]["active_publication_id"],
        },
    }
    second = await go_client("publish", Publish=second_request)
    assert second["Code"] == "OK", second
    rollback = {
        "command_id": str(uuid4()),
        "reason": "回退原版本",
        "confirm": True,
        "expected": {
            "selector": selector,
            "version": 2,
            "active_publication_id": second["State"]["current"]["active_publication_id"],
        },
        "target_publication_id": original["current"]["active_publication_id"],
    }
    back = await go_client("rollback", Rollback=rollback)
    assert back["Code"] == "OK", back
    assert back["State"]["current"]["publication"] == original["current"]["publication"]
    disable = {
        "command_id": str(uuid4()),
        "reason": "停用核验",
        "confirm": True,
        "expected": {
            "selector": selector,
            "version": 3,
            "active_publication_id": original["current"]["active_publication_id"],
        },
    }
    stopped = await go_client("disable", Disable=disable)
    assert stopped["Code"] == "OK", stopped
    assert stopped["State"]["current"]["version"] == 4
    assert not stopped["State"]["current"]["active_publication_id"]
    assert [len(values) for values in await inventory(tx)] == [2, 1, 4]


@pytest.mark.interop
async def test_go_publication_denial_stale_and_foreign_receipts(publication_server, go_client):
    tx, _, command, _, _ = publication_server
    request = go_publish(command)
    for overrides in ({"Allowed": False}, {"AuditOnly": True}):
        assert (await go_client("publish", Publish=request, **overrides))["Denied"]
    for identity in ("other", "ai"):
        assert (await go_client("publish", identity=identity, Publish=request))[
            "Code"
        ] == "PermissionDenied"
    assert (await go_client("publish", Publish=request, OrgID=2))["Code"] == "NotFound"
    assert (await go_client("publish", Publish={**request, "confirm": False}))["Invalid"]
    assert [len(values) for values in await inventory(tx)] == [0, 0, 0]
    assert (await go_client("publish", Publish=request))["Code"] == "OK"
    before = await inventory(tx)
    assert (await go_client("publish", Publish={**request, "command_id": str(uuid4())}))[
        "Code"
    ] == "Aborted"
    assert (await go_client("publish", Publish=request, Allowed=False))["Denied"]
    for altered in ({"OrgID": 2}, {"UserID": 43}):
        assert (await go_client("receipt", CommandID=request["command_id"], **altered))[
            "Code"
        ] == "NotFound"
    assert await inventory(tx) == before


def publish_request(scope, command):
    return pb.PublicationPublishCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        command_id=str(command.command_id),
        expected=pb.PublicationExpectation(
            selector=selector_message(command.selector), version=command.expected_version
        ),
        reason=command.reason,
        confirm=command.confirm,
        run_id=str(command.run_id),
        run_version=command.run_version,
        release_fingerprint=command.release_fingerprint,
    )


async def test_full_lifecycle_and_lost_acknowledgement_reconcile_over_mtls(running):
    tx, scope, command, client = running
    stub = client()
    request = publish_request(scope, command)
    first = await stub.Publish(request, timeout=10)
    query = pb.PublicationReceiptQuery(scope=request.scope, command_id=request.command_id)
    before = await inventory(tx)
    # Simulate a lost acknowledgement by discarding first and reading via a fresh channel.
    assert await client().GetReceipt(query, timeout=10) == first
    assert await stub.Publish(request, timeout=10) == first
    assert await inventory(tx) == before
    state_query = pb.PublicationQuery(scope=request.scope, selector=request.expected.selector)
    assert await stub.Get(state_query, timeout=10) == first.current
    second_request = publish_request(
        scope, replace(command, command_id=uuid4(), expected_version=1)
    )
    second_request.expected.active_publication_id = first.current.active_publication_id
    second = await stub.Publish(second_request, timeout=10)
    back = await stub.Rollback(
        pb.PublicationRollbackCommand(
            scope=request.scope,
            command_id=str(uuid4()),
            reason="回退原配置",
            confirm=True,
            expected=pb.PublicationExpectation(
                selector=request.expected.selector,
                version=2,
                active_publication_id=second.current.active_publication_id,
            ),
            target_publication_id=first.current.active_publication_id,
        ),
        timeout=10,
    )
    assert (
        back.current.version == 3
        and back.current.publication_json == first.current.publication_json
    )
    stopped = await stub.Disable(
        pb.PublicationDisableCommand(
            scope=request.scope,
            command_id=str(uuid4()),
            reason="停用核验",
            confirm=True,
            expected=pb.PublicationExpectation(
                selector=request.expected.selector,
                version=3,
                active_publication_id=first.current.active_publication_id,
            ),
        ),
        timeout=10,
    )
    assert stopped.current.version == 4 and not stopped.current.active_publication_id
    assert await stub.Get(state_query, timeout=10) == stopped.current
    assert [len(values) for values in await inventory(tx)] == [2, 1, 4]


async def test_wrong_workload_foreign_scope_and_stale_confirmation_cannot_mutate(running):
    tx, scope, command, client = running
    request = publish_request(scope, command)
    for identity in ("other", "ai"):
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client(identity).Publish(request, timeout=10)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert [len(values) for values in await inventory(tx)] == [0, 0, 0]
    stub = client()
    foreign = publish_request(replace(scope, organization_id=2), command)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Publish(foreign, timeout=10)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    request.confirm = False
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Publish(request, timeout=10)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    request.confirm = True
    await stub.Publish(request, timeout=10)
    before = await inventory(tx)
    request.command_id = str(uuid4())
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Publish(request, timeout=10)
    assert error.value.code() == grpc.StatusCode.ABORTED
    for altered in (PublicationScope(2, 42), PublicationScope(1, 43)):
        query = pb.PublicationReceiptQuery(
            scope=pb.PublicationScope(**asdict(altered)), command_id=str(command.command_id)
        )
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await stub.GetReceipt(query, timeout=10)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert await inventory(tx) == before
    with pytest.raises(NotFound):
        await MySQLPublications(tx).get_receipt(scope, uuid4())


async def test_history_over_real_mtls_preserves_audit_and_original_receipt_scope(running):
    tx, scope, command, client = running
    store = MySQLPublications(tx)
    from datetime import UTC, datetime

    receipt = await store.apply(scope, command, datetime.now(UTC))
    before = await inventory(tx)
    other = pb.PublicationScope(organization_id=2, operator_user_id=43)
    query = pb.PublicationHistoryQuery(
        scope=other, selector=selector_message(command.selector), limit=20
    )
    stub = client()
    response = await stub.ListHistory(query)
    body = json.loads(response.payload_json)
    assert body["entries"][0]["actor"] == "user:42"
    assert body["entries"][0]["version"] == 1
    assert "definition_json" not in response.payload_json
    history = await stub.GetHistory(
        pb.PublicationHistoryVersionQuery(scope=other, selector=query.selector, version=1)
    )
    assert history.command_id == str(receipt.command_id) and history.actor == "user:42"
    assert history.current.publication_json
    with pytest.raises(grpc.aio.AioRpcError) as denied:
        await stub.GetReceipt(
            pb.PublicationReceiptQuery(scope=other, command_id=str(receipt.command_id))
        )
    assert denied.value.code() == grpc.StatusCode.NOT_FOUND
    with pytest.raises(grpc.aio.AioRpcError) as wrong_identity:
        await client("ai").ListHistory(query)
    assert wrong_identity.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert await inventory(tx) == before
