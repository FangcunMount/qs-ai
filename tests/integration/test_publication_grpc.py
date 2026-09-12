"""Actual mTLS -> DI -> MySQL publication lifecycle with synthetic evaluated output."""

from dataclasses import asdict, replace
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
async def running(ready, tmp_path):
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
        yield tx, scope, command, client
    finally:
        for channel in channels:
            await channel.close()
        await server.stop(0)
        await container.close()


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
