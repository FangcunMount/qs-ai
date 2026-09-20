"""Actual mTLS and request-scoped DI; user authorization remains QS responsibility."""

import json
import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.asset_catalog import AssetCatalogService
from qs_ai.transport.grpc.quotas import QuotaManagement
from tests.integration.test_asset_catalog import assets as assets
from tests.integration.test_asset_catalog import catalog as catalog
from tests.integration.test_asset_catalog import complete_release as complete_release
from tests.integration.test_asset_catalog import evaluation_release as evaluation_release
from tests.integration.test_asset_catalog import persisted_assets as persisted_assets
from tests.integration.test_asset_catalog import registration as registration
from tests.integration.test_asset_catalog import setup_run as setup_run
from tests.integration.test_asset_catalog import suite_registration as suite_registration
from tests.integration.test_delivery import certificates

pytestmark = pytest.mark.integration


@pytest.fixture
async def catalog_server(catalog, tmp_path):
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_AssetCatalogServicer_to_server(AssetCatalogService(container), server)
    rpc.add_QuotaManagementServicer_to_server(QuotaManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    def channel(identity="qs"):
        return grpc.aio.secure_channel(
            f"localhost:{port}",
            grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            ),
        )

    try:
        yield channel
    finally:
        await server.stop(0)
        await container.close()


async def test_discover_read_exact_native_profile_without_receipt_audit(catalog, catalog_server):
    _, scope, receipt = catalog
    ref = receipt.manifest.profile
    query = pb.AssetCatalogQuery(
        scope=pb.PublicationScope(
            organization_id=scope.organization_id, operator_user_id=scope.operator_user_id
        ),
        kind="profile",
        identity=ref.identity,
        limit=1,
    )
    async with catalog_server() as channel:
        client = rpc.AssetCatalogStub(channel)
        page = await client.List(query, timeout=5)
        assert page.schema_version == "qs-ai-asset-page/v1"
        data = json.loads(page.payload_json)
        assert data["items"] and "definition_json" not in page.payload_json
        result = await client.Get(
            pb.AssetCatalogGetQuery(
                scope=query.scope, kind="profile", identity=ref.identity, version=ref.version
            ),
            timeout=5,
        )
        assert result.schema_version == "qs-ai-asset-detail/v1"
        detail = json.loads(result.payload_json)
        assert detail["item"]["reference"]["fingerprint"] == ref.fingerprint
        assert json.loads(detail["definition_json"])["version"] == ref.version
        assert set(detail) == {"item", "definition_json"}


async def test_wrong_workload_invalid_scope_filters_and_missing_detail(catalog_server):
    query = pb.AssetCatalogQuery(
        scope=pb.PublicationScope(organization_id=1, operator_user_id=42), kind="profile"
    )
    async with catalog_server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.AssetCatalogStub(channel).List(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with catalog_server() as channel:
        client = rpc.AssetCatalogStub(channel)
        for change in (
            {"kind": "receipt"},
            {"limit": 51},
            {"limit": -1},
            {"cursor": "bad"},
            {"identity": "a" * 9000},
            {"scope": pb.PublicationScope()},
        ):
            bad = pb.AssetCatalogQuery()
            bad.CopyFrom(query)
            for k, v in change.items():
                if k == "scope":
                    bad.scope.CopyFrom(v)
                else:
                    setattr(bad, k, v)
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await client.List(bad, timeout=5)
            assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            assert "9000" not in error.value.details()
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Get(
                pb.AssetCatalogGetQuery(
                    scope=query.scope, kind="profile", identity="missing", version="v1"
                ),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_configuration_status_requires_qs_identity_and_valid_scope(catalog_server):
    query = pb.QuotaQuery(scope=pb.PublicationScope(organization_id=1, operator_user_id=42))
    async with catalog_server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.QuotaManagementStub(channel).Status(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with catalog_server() as channel:
        client = rpc.QuotaManagementStub(channel)
        result = await client.Status(query, timeout=5)
        assert result.schema_version == "qs-ai-configuration-status/v1"
        data = json.loads(result.data_json)
        assert data["organization_id"] == 1
        assert data["categories"]
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Status(pb.QuotaQuery(), timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_policy_references_require_qs_and_reject_invalid_scope(catalog_server):
    query = pb.PolicyReferencesQuery(
        scope=pb.PublicationScope(organization_id=1, operator_user_id=42),
        kind="execution_policy",
        usage_kind="evaluation",
        reference=pb.FrozenEvaluationRef(
            id="missing", version="v1", fingerprint="sha256:" + "a" * 64
        ),
    )
    async with catalog_server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.AssetCatalogStub(channel).References(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with catalog_server() as channel:
        client = rpc.AssetCatalogStub(channel)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.References(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        query.scope.Clear()
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.References(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
