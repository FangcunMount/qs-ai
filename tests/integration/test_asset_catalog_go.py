"""QS audit authorization and Go catalog integrity checks over actual mTLS/MySQL."""

import hashlib
import json
import os

import grpc
import pytest
from sqlalchemy import delete

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets
from qs_ai.transport.grpc.asset_catalog import AssetCatalogService
from tests.integration.test_asset_catalog import assets as assets
from tests.integration.test_asset_catalog import complete_release as complete_release
from tests.integration.test_asset_catalog import evaluation_release as evaluation_release
from tests.integration.test_asset_catalog import persisted_assets as persisted_assets
from tests.integration.test_asset_catalog import registration as registration
from tests.integration.test_asset_catalog import setup_run as setup_run
from tests.integration.test_asset_catalog import suite_registration as suite_registration
from tests.integration.test_delivery import certificates
from tests.integration.test_prompt_draft_go import go_client as go_client
from tests.integration.test_prompt_draft_go import go_drafts as go_drafts

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def kit(suite_registration):
    return suite_registration[:6]


@pytest.fixture
async def rpc_server(suite_registration, tmp_path):
    _, store, scope, command, at, *_ = suite_registration
    await store.register(scope, command, at)
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    service = grpc.aio.server()
    rpc.add_AssetCatalogServicer_to_server(AssetCatalogService(container), service)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = service.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await service.start()

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
        yield port, channel
    finally:
        await service.stop(0)
        await container.close()


@pytest.mark.parametrize("kind", ["profile", "prompt", "route", "schema", "suite"])
async def test_go_discovery_pagination_exact_detail_and_shared_read(go_client, kind):
    cursor, seen = "", []
    for _ in range(100):
        result = await go_client(
            "catalog-list",
            AuditOnly=True,
            CatalogQuery={"kind": kind, "limit": 1, "cursor": cursor},
        )
        assert result["Code"] == "OK" and not result["Conflict"], result
        page = result["State"]
        for item in page["items"]:
            ref = item["reference"]
            key = (ref["identity"], ref["version"])
            assert key not in seen
            seen.append(key)
            detail = await go_client(
                "catalog-get",
                AuditOnly=True,
                CatalogGet={"kind": kind, "identity": ref["identity"], "version": ref["version"]},
            )
            assert detail["Code"] == "OK" and not detail["Conflict"], detail
            assert detail["State"]["item"] == item
            assert (
                hashlib.sha256(detail["State"]["definition_json"].encode()).hexdigest()
                == ref["content_sha256"]
            )
        if not page["next_cursor"]:
            break
        assert page["next_cursor"] != cursor
        cursor = page["next_cursor"]
    else:
        pytest.fail("Pagination did not terminate")
    assert seen == sorted(seen) and seen
    shared = await go_client(
        "catalog-list", AuditOnly=True, OrgID=2, UserID=43, CatalogQuery={"kind": kind, "limit": 50}
    )
    assert shared["Code"] == "OK" and not shared["Conflict"], shared
    assert [
        (item["reference"]["identity"], item["reference"]["version"])
        for item in shared["State"]["items"]
    ] == seen


async def test_go_catalog_denial_wrong_workload_and_invalid_cursor(go_client):
    args = {"kind": "profile", "limit": 1}
    assert (await go_client("catalog-list", Allowed=False, CatalogQuery=args))["Denied"]
    assert (
        await go_client(
            "catalog-get",
            Allowed=False,
            CatalogGet={"kind": "profile", "identity": "x", "version": "v1"},
        )
    )["Denied"]
    assert (await go_client("catalog-list", identity="other", CatalogQuery=args))[
        "Code"
    ] == "PermissionDenied"
    assert (await go_client("catalog-list", CatalogQuery={**args, "cursor": "bad"}))["Invalid"]
    assert (
        await go_client(
            "catalog-get", CatalogGet={"kind": "profile", "identity": "missing", "version": "v1"}
        )
    )["Code"] == "NotFound"


async def test_go_catalog_preserves_255_character_identity(suite_registration, go_client):
    tx = suite_registration[0]
    identity = "中" * 255
    raw = json.dumps({"profile_id": identity, "version": "v1"}, ensure_ascii=False)
    asset = ProfileAsset(identity, "v1", "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw)
    try:
        await MySQLProfileAssets(tx).put(asset, "catalog-interop", "synthetic")
        result = await go_client(
            "catalog-list", CatalogQuery={"kind": "profile", "identity": identity}
        )
        assert result["Code"] == "OK" and not result["Conflict"], result
        assert len(result["State"]["items"]) == 1
        detail = await go_client(
            "catalog-get", CatalogGet={"kind": "profile", "identity": identity, "version": "v1"}
        )
        assert detail["Code"] == "OK" and not detail["Conflict"], detail
        assert detail["State"]["definition_json"] == raw
    finally:
        async with tx.open() as db:
            await db.execute(delete(profile_assets).where(profile_assets.c.profile_id == identity))
            await db.commit()
