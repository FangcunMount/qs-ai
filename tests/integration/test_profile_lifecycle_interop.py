"""Real Go authority/client -> mTLS -> Python lifecycle projection -> MySQL."""

import asyncio
import json
import os
from dataclasses import asdict
from datetime import timedelta
from uuid import uuid4

import grpc
import pytest
from sqlalchemy import select

from qs_ai.application.governance.publication import MovePublication
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.schema import profile_assets
from qs_ai.transport.grpc.profile_registration import ProfileManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_profile_lifecycle import dispatched as dispatched
from tests.integration.test_profile_lifecycle import freeze_creation as freeze_creation
from tests.integration.test_profile_lifecycle import judge as judge
from tests.integration.test_profile_lifecycle import passing_reviewable as passing_reviewable
from tests.integration.test_profile_lifecycle import passing_semantics as passing_semantics
from tests.integration.test_profile_lifecycle import persisted_assets as persisted_assets
from tests.integration.test_profile_lifecycle import ready as ready
from tests.integration.test_profile_lifecycle import reviewable as reviewable
from tests.integration.test_profile_lifecycle import setup_run as setup_run

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_profile_lifecycle_current_authority_and_import_publication_distinction(
    ready, go_management, tmp_path
):
    tx, pub_scope, command, at = ready
    async with tx.open() as db:
        row = (await db.execute(select(profile_assets))).mappings().one()
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_ProfileManagementServicer_to_server(ProfileManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    async def call(certificate="qs", **changes):
        request = {
            "Action": "profile-list",
            "OrgID": pub_scope.organization_id,
            "Allowed": True,
            "AuditOnly": True,
            "ProfileLifecycle": {"identity": row["profile_id"], "status": "draft", "limit": 1},
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
        assert result["Code"] == "OK", result
        original = result["State"]["items"][0]
        assert original["status"] == "draft" and not original["active_publication_id"]
        assert original["source_ref"] == row["source_ref"]
        assert original["reference"]["fingerprint"] == row["fingerprint"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        # Profiles are shared definitions. The audit role does not expose actors or receipts.
        assert (await call(OrgID=2))["State"] == result["State"]
        assert not any(key in original for key in ("operator_user_id", "audit", "definition_json"))
        assert (await call(ProfileLifecycle={"identity": row["profile_id"], "status": "bad"}))[
            "Invalid"
        ]
        assert (await call(Action="profile-lifecycle", ProfileVersion="absent"))[
            "Code"
        ] == "NotFound"
        assert (await call(Action="profile-lifecycle", ProfileVersion=row["version"]))[
            "State"
        ] == original
        published = await MySQLPublications(tx).apply(pub_scope, command, at)
        active = published.change.current.active
        assert (await call())["State"]["items"] == []
        query = {"identity": row["profile_id"], "status": "published", "limit": 1}
        value = (await call(ProfileLifecycle=query))["State"]["items"][0]
        assert value["active_publication_id"] == str(active.publication_id)
        assert value["active_run_id"] == str(active.evidence.run_id)
        assert value["reference"] == original["reference"]
        pointer_before = asdict(published.change.current)
        assert (await call(Action="profile-lifecycle", ProfileVersion=row["version"]))[
            "State"
        ] == value
        assert asdict(await MySQLPublications(tx).get(command.selector)) == pointer_before
        await MySQLPublications(tx).apply(
            pub_scope,
            MovePublication(
                uuid4(), command.selector, 1, active.publication_id, "停用查询验证", True, None
            ),
            at + timedelta(seconds=1),
        )
        inactive = (await call(ProfileLifecycle={**query, "status": "disabled"}))["State"]["items"][
            0
        ]
        assert inactive["inactive_reason"] == "disabled" and not inactive["active_publication_id"]
    finally:
        await server.stop(0)
        await container.close()
