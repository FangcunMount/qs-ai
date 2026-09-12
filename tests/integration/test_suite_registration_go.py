"""Real QS application/TLS proxy to Python/MySQL; IAM snapshots remain synthetic."""

from dataclasses import asdict
from uuid import uuid4

import pytest

from tests.integration.test_prompt_draft_go import go_client as go_client
from tests.integration.test_prompt_draft_go import go_drafts as go_drafts
from tests.integration.test_suite_registration_grpc import assets as assets
from tests.integration.test_suite_registration_grpc import complete_release as complete_release
from tests.integration.test_suite_registration_grpc import (
    evaluation_release as evaluation_release,
)
from tests.integration.test_suite_registration_grpc import persisted_assets as persisted_assets
from tests.integration.test_suite_registration_grpc import registration as registration
from tests.integration.test_suite_registration_grpc import rpc_server as rpc_server
from tests.integration.test_suite_registration_grpc import setup_run as setup_run
from tests.integration.test_suite_registration_grpc import suite_registration as suite_registration

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def kit(suite_registration):
    return suite_registration[:6]


def body(context):
    command = asdict(context[3])
    command["command_id"] = str(command["command_id"])
    return command


async def test_go_suite_registration_replay_and_new_process_receipt(suite_registration, go_client):
    command = body(suite_registration)
    first = await go_client("suite-register", Suite=command)
    assert first["Code"] == "OK" and not first["Conflict"], first
    receipt = first["State"]
    assert receipt["suite"]["id"] == command["suite_id"]
    assert receipt["manifest"]["profile"] == command["profile"]
    assert receipt["manifest"]["prompt"] == command["prompt"]
    assert (await go_client("suite-register", Suite=command))["State"] == receipt
    assert (await go_client("suite-receipt", CommandID=command["command_id"], AuditOnly=True))[
        "State"
    ] == receipt
    assert (await go_client("suite-register", Suite={**command, "command_id": str(uuid4())}))[
        "Code"
    ] == "Aborted"


async def test_go_suite_authorization_revocation_and_wrong_identity(suite_registration, go_client):
    command = body(suite_registration)
    for scope in ({"Allowed": False}, {"AuditOnly": True}):
        assert (await go_client("suite-register", Suite=command, **scope))["Denied"]
    assert (await go_client("suite-register", Suite=command, identity="other"))[
        "Code"
    ] == "PermissionDenied"
    assert (await go_client("suite-register", Suite={**command, "suite_id": " "}))["Invalid"]
    wrong_source = {**command["source"], "fingerprint": "sha256:" + "0" * 64}
    assert (await go_client("suite-register", Suite={**command, "source": wrong_source}))[
        "Code"
    ] == "InvalidArgument"
    assert (await go_client("suite-register", Suite=command))["Code"] == "OK"
    for scope in ({"OrgID": 2}, {"UserID": 43}):
        assert (await go_client("suite-receipt", CommandID=command["command_id"], **scope))[
            "Code"
        ] == "NotFound"
    assert (await go_client("suite-receipt", CommandID=command["command_id"], Allowed=False))[
        "Denied"
    ]
