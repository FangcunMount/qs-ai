"""Real QS application/TLS proxy to Python/MySQL; IAM snapshots remain synthetic."""

import json
from dataclasses import asdict
from uuid import uuid4

import pytest

from tests.integration.test_profile_registration_grpc import assets as assets
from tests.integration.test_profile_registration_grpc import complete_release as complete_release
from tests.integration.test_profile_registration_grpc import (
    evaluation_release as evaluation_release,
)
from tests.integration.test_profile_registration_grpc import persisted_assets as persisted_assets
from tests.integration.test_profile_registration_grpc import registration as registration
from tests.integration.test_profile_registration_grpc import rpc_server as rpc_server
from tests.integration.test_profile_registration_grpc import setup_run as setup_run
from tests.integration.test_prompt_draft_go import go_client as go_client
from tests.integration.test_prompt_draft_go import go_drafts as go_drafts

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def kit(registration):
    return registration


def body(registration):
    command = asdict(registration[3])
    command["command_id"] = str(command["command_id"])
    return command


async def test_go_register_unicode_definition_replay_and_original_receipt(registration, go_client):
    command = body(registration)
    definition = json.loads(command["definition_json"])
    definition["profile_id"] = "profile-中文<&>\u2028"
    command["definition_json"] = json.dumps(definition, ensure_ascii=False, indent=2)
    first = await go_client("profile-register", Profile=command)
    assert first["Code"] == "OK" and not first["Conflict"], first
    receipt = first["State"]
    assert receipt["manifest"]["profile"]["identity"] == definition["profile_id"]
    assert receipt["command"]["definition_json"] == command["definition_json"]
    assert (await go_client("profile-register", Profile=command))["State"] == receipt
    assert (await go_client("profile-receipt", CommandID=command["command_id"], AuditOnly=True))[
        "State"
    ] == receipt
    assert (await go_client("profile-register", Profile={**command, "command_id": str(uuid4())}))[
        "Code"
    ] == "Aborted"


async def test_go_registration_denies_unauthorized_and_foreign_receipts(registration, go_client):
    command = body(registration)
    for scope in ({"Allowed": False}, {"AuditOnly": True}):
        assert (await go_client("profile-register", Profile=command, **scope))["Denied"]
    assert (await go_client("profile-register", identity="other", Profile=command))[
        "Code"
    ] == "PermissionDenied"
    assert (await go_client("profile-register", Profile={**command, "definition_json": "{}"}))[
        "Code"
    ] == "InvalidArgument"
    assert (await go_client("profile-register", Profile=command))["Code"] == "OK"
    for scope in ({"OrgID": 2}, {"UserID": 43}):
        assert (await go_client("profile-receipt", CommandID=command["command_id"], **scope))[
            "Code"
        ] == "NotFound"
    assert (await go_client("profile-receipt", CommandID=command["command_id"], Allowed=False))[
        "Denied"
    ]
