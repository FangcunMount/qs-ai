"""Actual Go scope/adapter and Python lifecycle; permission snapshots remain synthetic."""

import json

import pytest

from tests.integration.test_prompt_draft_go import go_client as go_client
from tests.integration.test_prompt_draft_go import go_drafts as go_drafts
from tests.integration.test_prompt_draft_grpc import rpc_server as rpc_server
from tests.integration.test_prompt_drafts import kit as kit
from tests.integration.test_prompt_freezes import freezing as freezing

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_reopen_frozen_draft_without_other_actor_receipt(kit, freezing, go_client):
    _, _, scope, _, at, _ = kit
    freezer, command, draft = freezing
    query = {"DraftID": str(draft.draft_id), "AuditOnly": True, "UserID": 43}
    editable = await go_client("lifecycle", **query)
    assert editable["Code"] == "OK" and not editable["Conflict"], editable
    assert editable["State"]["schema_version"] == "qs-ai-prompt-lifecycle/v1"
    assert editable["State"]["status"] == "editable" and "frozen" not in editable["State"]
    receipt = await freezer.freeze(scope, command, at)
    frozen = await go_client("lifecycle", **query)
    assert frozen["Code"] == "OK" and not frozen["Conflict"], frozen
    assert frozen["State"]["status"] == "frozen"
    assert frozen["State"]["frozen"]["revision"] == frozen["State"]["draft"]["revision"] == 1
    assert frozen["State"]["frozen"]["asset"]["fingerprint"] == receipt.asset.fingerprint
    assert str(command.command_id) not in json.dumps(frozen)
    assert (await go_client("freeze-receipt", CommandID=str(command.command_id), UserID=43))[
        "Code"
    ] == "NotFound"
    assert (await go_client("lifecycle", **{**query, "OrgID": 2}))["Code"] == "NotFound"
    assert (await go_client("lifecycle", **{**query, "Allowed": False}))["Denied"]
    assert (await go_client("lifecycle", **{**query, "DraftID": "bad"}))["Invalid"]
    assert (await go_client("lifecycle", identity="other", **query))["Code"] == "PermissionDenied"
