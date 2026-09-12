"""QS freeze proxy over actual mTLS and MySQL; request permission snapshots are synthetic."""

from dataclasses import asdict
from uuid import uuid4

import pytest

from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from tests.integration.test_prompt_draft_go import go_client as go_client
from tests.integration.test_prompt_draft_go import go_drafts as go_drafts
from tests.integration.test_prompt_draft_grpc import rpc_server as rpc_server
from tests.integration.test_prompt_drafts import kit as kit
from tests.integration.test_prompt_freezes import freezing as freezing

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_edit_freeze_replay_new_process_receipt_and_locked_revision(
    kit, freezing, go_client
):
    tx, _, _, _, _, source = kit
    _, command, draft = freezing
    draft_id = str(draft.draft_id)
    content = {
        **asdict(draft.content),
        "system_message": draft.content.system_message + "\n继续保持事实边界。",
    }
    edit = {
        "command_id": str(uuid4()),
        "reason": "冻结前修订",
        "expected_revision": 1,
        "content": content,
    }
    changed = await go_client("revise", DraftID=draft_id, Revise=edit)
    assert changed["Code"] == "OK" and not changed["Conflict"], changed
    freeze = {
        "command_id": str(command.command_id),
        "reason": command.reason,
        "expected_revision": 2,
    }
    first = await go_client("freeze", DraftID=draft_id, Freeze=freeze)
    assert first["Code"] == "OK" and not first["Conflict"], first
    receipt = first["State"]
    assert receipt["scope"] == {"organization_id": 1, "operator_user_id": 42}
    assert receipt["command"]["expected_revision"] == 2
    assert receipt["asset"]["version"] == draft.target_version
    assert receipt["asset"]["fingerprint"] != source.fingerprint
    assert (await go_client("freeze", DraftID=draft_id, Freeze=freeze))["State"] == receipt
    assert (await go_client("freeze-receipt", CommandID=freeze["command_id"], AuditOnly=True))[
        "State"
    ] == receipt
    assert (await go_client("get", DraftID=draft_id, Revision=2, AuditOnly=True))[
        "State"
    ] == changed["State"]
    locked = await go_client(
        "revise",
        DraftID=draft_id,
        Revise={**edit, "command_id": str(uuid4()), "expected_revision": 2},
    )
    assert locked["Code"] == "Aborted"
    assert await MySQLPromptAssets(tx).get(source.template_id, source.version) == source


async def test_go_freeze_rejects_unauthorized_invalid_and_foreign_receipt(kit, freezing, go_client):
    _, _, _, _, _, _ = kit
    _, command, draft = freezing
    draft_id = str(draft.draft_id)
    freeze = {
        "command_id": str(command.command_id),
        "reason": command.reason,
        "expected_revision": 1,
    }
    for scope in ({"Allowed": False}, {"AuditOnly": True}):
        assert (await go_client("freeze", DraftID=draft_id, Freeze=freeze, **scope))["Denied"]
    assert (await go_client("freeze", identity="other", DraftID=draft_id, Freeze=freeze))[
        "Code"
    ] == "PermissionDenied"
    assert (await go_client("freeze", DraftID=draft_id, Freeze=freeze, OrgID=2))[
        "Code"
    ] == "NotFound"
    assert (await go_client("freeze", DraftID=draft_id, Freeze={**freeze, "expected_revision": 0}))[
        "Invalid"
    ]
    assert (await go_client("freeze", DraftID=draft_id, Freeze=freeze))["Code"] == "OK"
    for scope in ({"OrgID": 2}, {"UserID": 43}):
        assert (await go_client("freeze-receipt", CommandID=freeze["command_id"], **scope))[
            "Code"
        ] == "NotFound"
    assert (await go_client("freeze-receipt", CommandID=freeze["command_id"], Allowed=False))[
        "Denied"
    ]


async def test_go_unfinished_template_freeze_is_rejected_without_asset(kit, freezing, go_client):
    tx, _, _, _, _, _ = kit
    _, command, draft = freezing
    draft_id = str(draft.draft_id)
    edit = {
        "command_id": str(uuid4()),
        "reason": "未完成模板",
        "expected_revision": 1,
        "content": {**asdict(draft.content), "task_template": "{{unfinished"},
    }
    assert (await go_client("revise", DraftID=draft_id, Revise=edit))["Code"] == "OK"
    freeze = {
        "command_id": str(command.command_id),
        "reason": command.reason,
        "expected_revision": 2,
    }
    assert (await go_client("freeze", DraftID=draft_id, Freeze=freeze))["Code"] == "InvalidArgument"
    assert (await go_client("freeze-receipt", CommandID=freeze["command_id"]))["Code"] == "NotFound"
    assert await MySQLPromptAssets(tx).get(draft.template_id, draft.target_version) is None
