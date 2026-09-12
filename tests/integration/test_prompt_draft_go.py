"""QS application -> actual mTLS Python/MySQL; IAM permission snapshots are synthetic."""

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest

from tests.integration.test_prompt_draft_grpc import rpc_server as rpc_server
from tests.integration.test_prompt_drafts import kit as kit

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def go_drafts(tmp_path):
    source = os.getenv("QS_AI_GOVERNANCE_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires isolated QS governance checkout and Go")
    root = Path(source)
    with tempfile.TemporaryDirectory(prefix="qs_ai_draft_", dir=root / "scripts") as directory:
        program = Path(directory) / "main.go"
        shutil.copyfile(
            Path(__file__).parents[1] / "fixtures/go_prompt_draft_management.go", program
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
async def go_client(kit, rpc_server, go_drafts, tmp_path):
    _, _, scope, _, _, _ = kit
    port, _ = rpc_server

    async def invoke(action, identity="qs", **overrides):
        request = {
            "Action": action,
            "OrgID": scope.organization_id,
            "UserID": scope.operator_user_id,
            "Allowed": True,
            **overrides,
        }
        process = await asyncio.create_subprocess_exec(
            str(go_drafts),
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


async def test_go_draft_edit_history_replay_and_receipt(kit, go_client):
    _, _, _, command, _, _ = kit
    body = asdict(command)
    draft_id = str(body.pop("draft_id"))
    body["command_id"] = str(body["command_id"])
    first = await go_client("create", DraftID=draft_id, Create=body)
    assert first["Code"] == "OK" and not first["Conflict"], first
    original = first["State"]
    content = {**original["content"], "task_template": "新的草稿 {{unfinished}}"}
    edit = {
        "command_id": str(uuid4()),
        "reason": "跨语言修订",
        "expected_revision": 1,
        "content": content,
    }
    second = await go_client("revise", DraftID=draft_id, Revise=edit)
    assert second["Code"] == "OK" and not second["Conflict"], second
    assert second["State"]["revision"] == 2
    assert second["State"]["content"] == content
    assert (await go_client("create", DraftID=draft_id, Create=body))["State"] == original
    assert (await go_client("revise", DraftID=draft_id, Revise=edit))["State"] == second["State"]
    assert (await go_client("get", DraftID=draft_id, AuditOnly=True))["State"] == second["State"]
    assert (await go_client("get", DraftID=draft_id, Revision=1, AuditOnly=True))[
        "State"
    ] == original
    assert (await go_client("receipt", CommandID=edit["command_id"], AuditOnly=True))[
        "State"
    ] == second["State"]
    assert (
        await go_client("revise", DraftID=draft_id, Revise={**edit, "command_id": str(uuid4())})
    )["Code"] == "Aborted"


async def test_go_draft_authorization_and_foreign_scope(kit, go_client):
    _, _, _, command, _, _ = kit
    body = asdict(command)
    draft_id = str(body.pop("draft_id"))
    body["command_id"] = str(body["command_id"])
    for overrides in ({"Allowed": False}, {"AuditOnly": True}):
        assert (await go_client("create", DraftID=draft_id, Create=body, **overrides))["Denied"]
    assert (await go_client("create", identity="other", DraftID=draft_id, Create=body))[
        "Code"
    ] == "PermissionDenied"
    assert (await go_client("create", DraftID=draft_id, Create=body))["Code"] == "OK"
    assert (await go_client("get", DraftID=draft_id, Allowed=False))["Denied"]
    assert (await go_client("get", DraftID=draft_id, OrgID=2))["Code"] == "NotFound"
    assert (await go_client("receipt", CommandID=body["command_id"], UserID=43))[
        "Code"
    ] == "NotFound"
    assert (await go_client("get", DraftID=draft_id, Revision=0))["Invalid"]
