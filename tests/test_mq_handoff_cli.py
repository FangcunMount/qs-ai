"""Native maintenance input and diagnostic boundaries; no storage acceptance claims."""

import argparse
import json
from uuid import uuid4

import pytest

from qs_ai.maintenance import messaging_handoff as cli
from qs_ai.maintenance.legacy_results import REVISION, SCHEMA_HEAD, HandoffError, digest


def manifest():
    source = {
        "event_id": str(uuid4()),
        "session_id": str(uuid4()),
        "version": 3,
        "payload_json_sha256": "a" * 64,
        "attempts": 0,
        "available_at": None,
        "created_at": None,
    }
    value = {
        "revision": REVISION,
        "header": {"database": "isolated", "schema_head": SCHEMA_HEAD},
        "rows": [
            {
                "source": source,
                "source_sha256": digest(source),
                "delivered": False,
                "mq_owned": False,
                "first_wire_sha256": None,
            }
        ],
    }
    return {**value, "digest": digest(value)}


def args(**values):
    return argparse.Namespace(
        **{
            "action": "dry-run",
            "event_id": [str(uuid4())],
            "manifest": None,
            "reviewed_digest": None,
            "all_claimers_stopped_and_admission_closed": False,
            "signing_key_file": None,
            "qs_recipient_key_file": None,
            **values,
        }
    )


@pytest.mark.parametrize("content", ['{"rows": [], "rows": []}', "{} {}", "[]", "x" * 1_048_577])
def test_handoff_manifest_rejects_ambiguous_or_unbounded_input(tmp_path, content):
    path = tmp_path / "manifest.json"
    path.write_text(content)
    with pytest.raises((HandoffError, ValueError)):
        cli.read_manifest(str(path))


def test_handoff_manifest_rejects_symlink(tmp_path):
    target = tmp_path / "original.json"
    target.write_text("{}")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(OSError):
        cli.read_manifest(str(link))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["digest", "attestation", "keys", "empty_ids", "duplicate_ids"])
async def test_handoff_invalid_review_refused_before_database(
    tmp_path, monkeypatch, capsys, invalid
):
    value = manifest()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(value))
    options = args(
        action="apply",
        event_id=[],
        manifest=str(path),
        reviewed_digest=value["digest"],
        all_claimers_stopped_and_admission_closed=True,
        signing_key_file="original-private-key",
        qs_recipient_key_file="original-public-key",
    )
    if invalid == "digest":
        options.reviewed_digest = "b" * 64
    elif invalid == "attestation":
        options.all_claimers_stopped_and_admission_closed = False
    elif invalid == "keys":
        options.signing_key_file = None
    elif invalid == "empty_ids":
        options = args(event_id=[])
    else:
        identity = str(uuid4())
        options = args(event_id=[identity, identity])
    monkeypatch.setattr(cli, "Database", lambda *_: pytest.fail("invalid review opened database"))
    assert await cli.run(options) == 1
    output = capsys.readouterr()
    assert output.out == "" and "no complete transfer" in output.err


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["read", "close"])
async def test_handoff_database_diagnostics_are_redacted_and_owned_pool_closed(
    monkeypatch, capsys, failure
):
    secret = "mysql-password-private-key-sensitive-body"
    closed = []

    class Database:
        def __init__(self, _url):
            pass

        async def close(self):
            closed.append(True)
            if failure == "close":
                raise RuntimeError(secret)

    class Handoff:
        def __init__(self, _transactions):
            pass

        async def dry_run(self, _ids):
            if failure == "read":
                raise RuntimeError(secret)
            return {"dry_run": True}

    monkeypatch.setenv("QS_AI_MESSAGING_HANDOFF_DATABASE_URL", "mysql+asyncmy://isolated")
    monkeypatch.setattr(cli, "Database", Database)
    monkeypatch.setattr(cli, "ResultHandoff", Handoff)
    assert await cli.run(args()) == 1
    output = capsys.readouterr()
    assert closed == [True] and secret not in output.out + output.err
