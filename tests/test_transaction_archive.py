import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from reliable_messaging.transaction import FrozenBranch, FrozenPlan, decode_plan, encode_plan

from qs_ai.infrastructure.workflow_transport.messaging import (
    MessagingContractError,
    PreparedMessage,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import (
    MAGIC,
    decode_archive,
    decode_branch,
    freeze_branch,
)


def fixture():
    value = json.loads(Path("tests/fixtures/messaging_archive/v1.json").read_text())
    archive = decode_archive(bytes.fromhex(value["payload_hex"]), bytes.fromhex(value["wire_hex"]))
    return value, archive


def test_go_python_archive_golden_preserves_unsigned_ids_unknown_header_and_first_wire():
    value, archive = fixture()
    assert archive.organization_id == archive.sequence == 2**64 - 1
    assert archive.ordered and archive.requires_receipt
    assert archive.header == bytes.fromhex(value["header_hex"])
    assert archive.header.endswith(bytes.fromhex("a00607"))
    assert archive.body == bytes.fromhex(value["body_hex"])
    assert archive.message().fingerprint == value["message_fingerprint"]
    assert hashlib.sha256(archive.wire).hexdigest() == value["wire_sha256"]
    assert archive.encode().hex() == value["payload_hex"]
    # Explicit layout checks protect integer width and raw byte boundaries.
    assert archive.encode()[len(MAGIC) : len(MAGIC) + 16] == bytes([255]) * 16
    assert archive.encode()[len(MAGIC) + 16] == 3


def test_reservation_plan_round_trip_is_sufficient_without_local_signing_keys():
    _, archive = fixture()
    branch = FrozenBranch("command", "m3.archive.test", "m3-1", archive.message(), archive.wire)
    plan = FrozenPlan(
        "qs.mysql",
        f"org:{archive.organization_id}",
        "operation",
        "same-request",
        hashlib.sha256(b"input").digest(),
        (branch,),
    )
    restored = decode_plan(encode_plan(plan))
    assert decode_branch(restored.branches[0]) == archive
    # A newly sealed candidate's wire/header is not needed to recover the winner.
    prepared = PreparedMessage(
        archive.envelope(), archive.body, archive.wire, branch.message.destination
    )
    frozen = freeze_branch(
        prepared,
        str(archive.organization_id),
        archive.sequence,
        True,
        True,
        "command",
        "m3.archive.test",
        "m3-1",
    )
    prepared.envelope.message_id = "changed-after-freeze"
    assert decode_branch(frozen) == archive


@pytest.mark.parametrize(
    "mutation", ["version", "flag", "header-length", "body", "truncated", "trailing"]
)
def test_archive_refuses_ambiguous_or_damaged_bytes(mutation):
    _, archive = fixture()
    raw = bytearray(archive.encode())
    if mutation == "version":
        raw[0] ^= 1
    elif mutation == "flag":
        raw[len(MAGIC) + 16] = 4
    elif mutation == "header-length":
        raw[len(MAGIC) + 17 : len(MAGIC) + 21] = b"\xff" * 4
    elif mutation == "body":
        raw[-1] ^= 1
    elif mutation == "truncated":
        raw = raw[:-1]
    else:
        raw.extend(b"\0")
    with pytest.raises(MessagingContractError):
        decode_archive(bytes(raw), archive.wire)


def test_archive_refuses_changed_scope_route_wire_and_incomplete_original_time():
    _, a = fixture()
    b = FrozenBranch("command", "physical", "m3-1", a.message(), a.wire)
    for changed in [
        replace(b, route_revision="wrong"),
        replace(b, message=replace(b.message, scope="org:7")),
        replace(b, wire=b"wrong"),
    ]:
        with pytest.raises((ValueError, MessagingContractError)):
            decode_branch(changed)
    with pytest.raises(MessagingContractError):
        replace(a, organization_id=7).encode()
    header = a.envelope()
    header.original_occurred_at = ""
    with pytest.raises(ValueError):
        replace(a, header=header.SerializeToString(deterministic=True)).message()


async def test_versioned_payload_reader_falls_back_only_when_new_identity_is_absent():
    from unittest.mock import AsyncMock, Mock

    from qs_ai.infrastructure.persistence.mysql.transaction_archive import ArchivePayloadReader

    current, legacy = Mock(), Mock()
    current.payload = AsyncMock(return_value=None)
    legacy.payload = AsyncMock(return_value=b"original legacy body")
    reader = ArchivePayloadReader(current, legacy)
    db, reference = Mock(), Mock()
    assert await reader.payload(db, reference, "qs-server") == b"original legacy body"
    legacy.payload.assert_awaited_once_with(db, reference, "qs-server")
    current.payload.return_value = b"committed original body"
    assert await reader.payload(db, reference, "qs-server") == b"committed original body"
    assert legacy.payload.await_count == 1
    current.payload.side_effect = MessagingContractError("prepared or damaged")
    with pytest.raises(MessagingContractError):
        await reader.payload(db, reference, "qs-server")
    assert legacy.payload.await_count == 1
