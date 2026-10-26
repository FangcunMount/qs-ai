"""Broker control inputs freeze a valid first timestamp/wire before any transport call."""

import hashlib

import pytest
from jwcrypto import jwk
from reliable_messaging.transport import physical_topic_v2

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.transaction_archive import freeze_branch
from tests.test_transaction_command import command_fixture
from tests.transaction_fixtures import command


@pytest.mark.parametrize(
    "kind", [pb.START, pb.CHANGE, pb.PARTICIPANT_RETRY, pb.EVALUATION_START, pb.EVALUATION_CANCEL]
)
def test_all_control_commands_are_valid_frozen_sdk_messages_with_first_timestamp(kind):
    header, raw, _ = command_fixture(kind)
    keys = {
        name: jwk.JWK.generate(kty="EC", crv="P-256", kid=name)
        for name in ("qs.sign", "ai.encrypt")
    }
    at = "2026-10-21T02:00:00.000001+00:00"
    prepared = command(
        keys,
        kind,
        pb.MessagingBody.FromString(raw),
        header.aggregate_key,
        header.message_id,
        original_occurred_at=at,
    )
    branch = freeze_branch(
        prepared,
        "7",
        1,
        False,
        False,
        header.message_id,
        physical_topic_v2(prepared.topic, "TRANSACTION", "qs.mysql", "m3-1"),
        "m3-1",
    )
    assert branch.message.occurred_at == prepared.envelope.original_occurred_at == at
    assert hashlib.sha256(branch.wire).digest() == hashlib.sha256(prepared.wire).digest()


def test_legacy_control_builder_does_not_invent_an_original_timestamp_by_default():
    header, raw, _ = command_fixture()
    keys = {
        name: jwk.JWK.generate(kty="EC", crv="P-256", kid=name)
        for name in ("qs.sign", "ai.encrypt")
    }
    prepared = command(
        keys, pb.START, pb.MessagingBody.FromString(raw), header.aggregate_key, header.message_id
    )
    assert prepared.envelope.original_occurred_at == ""
