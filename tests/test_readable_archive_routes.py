"""Exact DB/witness identity still requires a trusted current/historical route."""

from dataclasses import replace
from unittest.mock import Mock

import pytest
from reliable_messaging.transaction import Binding, branch_key, encode_reference, execution_key

from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError
from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_branch
from tests.route_fixtures import readable_routes
from tests.test_evaluation_state_plan import fixture


def stored_archive():
    frozen = fixture()
    branch = frozen.plan.branches[0]
    ref = frozen.plan.reference(branch.branch_id, 1, "m3-1")
    value = decode_branch(branch)
    row = dict(
        reference=encode_reference(ref),
        frozen_payload=branch.message.payload,
        first_wire=branch.wire,
        organization_id=value.organization_id,
        execution_key=execution_key(ref.owner, ref.operation_id, ref.writer_epoch, ref.fence),
        branch_key=branch_key(ref.branch_id),
    )
    return value, ref, row


def test_original_archive_bytes_are_readable_only_with_exact_historical_registration():
    value, ref, row = stored_archive()
    original = dict(row)
    archive = TransactionArchive(
        Mock(), Binding("ai.mysql", "primary", "m3-1"), routes=readable_routes()
    )
    restored, exact = archive._validate_row(row, ref.message_id)
    assert restored == value and exact == ref and row == original
    active = TransactionArchive(
        Mock(), Binding("ai.mysql", "primary", "m3-1"), routes=readable_routes(history=False)
    )
    with pytest.raises(MessagingContractError, match="route"):
        active._validate_row(row, ref.message_id)


def test_even_structurally_exact_reference_cannot_choose_an_unregistered_topic():
    _, ref, row = stored_archive()
    archive = TransactionArchive(
        Mock(), Binding("ai.mysql", "primary", "m3-1"), routes=readable_routes()
    )
    changed = dict(row, reference=encode_reference(replace(ref, physical_topic="foreign")))
    with pytest.raises(MessagingContractError, match="route"):
        archive._validate_row(changed, ref.message_id)
