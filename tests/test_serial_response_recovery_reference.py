"""Readonly first reference recovery never substitutes current lease, owner or version."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_serial_response_receipts as receipt
from tests.test_evaluation_serial_response_receipts import original as original


def fixture(original):
    p = original
    row = dict(
        run_id=str(p.ref.run_id),
        invocation_id=p.cp.invocation_id,
        execution_id=p.cp.execution_id,
        claim_version=p.ref.version,
        definition_json=p.raw,
        sha256=p.digest,
        execution_mode="serial_v1",
    )
    selected = Mock()
    selected.mappings.return_value.one_or_none.return_value = row
    db = SimpleNamespace(
        execute=AsyncMock(return_value=selected),
        commit=AsyncMock(),
        scalar=AsyncMock(return_value=receipt.encode_checkpoint(p.cp)),
    )

    async def call(**changes):
        fields = dict(
            run_id=p.ref.run_id,
            organization_id=p.ref.organization_id,
            invocation_id=p.cp.invocation_id,
        )
        fields.update(changes)
        return await receipt.read_serial_response_reference(db, **fields)

    return SimpleNamespace(**locals())


async def test_new_instance_gets_exact_first_reference_without_business_or_model_write(original):
    f = fixture(original)
    assert await f.call() == original.ref
    query = f.db.execute.await_args.args[0]
    assert query.is_select and query._for_update_arg is None
    assert query.compile().params["organization_id_1"] == original.ref.organization_id
    assert f.db.scalar.await_args.args[0].is_select
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["scope", "metadata", "digest", "body", "mode", "ledger"])
async def test_wrong_scope_or_inexact_receipt_cannot_authorize_response_finalization(
    original, damage
):
    f = fixture(original)
    changes = {}
    if damage == "scope":
        changes["organization_id"] = 2
    elif damage == "metadata":
        f.row["claim_version"] += 1
    elif damage == "digest":
        f.row["sha256"] = "0" * 64
    elif damage == "body":
        f.row["definition_json"] += " "
    elif damage == "mode":
        f.row["execution_mode"] = "candidate_v2"
    else:
        renewed = replace(
            original.cp, lease_expires_at=original.cp.lease_expires_at + timedelta(seconds=1)
        )
        f.db.scalar.return_value = receipt.encode_checkpoint(renewed)
    with pytest.raises((ValueError, CheckpointConflict)):
        await f.call(**changes)
    f.db.commit.assert_not_called()


async def test_absent_original_response_is_readonly_absence_not_rollback_evidence(original):
    f = fixture(original)
    f.selected.mappings.return_value.one_or_none.return_value = None
    assert await f.call() is None
    f.db.scalar.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("organization_id", [True, 0, 2**63])
async def test_invalid_recovery_scope_fails_before_database(original, organization_id):
    f = fixture(original)
    with pytest.raises(ValueError):
        await f.call(organization_id=organization_id)
    f.db.execute.assert_not_called()
