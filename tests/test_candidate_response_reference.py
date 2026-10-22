"""Lease renewal changes source ownership, never first durable response identity."""

import hashlib
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    CandidateResponseReference,
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from tests.test_semantic_readonly_source import source


def fixture(*, renewed=False, after_original_expiry=False):
    p = source()
    cp = p.cp
    current = (
        replace(cp, lease_expires_at=cp.lease_expires_at + timedelta(seconds=30)) if renewed else cp
    )
    claim = SlotClaim(p.run_id, 3, current)
    finished = (
        cp.lease_expires_at + timedelta(seconds=1)
        if after_original_expiry
        else cp.dispatch_started_at + timedelta(seconds=1)
    )
    response = ModelResponse(
        cp.invocation_id,
        "request:first",
        "model",
        '{"first":true}',
        '{"first":true}',
        "",
        None,
        None,
        10,
    )
    result = EvaluationResponse(response, None, finished)
    raw = result.encode()
    row = dict(
        execution_id=cp.execution_id,
        claim_version=3,
        definition_json=raw,
        sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )
    run, ledger, receipt = Mock(), Mock(), Mock()
    run.mappings.return_value.one_or_none.return_value = dict(execution_mode="candidate_v2")
    ledger.scalar_one_or_none.return_value = encode(cp)
    receipt.mappings.return_value.one_or_none.return_value = row
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[run, ledger, receipt]),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("renewed", [False, True])
@pytest.mark.parametrize("after_original_expiry", [False, True])
async def test_original_dispatch_reference_and_first_response_do_not_depend_on_current_lease(
    renewed, after_original_expiry
):
    p = fixture(renewed=renewed, after_original_expiry=after_original_expiry)
    actual = await read_candidate_response(p.db, 1, p.claim)
    assert actual.reference == CandidateResponseReference(p.p.run_id, 1, 3, p.cp)
    assert actual.result == p.result
    assert actual.reference.fields()["checkpoint"] == encode(p.cp)
    assert "first" not in repr(actual) and "lease" not in repr(actual)
    calls = p.db.execute.await_args_list
    assert len(calls) == 3 and all(
        c.args[0].is_select and c.args[0]._for_update_arg is None for c in calls
    )
    assert calls[0].args[0].compile().params["organization_id_1"] == 1
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize(
    "damage",
    [
        "missing_run",
        "serial_run",
        "missing_ledger",
        "foreign_owner",
        "wrong_version",
        "wrong_execution",
        "wrong_response_invocation",
        "early_response",
    ],
)
async def test_response_observation_rejects_foreign_scope_or_changed_immutable_identity(damage):
    p = fixture()
    if damage == "missing_run":
        p.run.mappings.return_value.one_or_none.return_value = None
    elif damage == "serial_run":
        p.run.mappings.return_value.one_or_none.return_value = dict(execution_mode="serial_v1")
    elif damage == "missing_ledger":
        p.ledger.scalar_one_or_none.return_value = None
    elif damage == "foreign_owner":
        p.ledger.scalar_one_or_none.return_value = encode(replace(p.cp, owner="foreign:owner"))
    elif damage == "wrong_version":
        p.row["claim_version"] += 1
    elif damage == "wrong_execution":
        p.row["execution_id"] = "foreign:execution"
    else:
        result = (
            replace(p.result, response=replace(p.response, invocation_id="foreign:call"))
            if damage == "wrong_response_invocation"
            else replace(p.result, finished_at=p.cp.claimed_at - timedelta(seconds=1))
        )
        raw = result.encode()
        p.row.update(definition_json=raw, sha256=hashlib.sha256(raw.encode()).hexdigest())
    with pytest.raises(CheckpointConflict):
        await read_candidate_response(p.db, 1, p.claim)
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("organization_id", True),
        ("organization_id", 0),
        ("organization_id", 2**63),
        ("version", True),
        ("version", 0),
        ("version", 2**63),
    ],
)
def test_reference_fence_and_scope_reject_invalid_cross_language_boundaries(field, value):
    p = fixture()
    args = dict(run_id=p.p.run_id, organization_id=1, version=3, checkpoint=p.cp)
    args[field] = value
    with pytest.raises(ValueError):
        CandidateResponseReference(**args)


async def test_absent_response_is_not_a_model_or_commit_authorization():
    p = fixture()
    p.receipt.mappings.return_value.one_or_none.return_value = None
    assert await read_candidate_response(p.db, 1, p.claim) is None
    p.db.commit.assert_not_called()
