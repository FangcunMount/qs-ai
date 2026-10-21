"""Original candidate ownership reads remain distinct from commit/model authority."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import completion_owner
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from tests.test_semantic_readonly_source import source


def fixture():
    p = source()
    claim = SlotClaim(p.run_id, 3, p.cp)
    coordinator = dict(version=10, checkpoint_json=None)
    run = dict(p.run, execution_mode="candidate_v2")
    claim_row = dict(
        version=claim.version,
        case_id=p.cp.case_id,
        slot_ordinal=p.cp.slot_ordinal,
        checkpoint_json=encode(p.cp),
    )
    results = []
    for value in (coordinator, run, claim_row, claim_row):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = value
        result.mappings.return_value.all.return_value = [value]
        results.append(result)
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=results), commit=AsyncMock(), rollback=AsyncMock()
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize("status", ["collecting", "blocked"])
@pytest.mark.parametrize("renewed", [False, True])
async def test_original_candidate_owner_keeps_scoped_coordinator_active_then_exact_claim(
    status, renewed, lock
):
    p = fixture()
    p.run["progress_json"]["status"] = status
    if renewed:
        p.claim_row["checkpoint_json"] = encode(
            replace(p.p.cp, lease_expires_at=p.p.cp.lease_expires_at + timedelta(seconds=5))
        )
    run, version, actual = await completion_owner(p.db, p.p.run_id, 1, p.claim, lock=lock)
    assert run == p.run and version == 10
    assert (
        actual.version == 3 and actual.checkpoint.invocation_id == p.claim.checkpoint.invocation_id
    )
    assert actual.checkpoint.lease_expires_at == p.p.cp.lease_expires_at + timedelta(
        seconds=5 if renewed else 0
    )
    calls = p.db.execute.await_args_list
    assert len(calls) == 4
    assert all(
        c.args[0].is_select and (c.args[0]._for_update_arg is not None) == lock for c in calls
    )
    assert calls[1].args[0].compile().params["organization_id_1"] == 1
    assert [c.args[0].get_final_froms()[0].name for c in calls] == [
        "evaluation_checkpoints",
        "evaluation_runs",
        "evaluation_slot_claims",
        "evaluation_slot_claims",
    ]
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize(
    "damage",
    [
        "foreign_run",
        "missing_coordinator",
        "missing_run",
        "serial_run",
        "serial_checkpoint",
        "ended_run",
        "missing_claim",
        "claim_version",
        "claim_owner",
        "claim_phase",
        "claim_execution",
        "claim_invocation",
        "claim_index",
    ],
)
async def test_original_candidate_owner_rejects_wrong_scope_or_changed_claim_before_any_write(
    damage,
    lock,
):
    p = fixture()
    claim = p.claim
    if damage == "foreign_run":
        claim = replace(claim, run_id=uuid4())
    elif damage in ("missing_coordinator", "missing_run", "missing_claim"):
        p.results[
            {"missing_coordinator": 0, "missing_run": 1, "missing_claim": 3}[damage]
        ].mappings.return_value.one_or_none.return_value = None
    elif damage == "serial_run":
        p.run["execution_mode"] = "serial"
    elif damage == "serial_checkpoint":
        p.coordinator["checkpoint_json"] = encode(p.p.cp)
    elif damage == "ended_run":
        p.run["progress_json"]["status"] = "awaiting_review"
    elif damage == "claim_version":
        p.claim_row["version"] += 1
    elif damage == "claim_index":
        p.claim_row["slot_ordinal"] += 1
    else:
        fields = {
            "claim_owner": "owner",
            "claim_phase": "phase",
            "claim_execution": "execution_id",
            "claim_invocation": "invocation_id",
        }
        key = fields[damage]
        p.claim_row["checkpoint_json"] = encode(
            replace(
                p.p.cp,
                **(
                    {"phase": "prepared", "dispatch_started_at": None}
                    if key == "phase"
                    else {key: "foreign:value"}
                ),
            )
        )
    with pytest.raises(CheckpointConflict):
        await completion_owner(p.db, p.p.run_id, 1, claim, lock=lock)
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
